//! A registry of **shared, periodically-refreshed tables**, keyed by whatever identifies the thing they
//! describe — copy-on-write merges, fleet-wide dedup of duplicate updates, buffering of updates that race
//! a fetch, single-flight fetches, and a reconcile clock.
//!
//! # The problem
//!
//! Many protocols serve a large reference table (an instrument list, a routing table, a capability set)
//! that is a property of the **server**, not of a connection. Hold it per connection and a fleet of N
//! sessions to one server keeps N copies of a multi-megabyte table, performs N fetches at startup, and
//! applies every server-pushed update N times — each application cloning the whole table.
//!
//! Sharing it per server fixes all of that, but introduces four hazards. Each behaviour below exists
//! because of a specific one:
//!
//! - **Fleet dedup** ([`SlotHandle::merge`]). A pushed update is server-global: every connection receives
//!   the *same* update and every one of them submits it. Merging is idempotent, so the extra merges are
//!   harmless in *content* — but each clones the whole table, so one upstream event forks N copies. A
//!   digest of the update collapses them to one.
//! - **Fetch/update buffering.** The only ordering hazard in the scheme, and the subtle one. A full table
//!   is a snapshot taken server-side at time *T*; an update applied locally at *T+ε* is silently reverted
//!   when the *T* snapshot lands. Protocols rarely carry a sequence number that would let you notice. So
//!   updates arriving *during* a fetch are buffered and replayed onto the fetched table.
//! - **Single-flight** ([`SlotHandle::get_or_fetch`]). When a fleet starts, N connections miss the same key
//!   in the same instant; the losers await the winner rather than all fetching.
//! - **Reconcile clock.** Push streams commonly signal additions and changes but not *removals*, so a
//!   periodic full re-fetch is the only way a deletion is ever observed. A merge deliberately does **not**
//!   reset that clock: an update keeps existing rows fresh but can never reveal a removal, so letting one
//!   postpone the reconcile would let a busy server defer it forever.
//!
//! # Locking
//!
//! Reads are lock-free `ArcSwap` loads, because the read path runs per inbound frame. Every *write*
//! happens while holding the slot's `state` mutex, which is what keeps "is a fetch in flight?" and "apply
//! this update" a single atomic decision — two independent atomics cannot give that. The fetch `await` is
//! held under a *separate* `tokio::sync::Mutex` (the single-flight gate), so **no std lock ever spans a
//! suspension point**.
//!
//! [`SlotHandle`] exists so a connection's hot read does not go through the map at all: resolve the key
//! once at setup, then load through the handle.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;

/// How many recent update-batch digests a slot remembers for the fleet dedup.
///
/// Small on purpose: the frames it deduplicates arrive within milliseconds of each other (every
/// connection to a server is pushed the same frame at once). A ring rather than a single "last
/// applied" so an interleaved `A, B, A` still collapses the duplicate `A` instead of re-applying it
/// over the newer `B`.
const DEDUP_WINDOW: usize = 16;

/// A merge submitted to the registry: takes the current table, returns the next one.
///
/// Copy-on-write, because the table is shared by every connection to the server and readers hold an
/// `Arc` to it. The copy happens **here**, once for the fleet — not per connection.
type Merge<T> = Box<dyn FnOnce(&T) -> T + Send>;

/// Everything about a slot that a writer must decide atomically. The value itself is *not* here —
/// it is an `ArcSwapOption` so readers never take this lock — but it is only ever stored while this
/// is held.
struct SlotState<T> {
    /// A fetch is in flight — merges are buffered rather than applied.
    fetching: bool,
    /// Merges deferred by that fetch, replayed in arrival order once it lands.
    deferred: Vec<Merge<T>>,
    /// Digests of recently applied batches — the fleet dedup.
    seen: VecDeque<u64>,
    /// When the value was last installed **wholesale** — a fetch, a refresh, or an insert. Drives the
    /// reconcile clock. A [`merge`](Registry::merge) deliberately does not bump it (module docs).
    updated_at: Option<Instant>,
    /// A refresh failed; do not attempt another before this. Without it a server erroring on the
    /// re-fetch would be re-asked by every connection on every heartbeat.
    next_attempt: Option<Instant>,
}

impl<T> Default for SlotState<T> {
    fn default() -> Self {
        Self {
            fetching: false,
            deferred: Vec::new(),
            seen: VecDeque::new(),
            updated_at: None,
            next_attempt: None,
        }
    }
}

impl<T> SlotState<T> {
    /// Is a periodic re-fetch owed? Requires a loaded table, no fetch already running, an age past
    /// `interval`, and any post-failure backoff elapsed.
    fn refresh_due(&self, loaded: bool, interval: Duration, now: Instant) -> bool {
        if !loaded || self.fetching {
            return false;
        }
        if self.next_attempt.is_some_and(|t| now < t) {
            return false;
        }
        self.updated_at.is_some_and(|t| now.duration_since(t) >= interval)
    }
}

struct Slot<T> {
    /// Lock-free for readers; written only under `state`.
    value: ArcSwapOption<T>,
    /// Mutated under a lock that is **recovered rather than propagated** on poisoning — every
    /// `lock()` here is `unwrap_or_else(|e| e.into_inner())`, never `expect`.
    ///
    /// This registry is process-wide and shared by every connection, so `expect` made one
    /// connection's panic fatal to the whole fleet: the panicking thread poisons the mutex, and the
    /// next `expect` on any *other* connection panics too, cascading until nothing can read a symbol
    /// table. That turns a single malformed frame on one socket into a total outage.
    ///
    /// Recovering is sound here because the value itself is not behind this lock — it is an
    /// `ArcSwap` swapped atomically, so it is never observed half-written. The lock guards only
    /// bookkeeping (the dedup ring, the deferred-merge list, the refresh clock). The worst a
    /// mid-update panic can leave is a digest in `seen` whose merge never applied, which costs one
    /// skipped duplicate push and self-heals on the next distinct frame — much cheaper than a
    /// fleet-wide cascade.
    state: Mutex<SlotState<T>>,
    /// Serializes every **table reload** for this key — the single-flight fetch, the periodic
    /// reconcile, and any caller-driven forced reload. Separate from `state` precisely so no std lock
    /// spans a suspension point.
    ///
    /// An `Arc` so the guard can be **owned**: a driver that cannot block issues its request on one
    /// loop iteration and receives the reply on a later one, so it has to hold the guard across them
    /// (see [`SlotHandle::begin_refresh`]).
    gate: Arc<tokio::sync::Mutex<()>>,
}

impl<T> Default for Slot<T> {
    fn default() -> Self {
        Self {
            value: ArcSwapOption::new(None),
            state: Mutex::new(SlotState::default()),
            gate: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
}

impl<T> std::fmt::Debug for Slot<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Slot").field("loaded", &self.value.load().is_some()).finish()
    }
}

/// A lock-free view of one key's slot — **hold this for the life of a connection**.
///
/// A native addition to the web mechanism, and the reason: live tick/book records resolve
/// `id → digits` against the table, so the decode path reads it once per inbound frame. Routing
/// each of those through the registry's map would put a whole fleet on one mutex.
/// [`load`](Self::load) is a bare atomic.
///
/// The handle keeps its slot alive, so [`Registry::remove`] only drops the registry's reference;
/// existing handles keep working against the detached slot.
pub struct SlotHandle<K, T> {
    key: K,
    slot: Arc<Slot<T>>,
}

impl<K: Clone, T> Clone for SlotHandle<K, T> {
    fn clone(&self) -> Self {
        Self { key: self.key.clone(), slot: self.slot.clone() }
    }
}

impl<K: std::fmt::Debug, T> std::fmt::Debug for SlotHandle<K, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlotHandle").field("key", &self.key).field("slot", &self.slot).finish()
    }
}

impl<K, T> SlotHandle<K, T> {
    pub fn key(&self) -> &K {
        &self.key
    }

    /// The current table, if loaded. **Lock-free** — this is the read the decode path uses; call it
    /// once per inbound frame, not once per record.
    pub fn load(&self) -> Option<Arc<T>> {
        self.slot.value.load_full()
    }

    /// Install `value` wholesale, resetting the reconcile clock.
    pub fn insert(&self, value: Arc<T>) {
        self.insert_aged(value, Duration::ZERO);
    }

    /// [`insert`](Self::insert) for a value that is already `age` old — a table read off disk.
    ///
    /// Installing a restored cache as if it were fresh would defer the reconcile a full interval past
    /// its real age, so a process that restarts often would never reconcile at all. Backdating the
    /// clock means a sufficiently old cache is due the moment it loads.
    pub fn insert_aged(&self, value: Arc<T>, age: Duration) {
        let mut st = self.slot.state.lock().unwrap_or_else(|e| e.into_inner());
        self.slot.value.store(Some(value));
        st.updated_at = Some(Instant::now().checked_sub(age).unwrap_or_else(Instant::now));
    }

    /// Install `value` only if the slot is **empty**, and report what the slot holds afterwards.
    ///
    /// Used to seed from disk: a running fleet's table is always at least as fresh as a file, so a
    /// seed must never overwrite one.
    pub fn seed_aged(&self, value: Arc<T>, age: Duration) -> Arc<T> {
        let mut st = self.slot.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(cur) = self.slot.value.load_full() {
            return cur;
        }
        self.slot.value.store(Some(value.clone()));
        st.updated_at = Some(Instant::now().checked_sub(age).unwrap_or_else(Instant::now));
        value
    }

    /// Install `value` unless the slot already holds an **equivalent version**, and report what the
    /// slot holds afterwards. The check and the store are one atomic step.
    ///
    /// This exists because the obvious spelling — `if load() is a different version { insert() }` —
    /// is check-then-act, and at a **cold fleet start** that is exactly when it races: N connections
    /// all find the slot empty, all insert, and each gets back *its own* copy of the table. They
    /// converge on the next `load`, so it is not a correctness bug, but it transiently retains N
    /// copies of the very thing the registry exists to hold once (multi-megabyte tables are typical).
    /// Holding the
    /// state lock across both steps makes concurrent publishers of one version share an `Arc`
    /// immediately.
    ///
    /// `same_version(current, candidate)` decides equivalence — a `symbols_hash` comparison for both
    /// platforms' symbol tables. Returning `false` always degrades to a plain last-writer-wins
    /// [`insert`](Self::insert).
    pub fn publish_if<F>(&self, value: Arc<T>, same_version: F) -> Arc<T>
    where
        F: FnOnce(&T, &T) -> bool,
    {
        let mut st = self.slot.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(cur) = self.slot.value.load_full() {
            if same_version(&cur, &value) {
                return cur; // same version — keep the shared Arc, drop the caller's copy
            }
        }
        self.slot.value.store(Some(value.clone()));
        st.updated_at = Some(Instant::now());
        value
    }

    /// Apply a push update, copy-on-write, deduplicated fleet-wide by `digest`.
    ///
    /// `digest` identifies the *batch*, not the table: every connection to the server receives a
    /// byte-identical frame, so the first submission wins and the rest are dropped before the clone.
    /// Returns whether *this* call did the merge — informational; a caller wanting the result should
    /// [`load`](Self::load) regardless, since another connection may have won.
    ///
    /// Buffered instead of applied while a fetch is in flight (module docs). Dropped when the slot is
    /// empty and no fetch is running: there is nothing to merge into, and the eventual fetch returns
    /// server state that already includes the change.
    pub fn merge<F>(&self, digest: u64, f: F) -> bool
    where
        F: FnOnce(&T) -> T + Send + 'static,
    {
        let mut st = self.slot.state.lock().unwrap_or_else(|e| e.into_inner());
        if st.seen.contains(&digest) {
            return false;
        }
        st.seen.push_back(digest);
        if st.seen.len() > DEDUP_WINDOW {
            st.seen.pop_front();
        }
        if st.fetching {
            st.deferred.push(Box::new(f)); // replayed onto the fetch result
            return true;
        }
        match self.slot.value.load_full() {
            Some(cur) => {
                self.slot.value.store(Some(Arc::new(f(&cur))));
                true
            }
            None => false,
        }
    }

    /// Apply a copy-on-write update, **undeduplicated**.
    ///
    /// [`merge`](Self::merge) is for a *server-global* push that every connection reports, so it
    /// collapses the fleet's copies to one. This is for an observation only *this* connection made —
    /// a fresh measurement from its own connect attempt — where there is
    /// nothing to deduplicate and the whole point is that each caller contributes something
    /// different.
    ///
    /// Does not touch the refresh clock: an observation refines the value in place, it cannot reveal
    /// what only a re-fetch can. Returns whether there was a value to update.
    ///
    /// An update racing a refresh is **lost** — the refresh installs a value it built before the
    /// update landed. Deliberate: the alternative is the fetch/push buffering machinery, and one
    /// sample dropped from a running estimate does not justify it.
    pub fn update<F>(&self, f: F) -> bool
    where
        F: FnOnce(&T) -> T,
    {
        // Held for the whole read-modify-write so two concurrent updates cannot lose one another.
        let _st = self.slot.state.lock().unwrap_or_else(|e| e.into_inner());
        match self.slot.value.load_full() {
            Some(cur) => {
                self.slot.value.store(Some(Arc::new(f(&cur))));
                true
            }
            None => false,
        }
    }

    /// Drop the table, keeping the slot. Clears the dedup ring too — a rebuilt table must not skip
    /// updates it never saw.
    pub fn invalidate(&self) {
        let mut st = self.slot.state.lock().unwrap_or_else(|e| e.into_inner());
        self.slot.value.store(None);
        st.seen.clear();
        st.updated_at = None;
        st.next_attempt = None;
    }

    /// Is this slot owed a periodic re-fetch? A pure lock-and-compare, safe to call at heartbeat rate
    /// from every connection in a fleet.
    pub fn is_refresh_due(&self, interval: Duration) -> bool {
        let loaded = self.slot.value.load().is_some();
        let st = self.slot.state.lock().unwrap_or_else(|e| e.into_inner());
        st.refresh_due(loaded, interval, Instant::now())
    }

    /// The reload gate for this key. Hold it across **any** table reload — a caller-driven forced
    /// reload included — so only one is ever in flight per server.
    ///
    /// Without it, two concurrent reloads race on a shared completion signal and each can resolve on
    /// the other's answer — a property of how the caller reports completion, not of the wire.
    pub fn gate(&self) -> Arc<tokio::sync::Mutex<()>> {
        self.slot.gate.clone()
    }

    /// **Claim** the periodic re-fetch, without holding a future across it.
    ///
    /// Returns the reload guard if this caller now owns an in-flight reconcile — in which case it
    /// **must** eventually call [`finish_refresh`](Self::finish_refresh) and drop the guard, including
    /// on a socket drop. Leaving the claim open parks every subsequent merge in the deferred list
    /// forever.
    ///
    /// This is [`refresh_if_due`](Self::refresh_if_due) split in two, for a driver that cannot block:
    /// the request goes out on one loop iteration and its reply arrives on a later one, so the guard is
    /// **owned** and travels with the driver's state.
    ///
    /// Claiming is **non-blocking on both counts**: a caller that loses the gate, or finds the table
    /// not yet due, returns `None` immediately rather than queueing. That is what keeps a fleet of N
    /// connections to one server to a single reconcile per interval — and what makes a caller-driven
    /// reload suppress the periodic one for its duration instead of colliding with it.
    pub fn begin_refresh(&self, interval: Duration) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        let guard = self.slot.gate.clone().try_lock_owned().ok()?;
        let loaded = self.slot.value.load().is_some();
        let mut st = self.slot.state.lock().unwrap_or_else(|e| e.into_inner());
        if !st.refresh_due(loaded, interval, Instant::now()) {
            return None; // dropping `guard` releases the gate for whoever is next
        }
        st.fetching = true;
        Some(guard)
    }

    /// Complete a [`begin_refresh`](Self::begin_refresh) claim.
    ///
    /// `Some(table)` installs it and replays any merges buffered during the window — the whole point
    /// of the claim, since the table is a snapshot taken *before* those pushes. `None` (a timeout, an
    /// error reply, or a socket drop) keeps the **old table serving** and backs off for
    /// `retry_after`: emptying the slot would take symbol ids away from a running connection, which
    /// is far worse than serving one that is a few hours stale. Buffered merges are replayed onto the
    /// survivor either way, or they would be silently lost.
    pub fn finish_refresh(&self, fetched: Option<T>, retry_after: Duration) {
        let mut st = self.slot.state.lock().unwrap_or_else(|e| e.into_inner());
        st.fetching = false;
        let deferred = std::mem::take(&mut st.deferred);
        match fetched {
            Some(mut table) => {
                for m in deferred {
                    table = m(&table);
                }
                self.slot.value.store(Some(Arc::new(table)));
                st.updated_at = Some(Instant::now());
                st.next_attempt = None;
            }
            None => {
                if let Some(cur) = self.slot.value.load_full() {
                    let mut next: Option<T> = None;
                    for m in deferred {
                        next = Some(m(next.as_ref().unwrap_or(&cur)));
                    }
                    if let Some(t) = next {
                        self.slot.value.store(Some(Arc::new(t)));
                    }
                }
                st.next_attempt = Some(Instant::now() + retry_after);
            }
        }
    }

    /// The periodic full re-fetch, run by whichever caller claims it first.
    ///
    /// **The claim is non-blocking.** N connections to one server all reach this on the same
    /// heartbeat; one takes the gate and fetches, the rest return `Ok(false)` immediately rather than
    /// queueing behind it. That is what keeps a fleet on one server to a single re-fetch per interval.
    ///
    /// Returns whether *this* call performed the refresh. `Ok(false)` means not due, or claimed
    /// elsewhere.
    ///
    /// Failure semantics differ from [`SlotHandle::get_or_fetch`], and the difference is load-bearing:
    /// there the slot is empty, so discarding buffered merges is correct — whatever fetch eventually
    /// succeeds already includes them. Here the **old table is still serving readers**, so the
    /// buffered merges are real updates that must land on it, and the old value must survive. A
    /// failed refresh keeps serving stale-but-valid data and backs off; it never empties the slot.
    pub async fn refresh_if_due<F, Fut, E>(
        &self,
        interval: Duration,
        retry_after: Duration,
        fetch: F,
    ) -> Result<bool, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        if !self.is_refresh_due(interval) {
            return Ok(false);
        }
        // try_lock, not lock: the loser has nothing to wait for — the winner's fetch *is* the refresh.
        let Ok(_gate) = self.slot.gate.clone().try_lock_owned() else {
            return Ok(false);
        };
        {
            let loaded = self.slot.value.load().is_some();
            let mut st = self.slot.state.lock().unwrap_or_else(|e| e.into_inner());
            if !st.refresh_due(loaded, interval, Instant::now()) {
                return Ok(false); // the gate holder before us just did it
            }
            // Buffering applies to a refresh exactly as to a first fetch: this snapshot is taken
            // server-side at T, so a push at T+ε must be buffered or the snapshot reverts it.
            st.fetching = true;
        }

        let fetched = fetch().await;

        let mut st = self.slot.state.lock().unwrap_or_else(|e| e.into_inner());
        st.fetching = false;
        let deferred = std::mem::take(&mut st.deferred);
        match fetched {
            Ok(mut table) => {
                for m in deferred {
                    table = m(&table);
                }
                self.slot.value.store(Some(Arc::new(table)));
                st.updated_at = Some(Instant::now());
                st.next_attempt = None;
                Ok(true)
            }
            Err(e) => {
                // Keep serving the old table — and replay onto it, or those pushes are lost until the
                // next successful reconcile.
                if let Some(cur) = self.slot.value.load_full() {
                    let mut next: Option<T> = None;
                    for m in deferred {
                        next = Some(m(next.as_ref().unwrap_or(&cur)));
                    }
                    if let Some(t) = next {
                        self.slot.value.store(Some(Arc::new(t)));
                    }
                }
                st.next_attempt = Some(Instant::now() + retry_after);
                Err(e)
            }
        }
    }

    /// The table, fetching it **once** across every racing caller.
    ///
    /// Merges submitted while the fetch is in flight are buffered and replayed onto its result, so a
    /// push cannot be reverted by a snapshot taken before it.
    ///
    /// A failed fetch leaves the slot empty — the next caller retries. Its buffered merges are
    /// discarded, which is correct: whatever fetch eventually succeeds reflects the server state
    /// *after* those pushes anyway.
    pub async fn get_or_fetch<F, Fut, E>(&self, fetch: F) -> Result<Arc<T>, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        self.get_or_fetch_aged(|| async { fetch().await.map(|t| (t, Duration::ZERO)) }).await
    }

    /// The whole lifecycle for a value with **no push stream**: fetch it when the slot is empty,
    /// re-fetch it when the clock says so, rate-limit failures — all claimed **once for a fleet**.
    ///
    /// [`get_or_fetch`](Self::get_or_fetch) + [`refresh_if_due`](Self::refresh_if_due) already cover
    /// a *symbol table*, whose loads are driven by the connection's own login. A value the registry
    /// fetches itself — an endpoint catalog refreshed on exhaustion, say — needs two things
    /// they do not give:
    ///
    /// - **Failure backoff on an empty slot.** `get_or_fetch` leaves a failed slot empty, so the next
    ///   caller in line fetches again; the gate serializes them, but a fleet cold-starting against a
    ///   failing source still costs one request per connection. Here a failure records
    ///   `next_attempt`, and callers arriving inside `retry_after` are answered without a request.
    /// - **Cancellation safety.** Neither of the other two survives its future being dropped
    ///   mid-fetch: `fetching` stays set, and the slot's refresh clock is then disabled forever. This
    ///   caller runs inside the reconnect supervisor's `select!` and *is* dropped on `close()`. So
    ///   this method never sets `fetching` — which costs nothing, because `fetching` exists only to
    ///   buffer merges and a value with no push stream has none (§3.4).
    ///
    /// Returns
    /// - `Ok(v)` — what the slot serves now: freshly fetched, freshly refreshed, or the previous
    ///   value surviving a failed/suppressed refresh (§3.3 — a stale list still connects);
    /// - `Err(Some(e))` — nothing to serve, and *this call's* fetch failed with `e`;
    /// - `Err(None)` — nothing to serve, and **no fetch was attempted** because a recent one failed.
    ///
    /// The claim is non-blocking for a caller that already has a value to serve and blocking for one
    /// that does not: the first has no reason to wait for a refresh, the second has nothing else to
    /// do.
    pub async fn get_or_refresh<F, Fut, E>(
        &self,
        interval: Duration,
        retry_after: Duration,
        fetch: F,
    ) -> Result<Arc<T>, Option<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        if let Some(v) = self.load() {
            if !self.is_refresh_due(interval) {
                return Ok(v);
            }
        }
        let _gate = match self.slot.gate.clone().try_lock_owned() {
            Ok(g) => g,
            Err(_) => match self.load() {
                // Loaded: the gate holder's fetch *is* our refresh, so serve what we have rather
                // than queue behind it. This is what keeps a herd to one request.
                Some(v) => return Ok(v),
                // Empty: nothing to serve, so wait for the holder's answer.
                None => self.slot.gate.clone().lock_owned().await,
            },
        };
        // Re-check under the gate — the holder before us may have just filled or refreshed it.
        let current = self.load();
        if let Some(v) = &current {
            if !self.is_refresh_due(interval) {
                return Ok(v.clone());
            }
        }
        {
            let st = self.slot.state.lock().unwrap_or_else(|e| e.into_inner());
            if st.next_attempt.is_some_and(|t| Instant::now() < t) {
                return current.ok_or(None);
            }
        }

        match fetch().await {
            Ok(value) => {
                let arc = Arc::new(value);
                let mut st = self.slot.state.lock().unwrap_or_else(|e| e.into_inner());
                self.slot.value.store(Some(arc.clone()));
                st.updated_at = Some(Instant::now());
                st.next_attempt = None; // recovery is immediate, not "after the last backoff"
                Ok(arc)
            }
            Err(e) => {
                self.slot.state.lock().unwrap_or_else(|e| e.into_inner()).next_attempt =
                    Some(Instant::now() + retry_after);
                // Keep serving whatever is there: stale is better than nothing for this value.
                self.load().ok_or(Some(e))
            }
        }
    }

    /// [`get_or_fetch`](Self::get_or_fetch) where the fetch also reports **how old** the value it
    /// produced already is — see [`insert_aged`](Self::insert_aged) for why that matters.
    pub async fn get_or_fetch_aged<F, Fut, E>(&self, fetch: F) -> Result<Arc<T>, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(T, Duration), E>>,
    {
        if let Some(v) = self.load() {
            return Ok(v);
        }
        let _gate = self.slot.gate.clone().lock_owned().await;
        if let Some(v) = self.load() {
            return Ok(v); // the caller that held the gate just filled it
        }
        self.slot.state.lock().unwrap_or_else(|e| e.into_inner()).fetching = true;

        let fetched = fetch().await;

        let mut st = self.slot.state.lock().unwrap_or_else(|e| e.into_inner());
        st.fetching = false;
        let deferred = std::mem::take(&mut st.deferred);
        match fetched {
            Ok((mut table, age)) => {
                for m in deferred {
                    table = m(&table);
                }
                let arc = Arc::new(table);
                self.slot.value.store(Some(arc.clone()));
                st.updated_at = Some(Instant::now().checked_sub(age).unwrap_or_else(Instant::now));
                Ok(arc)
            }
            Err(e) => Err(e),
        }
    }
}

/// A keyed set of shared, server-global tables.
///
/// One registry per platform, one slot per server key, one `Arc<T>` per slot. Construct one per
/// process and hand it to every connection; a connection derives its own key and reads through it, so
/// the application never selects a table itself.
#[derive(Debug)]
pub struct Registry<K, T> {
    slots: Mutex<HashMap<K, Arc<Slot<T>>>>,
}

impl<K, T> Default for Registry<K, T> {
    fn default() -> Self {
        Self { slots: Mutex::new(HashMap::new()) }
    }
}

impl<K, T> Registry<K, T>
where
    K: Eq + Hash + Clone,
    T: Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self::default()
    }

    /// A lock-free [`SlotHandle`] for `key`, creating the slot if needed. Take this **once** per
    /// connection — it is the only call that touches the registry's map lock.
    pub fn handle(&self, key: &K) -> SlotHandle<K, T> {
        let mut slots = self.slots.lock().expect("registry map poisoned");
        let slot = slots.entry(key.clone()).or_default().clone();
        SlotHandle { key: key.clone(), slot }
    }

    /// The table for `key` if one is loaded. Convenience — prefer [`handle`](Self::handle) on a hot
    /// path, since this takes the map lock.
    pub fn get(&self, key: &K) -> Option<Arc<T>> {
        let slot = self.slots.lock().expect("registry map poisoned").get(key)?.clone();
        slot.value.load_full()
    }

    /// See [`SlotHandle::insert`].
    pub fn insert(&self, key: &K, value: Arc<T>) {
        self.handle(key).insert(value);
    }

    /// See [`SlotHandle::merge`].
    pub fn merge<F>(&self, key: &K, digest: u64, f: F) -> bool
    where
        F: FnOnce(&T) -> T + Send + 'static,
    {
        self.handle(key).merge(digest, f)
    }

    /// See [`SlotHandle::invalidate`].
    pub fn invalidate(&self, key: &K) {
        let slot = self.slots.lock().expect("registry map poisoned").get(key).cloned();
        if let Some(slot) = slot {
            SlotHandle { key: key.clone(), slot }.invalidate();
        }
    }

    /// Forget `key`. Handles already taken keep working against the detached slot.
    pub fn remove(&self, key: &K) {
        self.slots.lock().expect("registry map poisoned").remove(key);
    }

    /// Number of keys with a table loaded — i.e. how many copies of the table are resident.
    pub fn len(&self) -> usize {
        self.slots
            .lock()
            .expect("registry map poisoned")
            .values()
            .filter(|s| s.value.load().is_some())
            .count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Every key with a table loaded.
    pub fn keys(&self) -> Vec<K> {
        self.slots
            .lock()
            .expect("registry map poisoned")
            .iter()
            .filter(|(_, s)| s.value.load().is_some())
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Every key with a slot, loaded or not.
    pub fn all_keys(&self) -> Vec<K> {
        self.slots.lock().expect("registry map poisoned").keys().cloned().collect()
    }
}

/// FNV-1a over a byte stream — the batch digest for [`SlotHandle::merge`]'s fleet dedup.
///
/// Not cryptographic and does not need to be: the inputs are the server's own frames, not adversarial
/// input, and the window is 16 entries wide. Matches the web client's helper so the two dedup paths
/// stay comparable.
pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// One connection's panic must not take out the fleet.
    ///
    /// The registry is process-wide, so a `lock().expect(...)` made a panic *while holding the slot
    /// lock* poison it, and every OTHER connection's next registry access would then panic too —
    /// cascading a single bad frame on one socket into a total outage. The locks recover instead.
    #[test]
    fn a_panic_under_the_slot_lock_does_not_poison_the_fleet() {
        let reg: Registry<String, u32> = Registry::new();
        let key = "server".to_string();
        reg.handle(&key).insert(Arc::new(1));

        // A merge closure that panics — the panic unwinds while the slot lock is held.
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            reg.handle(&key).merge(1, |_| panic!("simulated parser panic under the lock"));
        }))
        .is_err();
        std::panic::set_hook(prev);
        assert!(unwound, "the closure must actually have panicked under the lock");

        // A sibling connection must still be able to read and write the slot.
        let sibling = reg.handle(&key);
        assert_eq!(sibling.load().as_deref().copied(), Some(1), "the table still serves");
        sibling.insert(Arc::new(2));
        assert_eq!(sibling.load().as_deref().copied(), Some(2), "and the slot still accepts writes");
        assert!(sibling.merge(2, |v| *v + 1), "and still merges");
    }

    /// A stand-in table: a list of names, so a merge is observable.
    type Table = Vec<String>;

    fn reg() -> Registry<String, Table> {
        Registry::new()
    }

    fn key() -> String {
        "srv".into()
    }

    const ZERO: Duration = Duration::from_millis(0);
    const NEVER: Duration = Duration::from_secs(3600);

    fn push(name: &str) -> impl FnOnce(&Table) -> Table + Send + 'static {
        let name = name.to_string();
        move |t: &Table| {
            let mut n = t.clone();
            n.push(name);
            n
        }
    }

    #[test]
    fn a_handle_reads_without_the_map_lock_and_survives_removal() {
        let r = reg();
        let h = r.handle(&key());
        h.insert(Arc::new(vec!["EURUSD".into()]));
        assert!(Arc::ptr_eq(&h.load().unwrap(), &r.get(&key()).unwrap()));

        r.remove(&key());
        assert!(r.get(&key()).is_none(), "the registry forgot the key");
        assert!(h.load().is_some(), "an existing handle keeps working on the detached slot");
    }

    #[test]
    fn a_merge_is_copy_on_write_and_leaves_prior_readers_intact() {
        let r = reg();
        r.insert(&key(), Arc::new(vec!["EURUSD".into()]));
        let held = r.get(&key()).unwrap();

        assert!(r.merge(&key(), 1, push("BTCUSD")));

        assert_eq!(held.len(), 1, "a reader holding the old Arc is untouched");
        assert_eq!(r.get(&key()).unwrap().len(), 2);
    }

    #[test]
    fn the_same_batch_submitted_by_the_whole_fleet_merges_once() {
        let r = reg();
        r.insert(&key(), Arc::new(Vec::new()));
        let merges = Arc::new(AtomicUsize::new(0));

        for _ in 0..100 {
            let m = merges.clone();
            r.merge(&key(), 0xABCD, move |t: &Table| {
                m.fetch_add(1, Ordering::SeqCst);
                let mut n = t.clone();
                n.push("NEW".into());
                n
            });
        }
        assert_eq!(merges.load(Ordering::SeqCst), 1, "one frame, one merge");
        assert_eq!(r.get(&key()).unwrap().len(), 1);

        assert!(r.merge(&key(), 0x1234, push("OTHER")), "a different batch is not swallowed");
        assert_eq!(r.get(&key()).unwrap().len(), 2);
    }

    #[test]
    fn an_interleaved_duplicate_does_not_reapply_over_a_newer_batch() {
        let r = reg();
        r.insert(&key(), Arc::new(Vec::new()));
        assert!(r.merge(&key(), 1, push("A")));
        assert!(r.merge(&key(), 2, push("B")));
        assert!(!r.merge(&key(), 1, |_: &Table| panic!("must not re-run")));
        assert_eq!(*r.get(&key()).unwrap(), vec!["A".to_string(), "B".to_string()]);
    }

    #[test]
    fn a_merge_into_an_empty_slot_is_dropped() {
        let r = reg();
        assert!(!r.merge(&key(), 1, |t: &Table| t.clone()));
        assert!(r.get(&key()).is_none());
    }

    #[test]
    fn seed_never_overwrites_a_live_table() {
        let r = reg();
        let h = r.handle(&key());
        let live = Arc::new(vec!["LIVE".to_string()]);
        h.insert(live.clone());

        let effective = h.seed_aged(Arc::new(vec!["DISK".to_string()]), Duration::from_secs(1));
        assert!(Arc::ptr_eq(&effective, &live), "the fleet's table is at least as fresh as disk");
    }

    #[test]
    fn a_seeds_age_backdates_the_reconcile_clock() {
        // Without this a process that restarts often installs a stale cache as if it were fresh and
        // never reconciles.
        let r = reg();
        let h = r.handle(&key());
        h.seed_aged(Arc::new(vec!["OLD".into()]), Duration::from_secs(7200));
        assert!(h.is_refresh_due(Duration::from_secs(3600)), "an old cache is due immediately");

        let fresh = r.handle(&"other".to_string());
        fresh.seed_aged(Arc::new(vec!["NEW".into()]), Duration::ZERO);
        assert!(!fresh.is_refresh_due(Duration::from_secs(3600)));
    }

    #[test]
    fn a_merge_does_not_reset_the_reconcile_clock() {
        // A push keeps existing rows fresh but can never reveal a removal, so letting one postpone
        // the reconcile would let a busy server defer it forever.
        let r = reg();
        let h = r.handle(&key());
        h.insert(Arc::new(vec!["EURUSD".into()]));
        assert!(h.merge(1, push("X")));
        assert!(h.is_refresh_due(ZERO));
    }

    #[test]
    fn nothing_is_reconciled_before_it_is_loaded() {
        let r = reg();
        assert!(!r.handle(&key()).is_refresh_due(ZERO), "an empty slot is a fetch, not a refresh");
    }

    #[test]
    fn invalidate_clears_the_table_and_the_dedup_ring() {
        let r = reg();
        let h = r.handle(&key());
        h.insert(Arc::new(vec!["EURUSD".into()]));
        assert!(h.merge(7, push("X")));

        h.invalidate();
        assert!(h.load().is_none());
        assert_eq!(r.len(), 0);

        // The same digest must be applicable again against a rebuilt table.
        h.insert(Arc::new(vec!["EURUSD".into()]));
        assert!(h.merge(7, push("X")), "dedup history must not survive a rebuild");
    }

    #[test]
    fn keys_are_independent() {
        let r = reg();
        r.insert(&"a".to_string(), Arc::new(vec!["A".into()]));
        r.insert(&"b".to_string(), Arc::new(vec!["B".into()]));
        assert_eq!(r.len(), 2);
        let mut keys = r.keys();
        keys.sort();
        assert_eq!(keys, ["a", "b"]);
    }

    #[tokio::test]
    async fn a_fetch_happens_once_and_is_shared() {
        let r = reg();
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let a = r
            .handle(&key())
            .get_or_fetch::<_, _, ()>(|| async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(vec!["EURUSD".to_string()])
            })
            .await
            .unwrap();
        let b = r
            .handle(&key())
            .get_or_fetch::<_, _, ()>(|| async { panic!("must not refetch") })
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(&a, &b), "every caller gets the SAME Arc, not a copy");
    }

    #[tokio::test]
    async fn racing_callers_collapse_to_one_fetch() {
        let r = Arc::new(reg());
        let calls = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let (r, calls) = (r.clone(), calls.clone());
            tasks.push(tokio::spawn(async move {
                r.handle(&"srv".to_string())
                    .get_or_fetch::<_, _, ()>(|| async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(30)).await;
                        Ok(vec!["EURUSD".to_string()])
                    })
                    .await
                    .unwrap()
            }));
        }
        let mut tables = Vec::new();
        for t in tasks {
            tables.push(t.await.unwrap());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1, "8 racing callers, one fetch");
        assert!(tables.windows(2).all(|w| Arc::ptr_eq(&w[0], &w[1])));
    }

    #[tokio::test]
    async fn a_failed_fetch_leaves_the_slot_empty_for_the_next_caller() {
        let r = reg();
        assert!(r
            .handle(&key())
            .get_or_fetch::<_, _, &str>(|| async { Err("boom") })
            .await
            .is_err());
        assert!(r.get(&key()).is_none());
        let ok = r
            .handle(&key())
            .get_or_fetch::<_, _, &str>(|| async { Ok(vec!["X".into()]) })
            .await
            .unwrap();
        assert_eq!(ok.len(), 1, "a failure is not cached");
    }

    #[tokio::test]
    async fn a_push_racing_a_fetch_survives_the_snapshot() {
        // THE ordering hazard. The full table is a snapshot taken server-side BEFORE the push, so
        // applying the response naively reverts it — and there is no sequence number to notice.
        let r = Arc::new(reg());
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());

        let fetch = {
            let (r, started, release) = (r.clone(), started.clone(), release.clone());
            tokio::spawn(async move {
                r.handle(&"srv".to_string())
                    .get_or_fetch::<_, _, ()>(|| async move {
                        started.notify_one();
                        release.notified().await;
                        Ok(vec!["EURUSD".to_string()]) // the T snapshot — no NEWSYM in it
                    })
                    .await
                    .unwrap()
            })
        };

        started.notified().await;
        assert!(r.merge(&"srv".to_string(), 7, push("NEWSYM")), "buffered, not applied");
        release.notify_one();

        let table = fetch.await.unwrap();
        assert!(table.contains(&"NEWSYM".to_string()), "the buffered push replayed onto the snapshot");
        assert!(table.contains(&"EURUSD".to_string()));
    }

    #[tokio::test]
    async fn a_reconcile_replaces_the_table_only_once_it_is_due() {
        let r = reg();
        let h = r.handle(&key());
        h.insert(Arc::new(vec!["OLD".to_string(), "GONE".to_string()]));

        assert!(!h.is_refresh_due(NEVER));
        assert!(!h
            .refresh_if_due::<_, _, ()>(NEVER, ZERO, || async { panic!("not due") })
            .await
            .unwrap());

        // Due: the whole table is replaced, which is the only way a *removal* is ever noticed.
        assert!(h.is_refresh_due(ZERO));
        assert!(h
            .refresh_if_due::<_, _, ()>(ZERO, NEVER, || async { Ok(vec!["OLD".to_string()]) })
            .await
            .unwrap());
        assert_eq!(*h.load().unwrap(), vec!["OLD".to_string()], "GONE is gone");
    }

    #[tokio::test]
    async fn a_fleet_on_one_server_produces_one_reconcile_not_n() {
        let r = Arc::new(reg());
        r.insert(&key(), Arc::new(vec!["OLD".into()]));
        let calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());

        let winner = {
            let (r, calls, started, release) = (r.clone(), calls.clone(), started.clone(), release.clone());
            tokio::spawn(async move {
                r.handle(&"srv".to_string())
                    .refresh_if_due::<_, _, ()>(ZERO, NEVER, || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        started.notify_one();
                        release.notified().await;
                        Ok(vec!["NEW".into()])
                    })
                    .await
                    .unwrap()
            })
        };
        started.notified().await;

        // 99 more connections arrive mid-fetch. None may fetch, and none may block.
        for _ in 0..99 {
            assert!(!r
                .handle(&"srv".to_string())
                .refresh_if_due::<_, _, ()>(ZERO, NEVER, || async { panic!("second fetch") })
                .await
                .unwrap());
        }
        release.notify_one();
        assert!(winner.await.unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 1, "one re-fetch for the whole fleet");
        assert_eq!(*r.get(&key()).unwrap(), vec!["NEW".to_string()]);
    }

    #[tokio::test]
    async fn a_failed_reconcile_keeps_the_old_table_and_backs_off() {
        // Emptying the slot would take symbol ids away from a running connection — far worse than
        // serving one that is a few hours stale.
        let r = reg();
        let h = r.handle(&key());
        h.insert(Arc::new(vec!["OLD".into()]));

        let err = h
            .refresh_if_due::<_, _, _>(ZERO, NEVER, || async { Err::<Table, _>("boom") })
            .await
            .expect_err("the fetch failed");
        assert_eq!(err, "boom");
        assert_eq!(*h.load().unwrap(), vec!["OLD".to_string()], "still serving");
        assert!(!h.is_refresh_due(ZERO), "and not retried on the next heartbeat");
    }

    #[tokio::test]
    async fn a_push_racing_a_failed_reconcile_is_not_lost() {
        // Unique to a refresh: the old table stays in service, so buffered merges must land on it
        // rather than being discarded with the failed fetch.
        let r = Arc::new(reg());
        r.insert(&key(), Arc::new(vec!["EURUSD".to_string()]));
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());

        let refresh = {
            let (r, started, release) = (r.clone(), started.clone(), release.clone());
            tokio::spawn(async move {
                r.handle(&"srv".to_string())
                    .refresh_if_due::<_, _, _>(ZERO, NEVER, || async move {
                        started.notify_one();
                        release.notified().await;
                        Err::<Table, _>("boom")
                    })
                    .await
            })
        };
        started.notified().await;
        assert!(r.merge(&key(), 7, push("NEWSYM")));
        assert!(!r.get(&key()).unwrap().contains(&"NEWSYM".to_string()), "buffered, not applied");

        release.notify_one();
        assert!(refresh.await.unwrap().is_err());
        let table = r.get(&key()).unwrap();
        assert!(table.contains(&"NEWSYM".to_string()), "replayed onto the surviving table");
        assert!(table.contains(&"EURUSD".to_string()));
    }

    #[test]
    fn fnv1a_separates_batches_and_is_stable() {
        assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a(b"EURUSD:227:5"), fnv1a(b"EURUSD:227:5"));
        assert_ne!(fnv1a(b"EURUSD:227:5"), fnv1a(b"EURUSD:227:6"));
    }
}
