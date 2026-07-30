# abslib

Async networking primitives for **single-task protocol drivers**.

If you have written a binary-protocol client on tokio, you have probably written all three of these, and
possibly written one of them wrongly first. That is why they are here.

Not on crates.io yet — see [Status](#status):

```toml
[dependencies]
abslib = { git = "https://github.com/debdattabasu/abslib" }
```

## The shape this is for

One tokio task owns one connection, running a `select!` loop over the socket, a command channel from the
caller-facing handle, and some timers:

```rust,ignore
loop {
    tokio::select! {
        biased;
        _ = ctrl.recv()                 => break,          // terminal, never gated
        c = orders.recv(), if writable  => enqueue(c)?,     // synchronous; cannot block
        r = flush(&mut sink), if buffered > 0 => r?,        // the ONLY arm that awaits a write
        f = frames.next()               => dispatch(f?)?,   // inbound
        c = data.recv(), if writable    => enqueue(c)?,
        _ = sleep_until(stall_deadline) => break,           // no-progress check
        _ = heartbeat.tick()            => keepalive()?,
    }
}
```

One task per connection — an `asio::strand` in effect — is not a shortcut. It is what makes stateful
transports sound: a stream cipher, a compression context, or a sequence counter must see every byte
exactly once, in order, and a single owner gives that for free with no locks.

Everything here follows from one property of that shape:

> **A `select!` branch _handler_ runs to completion.** The branch futures are polled concurrently, but
> once one wins, its body holds the task. No other arm is polled until it returns.

So anything that awaits inside a handler stalls reads, writes, timers **and** shutdown together. Worse, it
stalls them *silently*: liveness detection normally lives on a timer arm, and a stalled task cannot poll
its own detector. A connection wedged this way reports itself healthy indefinitely.

Hence the rule all three modules apply:

> **Gate at the point of accepting work. Never sleep inside a handler.**

| module | gates | on |
|---|---|---|
| [`egress`](src/egress.rs) | how much outbound data we accept | a byte watermark, plus a no-progress deadline |
| [`limits`](src/limits.rs) | how many of one request kind are in flight | a fair semaphore, per request kind |
| [`pacer`](src/pacer.rs) | how many of one request kind per second | a token bucket, per request kind |
| [`registry`](src/registry.rs) | how often shared state is re-fetched | a refresh clock plus single-flight |

## `egress` — outbound backpressure that isn't a timeout

Buffer outbound frames, and when the buffer crosses a **boundary**, stop draining your command channel.
The stall then lands on the caller — blocking at a bounded `mpsc` — instead of being absorbed by the I/O
loop. This is Netty's `channelWritabilityChanged` and proxygen's `onWriteBufferHighWatermark`.

`EgressMeter` is the byte accounting that makes it work, and it answers three questions from one
monotonic pair:

```rust
use abslib::EgressMeter;
use std::time::Duration;
# use tokio::time::Instant;
# #[tokio::main(flavor = "current_thread")] async fn main() {
let mut meter = EgressMeter::new(Duration::from_secs(5));
# let now = Instant::now();

let first  = meter.enqueue(100, now);   // cumulative offset this frame ENDS at
let second = meter.enqueue(50, now);

let flushed = meter.observe(30, now);   // 30 bytes still buffered => 120 accepted
assert!(first <= flushed && second > flushed);
// frame 1 is on the wire; frame 2 provably is not, so it is cleanly re-sendable
# }
```

**Why not just a timeout on the write?** Because a timeout *bounds* head-of-line blocking instead of
removing it — for up to the bound, reads still cannot run. And it cannot be tightened: an elapsed-time
bound cannot tell a legitimate large flush over a slow link (many seconds, healthy, moving bytes the whole
time) from a peer that stopped reading. Measuring *bytes the kernel accepted* discriminates them, which is
what lets the bound be seconds instead of tens of seconds.

**Why not `tokio::io::BufWriter`?** Verified in tokio's source, not inferred: it defers its
`buf.drain(..written)` past the `ready!` in `flush_buf`, so its buffer length stays *pinned* during a
partial flush and is not a progress signal at all. `tokio_util::codec::FramedWrite` advances via
`buf.advance(n)` immediately after each accepted write, so its length is usable.

## `limits` — per-kind concurrency ceilings

For protocols that cap concurrent requests of some kind and **fail rather than queue** the excess — often
with a generic error you cannot distinguish from a real fault:

```rust
use abslib::RequestLimits;
use std::collections::HashMap;
# #[tokio::main(flavor = "current_thread")] async fn main() {
let limits = RequestLimits::new(&HashMap::from([(11, 1)]));

// Acquire BEFORE building or enqueueing, inside the caller's own deadline, so the wait counts
// against the budget the caller asked for.
let permit = limits.acquire(11).await;

// Uncapped kinds never wait and yield no permit: the fast path is one hash lookup.
assert!(limits.acquire(22).await.is_none());

drop(permit);   // owned permit: returns however the request ends, including a dropped future
# }
```

The failure does not disappear — it becomes a *better* failure. A timeout after waiting your turn is
honest and safe to retry; a generic error from a refused concurrent request is neither.

**Measure the ceilings; don't guess.** And don't assume the measured allowance is *usable*: a measured
allowance of 2 may still lose requests intermittently, varying by access pattern and between sessions,
which is consistent with server-side load rather than any rule a client could schedule around. Depth 1 is
often the only depth that never fails.

## `pacer` — per-kind rate limiting

The sibling of `limits`, and constantly confused with it. `limits` bounds how many are **in flight**;
`pacer` bounds how many per **second**. They are not interchangeable: a semaphore cannot enforce a rate,
because rate ≈ concurrency ÷ latency and latency is not yours to control — a cap of 2 permits 20/s against a
100 ms server and 200/s against a 10 ms one.

A bucket rather than a minimum interval, for two reasons found by measuring:

- Servers commonly tolerate a **burst** and object only to the sustained rate. Even spacing throws that
  headroom away — a four-way fan-out pays three artificial delays for something the server takes at once.
- Even spacing taxes every request when nothing is near the limit: at 45/s that is ~22 ms each, so a burst
  of ten costs ≥220 ms of pure waiting.

```rust
use abslib::{Pacer, Rate};
use std::collections::HashMap;
# #[tokio::main(flavor = "current_thread")] async fn main() {
// Kind 11: 10/s sustained, but 4 may go at once from idle.
let pacer = Pacer::new(&HashMap::from([(11, Rate::new(10.0, 4))]));

pacer.acquire(11).await;   // free — the bucket starts full
// ...three more are also free, then the rate binds.

// `Rate::spaced(n)` is strict 1/n spacing, for a server that really wants no burst.
# }
```

Wait in the **caller's** task, before building the request and inside the caller's own deadline — same
placement as `limits`, and for the same reason: pacing inside a driver means either sleeping in a `select!`
handler or parking the command in a queue something else must remember to retry. Where a driver must pace
its own traffic, gate the loop on `ready_at` instead.

## `registry` — shared tables, refreshed safely

For reference tables that belong to the **server**, not the connection. Held per connection, a fleet of N
sessions keeps N copies of a multi-megabyte table, fetches it N times at startup, and applies every pushed
update N times — each application cloning the whole table.

Sharing it introduces four hazards, and there is a behaviour for each:

- **Fleet dedup** — every connection receives the *same* pushed update and submits it. Merging is
  idempotent so the content is fine, but each merge clones the table. A digest collapses N to one.
- **Fetch/update buffering** — the subtle one. A full table is a snapshot taken at time *T*; an update
  applied at *T+ε* is silently reverted when the *T* snapshot lands, and protocols rarely carry a sequence
  number that would let you notice. Updates arriving during a fetch are buffered and replayed onto it.
- **Single-flight** — N connections miss the same key in the same instant; the losers await the winner.
- **Reconcile clock** — push streams often signal additions but not *removals*, so a periodic full
  re-fetch is the only way a deletion is observed. A merge deliberately does **not** reset that clock, or
  a busy server could defer the reconcile forever.

Reads are lock-free `ArcSwap` loads. Writes hold the slot's mutex, which is what keeps "is a fetch in
flight?" and "apply this update" one atomic decision. The fetch `await` is held under a *separate*
`tokio::sync::Mutex`, so **no std lock ever spans a suspension point**.

## What this is not

Not a framework, and deliberately not [tower](https://docs.rs/tower). Tower's abstraction is
`Service<Request> -> Future<Response>`, which starts one layer *above* the transport: it has no notion of
bytes, of what the kernel accepted, or of ordering within a connection. These primitives all live below
that line, and compose fine underneath it — for request-level middleware (retries, load shedding,
balancing) tower is the right tool.

Nothing here knows what a connection *carries*. Framing belongs in a
[`tokio_util::codec`](https://docs.rs/tokio-util) `Encoder`/`Decoder`; wire semantics belong in the client;
measured limits belong in the client that measured them.

## Why it exists

Each of these was written three times across three unrelated protocol clients before being pulled out. Two
were written *wrongly* at least once first — the egress bound started as a timeout, and the concurrency
ceilings started from a measured allowance that turned out not to be reliably usable.

So the code is the smaller half of what is shared here. The module docs carry the reasoning: what the
failure looks like, why the obvious fix is wrong, and which parts are measured versus chosen. That is the
part worth not rediscovering.

## Status

Pre-1.0 and **not yet published to crates.io**, deliberately: the design has already been revised once in
a way that would have been a breaking change, and freezing a public API around something that new is how
you end up maintaining a mistake. It is consumed by three clients via git/path dependencies, and will be
published once the API has held still for a while.

The mechanisms themselves are in production use and tested; it is the *shape of the API* that is not
settled.

## License

MIT OR Apache-2.0.
