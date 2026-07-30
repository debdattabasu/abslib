//! Byte accounting for a **buffered** outbound path — the write half of a single-task driver.
//!
//! # The failure this exists to prevent
//!
//! The obvious way to write from a `select!` loop is to await the socket inside the command arm's
//! handler. That is harmless while the kernel send buffer has room, because a write is then just a memcpy
//! that never yields. But a peer that stops draining eventually closes its receive window regardless of
//! how small each frame is, and from that moment the await parks with no bound.
//!
//! The failure is **self-concealing**, which is what makes it dangerous rather than merely slow: read-idle
//! detection normally lives on a timer arm, and a task parked in a write cannot poll that arm. Nothing
//! left running can notice. A probe against a peer with a deliberately small `SO_RCVBUF` will sit `Open`
//! indefinitely.
//!
//! # The fix, and why it is not a timeout
//!
//! Bounding the write with a timeout *bounds* the head-of-line blocking instead of removing it: for up to
//! the bound, reads still cannot run. It also cannot be tightened, because an **elapsed-time** bound
//! cannot tell a legitimate large flush over a slow link — many seconds, healthy, moving bytes the whole
//! time — from a peer that has stopped reading, which takes forever while moving nothing.
//!
//! So instead: own an egress buffer, make enqueueing synchronous so it can never block, and when the
//! buffer crosses a **backpressure boundary**, stop draining the command channel. The stall then lands on
//! the caller — blocking at a bounded `mpsc` — which is where it belongs, instead of being absorbed by the
//! I/O loop. This is the shape Netty (`channelWritabilityChanged`) and Folly/proxygen
//! (`onWriteBufferHighWatermark` → `pauseIngress`) standardised on. In a `select!` loop the signal is just
//! a conditional arm, because the flag has no observer outside the task:
//!
//! ```ignore
//! let buffered = eg.buffered();
//! tokio::select! {
//!     biased;
//!     _ = ctrl.recv()                                  => { /* terminal: never gated */ }
//!     r = flush(&mut sink),        if buffered > 0      => { /* the ONLY arm that awaits a write */ }
//!     r = read.next()                                   => { /* inbound */ }
//!     c = cmds.recv(), if buffered < BOUNDARY           => { /* synchronous enqueue */ }
//!     _ = sleep_until(meter.deadline()), if buffered > 0 => { /* no-progress check */ }
//! }
//! ```
//!
//! Backpressure alone is not enough. If the peer never drains, the buffer stays over the boundary forever:
//! reads keep flowing, but nothing can ever be sent again — a wedged connection in nicer clothes. Hence
//! the second half, [`EgressMeter::stalled`], which measures **zero bytes accepted by the kernel** rather
//! than elapsed time. That distinction is what lets the bound be seconds instead of tens of seconds.
//!
//! # What this module actually holds
//!
//! Almost none of the above is code — it is a buffer and a `select!` arm, both of which belong to the
//! driver. What *is* subtle, and what every such driver would otherwise reimplement, is the arithmetic
//! tying three questions together:
//!
//! 1. **Has this specific frame reached the kernel?** Needed for at-most-once semantics. A request whose
//!    bytes are still buffered was provably never seen by the peer, so it is cleanly re-sendable; one the
//!    kernel has taken is in-flight-unknown. Netty draws exactly this line with its
//!    `flushedEntry`/`unflushedEntry` split and fails the two groups differently on close.
//! 2. **Is the peer still moving bytes?** The no-progress bound.
//! 3. **May we accept more work?** The boundary test.
//!
//! All three fall out of one monotonic pair. `enqueued` only grows; `flushed` is *derived*
//! (`enqueued - buffered`) rather than tracked separately, so it cannot drift from the buffer.
//!
//! # Why progress is sampled rather than observed
//!
//! With [`tokio_util::codec::FramedWrite`](https://docs.rs/tokio-util), `poll_flush` loops internally
//! until the buffer empties, returning `Pending` from the middle of that loop — so a partial flush advances
//! the buffer *without* the branch handler ever running. And while every `select!` arm is `Pending` the
//! enclosing loop body does not re-run either; only the branch futures are re-polled. Progress therefore
//! has to be read off the buffer length from whatever arm *does* complete, which is what
//! [`EgressMeter::observe`] is for. Call it from several arms; it is cheap and idempotent.
//!
//! One concrete trap, verified in tokio's source rather than inferred: **`tokio::io::BufWriter` cannot
//! support this at all.** It defers its `buf.drain(..written)` past the `ready!` in `flush_buf`, so its
//! buffer length stays *pinned* during a partial flush and is not a progress signal. `FramedWrite`
//! advances via `buf.advance(n)` immediately after each accepted write, so its length is usable. That
//! asymmetry is the reason to prefer `FramedWrite` here.
//!
//! # A caveat for encrypted or compressed transports
//!
//! If frames are encrypted with a chained stream cipher (or fed through a stateful compressor) at the
//! moment they enter the buffer, then **encrypting is a point of no return**: the keystream has advanced
//! over those bytes, so the only legal moves are "write them next, in order" or "destroy the connection".
//! Dropping one and continuing desyncs the peer permanently. Two consequences worth stating, because both
//! are easy to get wrong:
//!
//! - A stalled buffer **must** tear the connection down. It can never discard-and-continue.
//! - Anything held for possible re-send must hold **plaintext**, and the transform must happen only at the
//!   instant of committing to the buffer. Putting the cipher inside an `Encoder` makes that structural
//!   rather than a convention, since `Sink::start_send` is then the only thing that can invoke it.
//!
//! If the transport is TLS, note that "flushed" degrades in meaning: `FramedWrite`'s buffer length tells
//! you what the TLS layer accepted, not what the kernel took, and TLS implementations buffer internally.
//! A frame still in *your* buffer is definitively unsent, which is the useful half; beyond that the
//! boundary is fuzzier than on a plain socket.
//!
//! The counters are `u64` byte totals: at a sustained 1 Gbit/s a wrap needs ~4700 years.

use std::time::Duration;

use tokio::time::Instant;

/// A reasonable default egress backpressure boundary, in bytes.
///
/// Pick your own from the largest **legitimate** burst the driver itself generates, rather than from a
/// round number. The usual candidate is a post-reconnect flush of held requests: if a driver may replay
/// up to N queued frames at once, the boundary should sit above `N × frame_size`, so a healthy reconnect
/// never pauses command intake. 256 KB covers a 256-frame replay of ~800 B frames with room to spare.
///
/// This is a *soft* threshold, not a capacity — `Sink::start_send` may overshoot it by one frame — which
/// is precisely why no "frame larger than the buffer" special case is needed.
pub const DEFAULT_EGRESS_BOUNDARY: usize = 256 * 1024;

/// A reasonable default no-progress bound for the egress buffer.
///
/// Far tighter than an elapsed-time write timeout could safely be, and that is the point: this measures
/// *zero bytes accepted by the kernel* within the window, not total time spent writing. A slow link still
/// moves bytes every few milliseconds, so it never trips. An elapsed-time bound has to be tens of seconds
/// to avoid killing a large flush over a bad link (a 207 KB flush at 100 Kbit/s legitimately takes ~17 s);
/// discriminating on progress instead is what buys the order of magnitude.
pub const DEFAULT_EGRESS_STALL_MS: u64 = 5_000;

/// Cumulative byte accounting for one connection's egress buffer.
///
/// ```
/// # use abslib::egress::EgressMeter;
/// # use std::time::Duration;
/// # use tokio::time::Instant;
/// # #[tokio::main(flavor = "current_thread")] async fn main() {
/// let mut meter = EgressMeter::new(Duration::from_secs(5));
/// let now = Instant::now();
///
/// // Two frames encoded into the buffer. `enqueue` returns the cumulative offset each one ENDS at —
/// // keep it beside whatever correlation state the frame carries.
/// let first = meter.enqueue(100, now);
/// let second = meter.enqueue(50, now);
/// assert_eq!((first, second), (100, 150));
///
/// // The kernel took 120 of the 150. `flushed` is DERIVED from what is still buffered, so it can
/// // never drift from the buffer.
/// let flushed = meter.observe(30, now);
/// assert_eq!(flushed, 120);
///
/// // So frame 1 is on the wire and frame 2 is not — which is what makes at-most-once exact: on a
/// // teardown, frame 2 was provably never seen and is cleanly re-sendable.
/// assert!(first <= flushed && second > flushed);
///
/// // Nothing outstanding is never "stalled", however long it has been idle.
/// meter.observe(0, now);
/// assert!(!meter.stalled(0, now));
/// # }
/// ```
///
/// `enqueued` counts every byte handed to the buffer; `flushed` is recomputed from the buffer's current
/// length, so the two can never disagree about what is outstanding. Frames are identified by the
/// cumulative offset at which they *end* — [`enqueue`](Self::enqueue) returns it, and a frame has
/// reached the kernel once [`flushed`](Self::flushed) is `>=` that offset.
#[derive(Debug)]
pub struct EgressMeter {
    enqueued: u64,
    flushed: u64,
    last_progress: Instant,
    stall_after: Duration,
}

impl EgressMeter {
    /// `stall_after` is the no-progress bound; see [`DEFAULT_EGRESS_STALL_MS`].
    pub fn new(stall_after: Duration) -> Self {
        Self { enqueued: 0, flushed: 0, last_progress: Instant::now(), stall_after }
    }

    /// Account for `n` bytes just appended to the buffer. Returns the cumulative offset at which those
    /// bytes **end** — keep it alongside whatever correlation state the frame carries.
    ///
    /// Starts the stall clock when the buffer was previously drained. Without that, a connection idle
    /// for longer than `stall_after` would have a deadline already in the past the instant it enqueued
    /// its next frame, and would tear itself down before the kernel had been given a chance to take
    /// anything.
    pub fn enqueue(&mut self, n: usize, now: Instant) -> u64 {
        if self.flushed == self.enqueued {
            self.last_progress = now;
        }
        self.enqueued += n as u64;
        self.enqueued
    }

    /// Recompute `flushed` from the buffer's current length, stamping progress if it advanced. Returns
    /// the new flushed total, i.e. every frame ending at or before it is now on the wire.
    ///
    /// Cheap and idempotent — call it from any arm that runs, and in particular before dispatching
    /// inbound frames, so a reply can never be handled before its own request has been promoted.
    pub fn observe(&mut self, buffered: usize, now: Instant) -> u64 {
        let flushed = self.enqueued - buffered as u64;
        if flushed > self.flushed {
            self.flushed = flushed;
            self.last_progress = now;
        }
        self.flushed
    }

    /// Bytes the kernel has accepted. Monotonic.
    pub fn flushed(&self) -> u64 {
        self.flushed
    }

    /// Bytes handed to the buffer. Monotonic.
    pub fn enqueued(&self) -> u64 {
        self.enqueued
    }

    /// When the current outstanding bytes will be declared stuck, absent further progress. Arm a
    /// `sleep_until` on this — only meaningful while something is buffered.
    pub fn deadline(&self) -> Instant {
        self.last_progress + self.stall_after
    }

    /// The peer has taken nothing for `stall_after` while bytes were outstanding.
    ///
    /// A teardown is **mandatory** on this, never a retry: the frames in the buffer were encrypted with
    /// a chained stream cipher, so the keystream has already advanced over them. Dropping them and
    /// continuing on the same socket would desync the peer's receive cipher permanently. Encrypting is
    /// the point of no return — the only two legal moves are "write these bytes next" and "destroy the
    /// connection".
    pub fn stalled(&self, buffered: usize, now: Instant) -> bool {
        buffered > 0 && now >= self.deadline()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn flushed_is_derived_from_the_buffer_not_tracked_separately() {
        let mut m = EgressMeter::new(Duration::from_secs(5));
        let now = Instant::now();

        let a = m.enqueue(100, now);
        let b = m.enqueue(50, now);
        assert_eq!((a, b), (100, 150), "offsets are cumulative frame ENDS");
        assert_eq!(m.observe(150, now), 0, "nothing drained yet");

        // The kernel took 120 of the 150: frame `a` is on the wire, `b` is only partly.
        assert_eq!(m.observe(30, now), 120);
        assert!(120 >= a && 120 < b, "a is promotable, b is not");

        assert_eq!(m.observe(0, now), 150, "fully drained");
        assert_eq!(m.enqueued(), m.flushed(), "and the two totals meet");
    }

    #[tokio::test(start_paused = true)]
    async fn observe_never_walks_flushed_backwards() {
        // `enqueue` grows the buffer, so a naive `flushed = enqueued - buffered` reading taken right
        // after an enqueue must not look like the peer un-received something.
        let mut m = EgressMeter::new(Duration::from_secs(5));
        let now = Instant::now();
        m.enqueue(100, now);
        assert_eq!(m.observe(0, now), 100);
        m.enqueue(100, now);
        assert_eq!(m.observe(100, now), 100, "still 100 flushed, not 0");
        assert_eq!(m.flushed(), 100);
    }

    #[tokio::test(start_paused = true)]
    async fn a_peer_taking_nothing_trips_the_bound_and_any_progress_resets_it() {
        let mut m = EgressMeter::new(Duration::from_secs(5));
        m.enqueue(1000, Instant::now());

        tokio::time::advance(Duration::from_secs(4)).await;
        assert!(!m.stalled(1000, Instant::now()), "4s of no progress is within the bound");

        // One byte accepted is progress: the clock restarts.
        m.observe(999, Instant::now());
        tokio::time::advance(Duration::from_secs(4)).await;
        assert!(!m.stalled(999, Instant::now()), "the bound is per-stall, not per-frame");

        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(m.stalled(999, Instant::now()), "6s with nothing accepted is a stalled peer");
    }

    #[tokio::test(start_paused = true)]
    async fn an_empty_buffer_is_never_stalled_however_long_it_has_been_idle() {
        let mut m = EgressMeter::new(Duration::from_secs(5));
        m.enqueue(10, Instant::now());
        m.observe(0, Instant::now());

        tokio::time::advance(Duration::from_secs(3600)).await;
        assert!(!m.stalled(0, Instant::now()), "nothing outstanding cannot be stuck");

        // ...and the first frame after a long idle gets a full window, rather than inheriting a
        // deadline that elapsed an hour ago.
        m.enqueue(10, Instant::now());
        assert!(!m.stalled(10, Instant::now()), "the clock starts when the buffer stops being empty");
        assert_eq!(m.deadline(), Instant::now() + Duration::from_secs(5));
    }
}
