//! Async networking primitives for **single-task protocol drivers**.
//!
//! The shape these are built for: one tokio task owns one connection, running a `select!` loop over the
//! socket, a command channel from the caller-facing handle, and some timers. That "one task per
//! connection" design — an `asio::strand` in effect — is not a shortcut. It is what makes stateful
//! transports sound: a stream cipher, a compression context, or a sequence counter must see every byte
//! exactly once, in order, and a single owner gives that for free with no locks.
//!
//! Everything here follows from one property of that shape:
//!
//! > **A `select!` branch *handler* runs to completion.** The branch futures are polled concurrently, but
//! > once one wins, its body holds the task. No other arm is polled until it returns.
//!
//! So anything that awaits inside a handler stalls reads, writes, timers and shutdown *together*. Worse,
//! it stalls them silently: liveness detection usually lives on a timer arm, and a stalled task cannot
//! poll its own detector. Every module here exists because of that, and they all apply the same rule:
//!
//! > **Gate at the point of accepting work. Never sleep inside a handler.**
//!
//! | module | gates | on |
//! |---|---|---|
//! | [`egress`] | how much outbound data we accept | a byte watermark, plus a no-progress deadline |
//! | [`limits`] | how many of one request kind are in flight | a fair semaphore, per request kind |
//! | [`pacer`] | how many of one request kind per second | a token bucket, per request kind |
//! | [`registry`] | how often shared state is re-fetched | a refresh clock plus single-flight |
//!
//! # What these are not
//!
//! Not a framework, and deliberately not [`tower`](https://docs.rs/tower). Tower's abstraction is
//! `Service<Request> -> Future<Response>`, which starts one layer *above* the transport: it has no notion
//! of bytes, of what the kernel accepted, or of ordering within a connection. These primitives are all
//! below that line. If you want request-level middleware — retries, load shedding, load balancing — tower
//! is the right tool and composes fine on top of a driver built with these.
//!
//! Nothing here is protocol-specific, and nothing here knows what a connection *carries*. Framing belongs
//! in a [`tokio_util::codec`](https://docs.rs/tokio-util) `Encoder`/`Decoder`; wire semantics belong in
//! the client. What is shared is the small set of mechanisms that every such driver otherwise
//! re-implements — and, more valuably, the reasoning about *why* each one has the shape it does, which is
//! kept in the module docs rather than rediscovered per project.

pub mod egress;
pub mod limits;
pub mod pacer;
pub mod registry;

pub use egress::EgressMeter;
pub use limits::RequestLimits;
pub use pacer::{Pacer, Rate};
pub use registry::{Registry, SlotHandle};
