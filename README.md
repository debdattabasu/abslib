# abslib

Async networking primitives for **single-task protocol drivers**.

The shape these are built for: one tokio task owns one connection, running a `select!` loop over the
socket, a command channel from the caller-facing handle, and some timers. One task per connection — an
`asio::strand` in effect — is what makes stateful transports sound: a stream cipher, a compression
context or a sequence counter must see every byte exactly once, in order, and a single owner gives that
with no locks.

Everything here follows from one property of that shape:

> **A `select!` branch *handler* runs to completion.** The branch futures are polled concurrently, but
> once one wins, its body holds the task. No other arm is polled until it returns.

So anything that awaits inside a handler stalls reads, writes, timers and shutdown *together* — and it
does so silently, because liveness detection usually lives on a timer arm and a stalled task cannot poll
its own detector. Hence the rule every module here applies:

> **Gate at the point of accepting work. Never sleep inside a handler.**

| module | gates | on |
|---|---|---|
| `egress` | how much outbound data we accept | a byte watermark, plus a no-progress deadline |
| `limits` | how many of one request kind are in flight | a fair semaphore, per request kind |
| `registry` | how often shared state is re-fetched | a refresh clock plus single-flight |

## What this is not

Not a framework, and deliberately not [tower](https://docs.rs/tower). Tower's abstraction is
`Service<Request> -> Future<Response>`, which starts one layer *above* the transport: it has no notion of
bytes, of what the kernel accepted, or of ordering within a connection. These primitives all live below
that line. For request-level middleware — retries, load shedding, balancing — tower is the right tool and
composes fine on top of a driver built with these.

Nothing here is protocol-specific, and nothing here knows what a connection *carries*. Framing belongs in
a [`tokio_util::codec`](https://docs.rs/tokio-util) `Encoder`/`Decoder`; wire semantics belong in the
client.

## Why it exists

Each of these was written three times across three unrelated protocol clients before being pulled out.
Two of them were written *wrongly* at least once first — the egress bound started as a timeout, which
bounds head-of-line blocking instead of removing it and cannot be tightened; the concurrency ceilings
started from a measured allowance that turned out not to be reliably usable.

So the code is the smaller half of what is shared here. The module docs carry the reasoning: what the
failure looks like, why the obvious fix is wrong, and which parts are measured versus chosen. That is the
part worth not rediscovering.

## Status

Pre-1.0 and shaped by three consumers so far. The API may move.

## License

MIT OR Apache-2.0.
