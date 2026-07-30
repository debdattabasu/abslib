# CLAUDE.md — orientation for Claude working in abslib

Async networking primitives for **single-task protocol drivers**: one tokio task owns one connection and
runs a `select!` loop over the socket, a command channel, and some timers. `README.md` is the user-facing
introduction and is accurate — read it first, it is not marketing.

Three modules, no framework, no glue:

| module | gates | mechanism |
|---|---|---|
| `egress` | how much outbound data we accept | byte watermark + a no-progress deadline |
| `limits` | how many of one request kind are in flight | one fair semaphore per request kind |
| `registry` | how often shared state is re-fetched | refresh clock + single-flight + merge dedup |

```
cargo test              # 31 unit + 2 doctests
cargo clippy --all-targets
cargo doc --no-deps     # must be warning-clean
```

All three must stay warning-clean. The crate is public and intended for crates.io, so a rustdoc warning is
a broken link on docs.rs.

## The one premise everything derives from

**A `select!` branch _handler_ runs to completion.** Branch futures are polled concurrently, but once one
wins, its body holds the task — no other arm is polled until it returns. So awaiting inside a handler
stalls reads, writes, timers **and** shutdown together, and does it *silently*, because liveness detection
lives on a timer arm that a stalled task cannot poll.

Hence the rule every module here obeys, and which any new module must obey to belong:

> **Gate at the point of accepting work. Never sleep inside a handler.**

If a proposed addition does not fit that sentence, it probably belongs in a consumer, not here.

## Hard rule: this crate stays protocol-free

**This repo is public. Its consumers are not.** That asymmetry is the whole reason the crate exists in this
shape, and it is the one thing not to erode:

- **No protocol, vendor, venue, or product names** anywhere — code, comments, tests, commit messages.
- **No measured tables.** A concurrency ceiling or a rate is a fact about someone's server; it belongs in
  the client that measured it. This crate ships the *mechanism* and the advice on how to measure.
- **No framing and no wire semantics.** Those are a `tokio_util::codec` `Encoder`/`Decoder` and the client.
- **No credentials, hosts, or fixtures.** Obviously, but: the tests here are pure and must stay pure.

**The disclosure surface is the docs, not the code.** This was the actual lesson when these modules were
extracted: the Rust was already generic, and every leak was in a doc comment — references to specific
command numbers, error codes, brokers, and internal audit documents. When you touch a doc comment here,
re-read it as a stranger. Keep the *reasoning* ("a protocol that fails rather than queues the excess"),
drop the *identifiers*.

Before any commit, sweep the tree **and the history**:

```sh
# Build the alternation from your consumers' vendor / protocol / venue names, their error-code
# spellings, and their credential-file names. Deliberately not spelled out here: this file would then
# contain the very strings it exists to keep out.
PAT='name1|name2|...'
grep -rniE "$PAT" src/ README.md CLAUDE.md
git log --format=%B | grep -niE "$PAT"   # history is published too
```

Illustrative domain words are fine and make the docs concrete — "a per-instrument request", "a trade, say"
— because they name a shape, not a system.

## The docs are the product, as much as the code

Each of these mechanisms was written three times in three unrelated clients before being pulled out, and
**two of them were written wrongly first**. The module docs record that, on purpose:

- `egress` — the bound started as a 30 s timeout on the write. That *bounds* head-of-line blocking instead
  of removing it, and cannot be tightened, because elapsed time cannot separate a healthy slow flush from a
  peer that stopped reading. Measuring bytes-the-kernel-accepted is what buys the order of magnitude.
- `limits` — the ceilings started from the measured allowance, which turned out not to be reliably usable.

When you change behaviour here, update the *why*, not just the *what*. A future reader re-deriving a wrong
turn we already took is the failure this crate is meant to prevent. Keep the distinction between what is
**measured**, what is **chosen**, and what is **verified in a dependency's source** (there are several of
the third kind — e.g. `BufWriter`'s deferred `drain`, `FramedWrite`'s immediate `advance`; both were read
out of tokio, not inferred, and the docs say so).

## API stability and the consumers

Pre-1.0, and consumed by several private clients via **path dependencies**. Two consequences:

1. **A signature change here breaks them at build time, silently from this repo's point of view.** There is
   no CI spanning the set. After any API change, build the consumers before considering the work done.
2. **Do not freeze the API prematurely.** It is deliberately unpublished-to-crates.io-and-pre-1.0 rather
   than versioned optimistically; this design has already been revised once in a way that would have been a
   breaking change.

`registry` has the widest blast radius — it is the oldest of the three and the most deeply used.

## Testing conventions

- **Virtual clock.** Anything with a deadline uses `#[tokio::test(start_paused = true)]` plus
  `tokio::time::advance`, never real sleeps. `test-util` is a dev-dependency for exactly this; nothing in
  `src/` calls `pause()`.
- **Test the mechanism, not a mock of it.** `registry`'s tests drive real concurrent tasks through the
  single-flight gate; `limits` asserts FIFO wake order by serializing enqueue and checking arrival order.
- **Mutation-check anything load-bearing.** Disable the mechanism and confirm the test fails. A test that
  passes with the feature turned off is worse than no test, because it reads as coverage. Two tests in the
  consumers were found worthless exactly this way.
- Doctests are compile-checked API demonstrations — keep them working; they are the first thing a reader
  sees on docs.rs.

## Dependencies

Kept deliberately thin: `tokio` (`sync` + `time` only, `default-features = false`) and `arc-swap`. Adding a
dependency to a low-level crate everything else depends on is a real cost — argue it, don't assume it. In
particular **do not** add `tokio-util` here: framing is the consumer's job, and this crate must not have an
opinion about it.
