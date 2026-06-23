# Bandwidth / Throughput Modeling for snailmail

A technical survey of bandwidth models for a network-simulation benchmark
harness, and a recommendation on whether snailmail should ship more than one.

Audience: the maintainer. Assumes familiarity with TCP, object stores, and the
existing `SharedPipe` / `AsyncSharedPipe` code in
`src/snailmail/bandwidth.py`.

---

## 0. The job snailmail is actually doing

snailmail exists to make **localhost reproduce the wall-clock behavior of a
client reading from a cloud object store**, so that Zarr / Icechunk read paths
(many small metadata GETs + some larger chunk range-GETs) can be benchmarked
deterministically. Two transports share one bandwidth model:

```
                  +-------------------------------+
  client  ─────▶  |  latency injection (parallel) |  ─────▶  payload
  (zarr/         |        +  BANDWIDTH MODEL       |
   icechunk)      |        (shared today)          |
                  +-------------------------------+
                     range/file server (aiohttp)
                     S3 object store (moto + WSGI)
```

Two design constraints frame everything below:

1. **Cross-server comparability.** The aiohttp server and the moto/WSGI S3
   server must produce the *same* throughput behavior for the same config.
   A model is only acceptable if both transports can drive it identically.
2. **Buy-not-build / simplicity.** This is a test fixture, not a network
   simulator. Every knob is a maintenance and a "is my benchmark honest?"
   liability.

The key question this report answers: the current model **serializes
concurrent transfers**. For a workload whose whole point is "fan out many
parallel GETs," does that serialization mislead the benchmark? And if so, is the
cure worth the complexity?

---

## 1. The model space

Notation used throughout:

- `B` = aggregate downlink capacity (bytes/s).
- `n` = number of concurrent in-flight transfers.
- We diagram **3 transfers** starting near `t=0`, each `S` bytes, with
  `S/B = 3` time units. `T1`,`T2`,`T3` issued at roughly the same instant.
- Diagrams are throughput-vs-time and completion timelines. `█` = transfer
  actively moving bytes; `·` = issued but waiting/starved.

---

### (a) Single shared serial pipe / FIFO reservation  — *the current model*

**Definition.** One global cursor `_free` = the timestamp the wire is next idle.
Each transfer reserves a contiguous block on the wire:
`start = max(now, _free); _free = start + nbytes/B; sleep(_free - now)`.
Transfers serialize in arrival order; aggregate egress never exceeds `B`; total
wall-clock for any set of transfers = `Σbytes / B` regardless of concurrency.

**Timeline (3 transfers, each takes 3 units of pipe time):**

```
B  |█████████████████████████████|   one transfer's worth of B at a time
   |   T1     T2      T3          |
t: 0   1   2 3   4  5 6   7  8  9
       └─T1─┘   completes at t=3
               └─T2─┘ completes at t=6
                      └─T3─┘ completes at t=9
aggregate throughput over time = flat B the whole time (pipe always full)
```

**Models:** a single client whose *bottleneck is its own access link* (home/
office uplink, a throttled NIC, a hard egress quota). The wire is a strictly
serial resource; parallelism buys nothing.

**Fidelity:** Correct for "I have a fixed pipe and I'm saturating it." The
aggregate cap is exactly right and the math is exact and deterministic.
**Wrong** for cloud object stores, where added connections usually raise
aggregate throughput, and wrong on *fairness*: real concurrent flows finish
*together-ish*, this finishes them *staggered* (T1 at 3, T3 at 9) even though a
real client issuing 3 parallel GETs over one fat pipe would see all three
progress at B/3 and finish near t=9 simultaneously.

**Complexity:** trivial. One lock, one float. Already implemented and identical
across both servers. This is the baseline to beat.

---

### (b) Fair-share / processor-sharing (PS)

**Definition.** The `B` capacity is split evenly across all currently active
flows: each gets `B/n` *instantaneously*, and the split **rebalances** every
time a flow starts or finishes. This is the classic **egalitarian processor
sharing** model (Kleinrock); it is the idealized limit of what TCP congestion
control converges to on a shared bottleneck.

**Timeline (3 transfers issued at t=0, each S bytes, S/B = 3):**

```
phase 1: 3 flows, each at B/3        phase 2: ... eventually 1 flow at B
t=0 ───────────────────────────────────────────────▶ all finish ~t=9
   T1 █(B/3)██(B/3)██(B/3)█  ... finishes
   T2 █(B/3)██(B/3)██(B/3)█  ... finishes   (if equal size, all ~together)
   T3 █(B/3)██(B/3)██(B/3)█  ... finishes
aggregate = B (fully utilized), but each flow is slow & they finish ~together
```

If sizes differ, short flows finish first, freeing capacity that rebalances to
the survivors (the realistic, desirable behavior):

```
sizes S, 2S, 3S, capacity B:
  T1(S)   █B/3█  done@~1.5  ─┐ frees 1/3
  T2(2S)  █B/3█──█B/2█ done  │ then survivors speed up
  T3(3S)  █B/3█──█B/2█──█B█ done last
```

**Models:** the realistic single-bottleneck-link case where TCP flows share
fairly. Same aggregate cap `B` as the FIFO model — but *honest about
concurrency latency*: each individual GET is slowed by contention, and they
complete together instead of artificially staggered.

**Fidelity:** High for "one bottleneck link, many TCP flows." This is what you'd
actually measure on a saturated home/office link. Fairness can be quantified
with **Jain's fairness index**; PS gives index = 1.0.

**Complexity:** Moderate, and notably harder in an event/sleep model. You can't
just `sleep(nbytes/B)` up front because the rate of a flow *changes* whenever
another flow enters or leaves. You need either:
- a discrete-event scheduler that recomputes finish times on every
  arrival/departure (exact PS), or
- a periodic "tick" that advances each flow's remaining bytes by
  `(B/n)·Δt` (approximate, simpler, introduces tick granularity error).
Both require shared mutable state (set of active flows + remaining bytes) and a
wake-up mechanism. This is the first model that is meaningfully more code than
the current one, and the first whose **sync/async parity** takes real care
(asyncio tasks vs WSGI threads must observe the same active-flow set).

---

### (c) Per-connection cap (parallelism helps)

**Definition.** Each connection is limited to a per-flow rate `b`. Aggregate is
`n·b`, capped by a ceiling `B_max`. So `effective = min(n·b, B_max)`. With one
connection you get `b`; with many you scale up until you hit the ceiling.

**Timeline (3 transfers, per-flow `b`, ceiling not yet reached):**

```
each flow runs independently at b, no mutual slowdown:
   T1 █b█b█b█ done@3
   T2 █b█b█b█ done@3
   T3 █b█b█b█ done@3        all finish together @ t=3 (3x faster aggregate!)
aggregate throughput = 3b  (rises with n, until it pins at B_max)
```

With a ceiling `B_max = 2b`:

```
   T1 █(2b/3)...        ┐ three flows share the 2b ceiling once n·b > B_max
   T2 █(2b/3)...        │ -> degrades toward PS above the knee
   T3 █(2b/3)...        ┘
aggregate = min(3b, 2b) = 2b
```

**Models:** **cloud object stores.** S3/GCS throttle *per connection* (a single
TCP stream to S3 tops out at some tens to low-hundreds of MB/s), and the
documented way to go faster is **more parallel connections / byte-range
parallelism** — until you hit account/prefix or client-NIC limits (`B_max`).
This is the single most representative behavior for snailmail's stated target.

**Fidelity:** High for the object-store regime *below* the ceiling, where it
captures the thing the FIFO model gets most wrong: **parallelism is supposed to
help.** Above the ceiling it converges to PS (flows share `B_max`). It does
*not* model TCP ramp on each stream (see (e)).

**Complexity:** Low-to-moderate. Below the ceiling each flow is *independent* —
you can literally `sleep(nbytes/b)` per transfer with **no shared state**,
which is even simpler than today's lock. The only shared state needed is for the
ceiling: enforce `B_max` with one shared FIFO-style cursor at rate `B_max`
*composed* on top of the per-flow delay (i.e. delay = max(per-flow time,
ceiling reservation)). Without a ceiling it's embarrassingly parallel and
trivially identical across both servers.

---

### (d) Token bucket / leaky bucket

**Definition.** A bucket holds up to `C` tokens (burst capacity) and refills at
sustained rate `r` tokens/s. A transfer of `nbytes` consumes `nbytes` tokens;
if the bucket is short, it waits for refill. Allows **bursts up to `C`** then
clamps to sustained `r`. (Leaky bucket is the dual: a fixed-rate drain with a
queue.) This is the canonical rate-limiter shape (it's what most API gateways,
QoS shapers, and `tc tbf` implement).

**Timeline (burst then throttle):**

```
tokens
  C |▔▔▔\          /▔\          (bucket full -> drains during burst,
    |    \        /   \          refills at r during idle)
  0 |     \______/     \_____
       burst: fast      throttled to r once empty

throughput
  B |████             █            spiky: fast while tokens last,
  r |    ▔▔▔▔▔▔▔▔  ▔▔▔▔ ▔▔▔▔▔      then pinned to r
```

**Models:** providers/links that permit short bursts above sustained rate (cloud
"burst balance" disks/instances, throttled APIs, shaped corporate links). Good
when you specifically want to study **burst credit exhaustion**.

**Fidelity:** High for *shaped/credited* links; low relevance to the steady
saturated object-store read pattern snailmail targets, where you're rarely
studying burst-credit dynamics. It's an *orthogonal* shaping layer — it answers
"what's my allowed rate over time," not "how is capacity shared among flows," so
it could in principle wrap any of (a)/(b)/(c).

**Complexity:** Low in isolation (one bucket = two floats: tokens + last-refill
time, refill lazily on each access). But it adds 2 user-facing knobs (`C`, `r`)
whose *interpretation in a benchmark* is subtle, and combining it with a sharing
model multiplies the configuration surface.

---

### (e) Latency–bandwidth interaction: BDP & TCP slow-start

**Definition.** Real throughput of a single TCP flow is not `B` from `t=0`.
- **Bandwidth-delay product (BDP)** `= B · RTT` is the in-flight bytes needed to
  fill the pipe. A flow can't exceed `window / RTT`; until the congestion window
  grows to ≥ BDP, you're RTT-bound, not bandwidth-bound.
- **TCP slow-start** grows the window exponentially (~doubles per RTT) from a
  small initial window (~10 segments, ~14 KB). So small/medium transfers
  **finish before ever reaching `B`**, and effective throughput is a function of
  transfer size and RTT.

**Throughput vs. time for ONE flow (the ramp the other models ignore):**

```
B  |              ____________________   <- only large transfers get here
   |          ___/
   |       __/        each step ~ one RTT, window roughly doubles
   |    __/
   |  _/
 0 |_/__________________________________  t
   0  RTT 2RTT 3RTT 4RTT ...
small object  ──┘ (done up here, never sees B)
large object ───────────────┘ (eventually saturates)
```

Consequence for snailmail's exact workload: a 4 KB Zarr metadata GET is
**entirely dominated by latency + slow-start**; `B` is nearly irrelevant to it.
A 16 MB chunk read *does* approach `B` (or the per-connection `b`). The current
model treats latency and bandwidth as **independent and additive** (`total =
latency + bytes/B`), which is a *reasonable first-order approximation* but
systematically **over-credits throughput on small objects** — exactly the
objects snailmail has the most of.

**Models:** real TCP behavior; the reason "just multiply by `B`" lies for small
reads.

**Fidelity:** Highest realism, but...

**Worth it? Probably not, and here's the honest argument.** Modeling slow-start
faithfully means per-connection window state, RTT accounting, segment math, and
making the moto/WSGI side and aiohttp side agree byte-for-byte on a stateful TCP
emulation. That is a *network simulator*, which violates buy-not-build hard. The
**80/20 alternative** is to keep latency and bandwidth additive (as today): the
per-object latency injection *already* captures the dominant cost for small
objects, and the slow-start error mostly affects the mid-size regime. If
ramp-up ever matters, a cheap approximation is an *effective throughput* that
blends in size, e.g. `T(size) ≈ latency + size/B + extra_RTTs(size)`, rather
than a real window simulation. Recommendation: **acknowledge, don't simulate.**

---

### (f) Others, briefly (mostly out of scope)

- **Weighted fair queueing (WFQ) / DRR.** PS with per-flow weights. Relevant
  only if you want to model QoS priorities between request classes — snailmail
  has no such notion. Skip.
- **AQM / RED / CoDel, ECN.** Queue-management and congestion-signaling
  policies. These shape *loss/delay under congestion*; a benchmark fixture that
  never actually congests a real queue gains nothing. Skip.
- **Little's law** (`L = λ·W`) is worth keeping in mind as a *sanity check*, not
  a model: average in-flight requests = arrival rate × mean response time. Handy
  for validating that whatever model you pick produces self-consistent
  concurrency numbers in a benchmark report.

---

## 2. Comparison table

| Model | Captures | Gets wrong | Complexity | Use when |
|---|---|---|---|---|
| (a) FIFO shared pipe *(current)* | Hard aggregate cap `B`; over-read costs real time; exact & deterministic | Penalizes concurrency (serializes); staggers completions; no fair-share; no parallel speedup | Trivial (1 lock, 1 float) — done | Client's own fixed access link is the bottleneck; you want a worst-case serial floor |
| (b) Fair-share / PS | Realistic single-bottleneck sharing; honest per-flow slowdown; flows finish together; Jain index = 1 | No parallel *speedup* (still capped at `B`); ignores TCP ramp | Moderate (active-flow set + rebalancing or ticks); sync/async parity is fiddly | Modeling one saturated link shared by many TCP flows |
| (c) Per-connection cap | **Parallelism helps** (`min(n·b, B_max)`); matches object-store throughput scaling | Ignores TCP ramp; below-ceiling assumes perfect independence | Low (independent per-flow sleep) + small shared cursor for ceiling | **Cloud object store** reads — snailmail's target |
| (d) Token bucket | Burst-then-sustain shaping; credit exhaustion | Orthogonal to sharing; irrelevant to steady reads; +2 subtle knobs | Low alone, multiplies config when combined | Studying burst credits / shaped APIs |
| (e) BDP + slow-start | Why small reads never hit `B`; size/RTT-dependent throughput | — (it's the realistic one) | High; stateful TCP emulation; hard cross-server parity | Almost never in a fixture — approximate instead |

---

## 3. The benchmark use case, analyzed

snailmail's canonical workload: **one machine reading from a cloud object store**
— a flood of *small metadata objects* (Zarr `.zarray`/`.zattrs`/Icechunk
manifest/refs, a few KB each) plus *some larger chunk reads* (range-GETs,
hundreds of KB to tens of MB), typically issued with **high client-side
concurrency** (async Zarr, fsspec/obstore thread or connection pools).

What actually governs wall-clock here:

```
  small metadata GETs:   cost ≈ latency  (+ slow-start)     <- latency-bound
  large chunk GETs:      cost ≈ size / per-connection-rate  <- bandwidth-bound
  overall throughput:    RISES with parallelism until a ceiling
```

Where the **current FIFO model misleads**:

1. **It penalizes the concurrency the workload depends on.** A client that fans
   out 50 parallel chunk GETs against S3 normally sees *aggregate throughput
   climb* with parallelism. FIFO serializes them through one `B`-rate wire, so
   the benchmark reports `Σbytes / B` no matter how parallel the client is.
   Parallelism that should *help* shows *zero benefit* — the opposite of the
   real system's defining behavior.
2. **It staggers completions that should be roughly simultaneous.** Three
   equal GETs issued together finish at `t = 3, 6, 9` under FIFO; under any
   realistic model they finish near `t = 9` *together* (PS) or near `t = 3`
   *together* (per-connection, sub-ceiling). The shape of the completion-time
   distribution — which is exactly what a concurrency benchmark is measuring —
   is wrong.
3. **It can't show diminishing returns / the ceiling.** Real tuning questions
   ("does going from 16 to 64 connections still help?") are invisible: FIFO has
   no knee, per-connection-cap has exactly that knee at `B_max`.

What it gets *right*, and why it's still worth keeping: if the bottleneck really
is the **client's own access link** (laptop on hotel wifi, capped egress), FIFO
is the *correct* and conservative model, and its determinism (`total = Σbytes/B`,
exactly) makes it an excellent reproducible **lower bound / regression guard**.

**Best fit for the target workload:** **(c) per-connection cap with a ceiling.**
It is the only model whose defining behavior is "parallelism helps, up to a
limit," which is the defining behavior of cloud object-store reads. **(b) PS** is
the right model for the *single-link* scenario and is a strictly better default
than FIFO when you *don't* want parallel speedup but *do* want honest fairness.
**(a) FIFO** stays as the deterministic floor. (d) and (e) are not needed for
this workload.

---

## 4. Recommendation

### Verdict: go pluggable, but keep it to a tiny interface and at most three implementations.

One model is genuinely insufficient here — not on aesthetic grounds, but because
the *current* model contradicts the workload snailmail was built to measure
(parallelism is the point, and FIFO erases it). At the same time, a full
network simulator is the wrong tool. The sweet spot is a **one-method strategy
interface** with two or three small implementations, defaulting to the one that
matches the advertised use case.

### Proposed interface

Keep it as small as the thing it replaces. A bandwidth model needs to answer one
question: *given a transfer of `nbytes` belonging to some flow, how long should
I block before the bytes are "delivered"?* Provide both an async and a sync
entry point (the two transports already need both), e.g. a `Protocol`:

```python
class BandwidthModel(Protocol):
    """Returns/awaits the delay to charge a transfer of `nbytes`.
    Implementations own any shared state and locking. `B is None` => no limit."""
    async def transfer(self, nbytes: int) -> None: ...   # aiohttp path
    def transfer_sync(self, nbytes: int) -> None: ...    # WSGI/moto path
    def reset(self) -> None: ...
```

- **`SharedPipe` / `AsyncSharedPipe` become the first implementation** of this
  protocol *unchanged* — they already expose `transfer`/`reset`; just relabel
  them `FifoPipe` (model (a)) and register them. This is a pure refactor, zero
  behavior change, and preserves the existing default if you want a no-surprises
  release.
- Selection is a single config/CLI enum (`--bandwidth-model fifo|per-conn|fair`)
  threaded to *both* servers, satisfying the comparability requirement by
  construction: the same model object (or same class + params) is what each
  transport calls.

### Which models to actually build

| Model | Build? | Rationale |
|---|---|---|
| (a) FIFO | **Yes — already built.** | Deterministic floor / regression guard; models a capped client link. Make it one strategy, possibly the conservative default. |
| (c) Per-connection cap | **Yes — build this.** | The model that matches the advertised object-store workload. Cheap: below the ceiling it's an independent per-flow `sleep(nbytes/b)` (less shared state than today); the ceiling is one extra FIFO-style cursor at `B_max`. Two knobs (`b`, `B_max`), both meaningful to users. Trivial sync/async parity. **Highest value-to-cost.** |
| (b) Fair-share / PS | **Maybe — only if the single-link scenario is in scope.** | Strictly more honest than FIFO for shared-link sharing, but the rebalancing scheduler is the most code and the trickiest cross-server parity. Defer unless a user actually wants "one bottleneck, fair flows." A *cheap approximation* (charge each concurrent transfer `nbytes/(B/n_active)` sampled at start, no mid-flight rebalancing) gets ~80% of the realism for ~20% of the code, if you want it. |
| (d) Token bucket | **No (for now).** | Orthogonal shaping; not what this workload studies; doubles config surface. Easy to add later as a wrapper if burst studies ever come up. |
| (e) BDP / slow-start | **No — document, don't simulate.** | Real value but real simulator; violates buy-not-build. Keep latency+bandwidth additive; note in docs that small-object throughput is over-credited and that per-object latency already dominates them. Optionally add a one-line "effective throughput" fudge only if a benchmark visibly diverges from reality. |

### Concrete plan

1. Extract the `BandwidthModel` protocol; make existing pipes `FifoPipe` (no
   behavior change). Ship that as a refactor.
2. Add `PerConnectionModel(b, B_max)` (model (c)) and make it the **documented
   default for the object-store use case**, since it's the one that doesn't
   contradict the workload. Keep `fifo` as the conservative floor.
3. Wire a single `--bandwidth-model` enum through both servers; assert in a test
   that both transports, given the same model+config, produce the same
   wall-clock for an identical transfer schedule (locks in comparability).
4. Stop there. Treat (b) as a stretch (or its cheap approximation), and (d)/(e)
   as explicitly out of scope with a short docs note explaining *why* (so the
   omission reads as a decision, not an oversight).

**Bottom line:** keep the current model — but not as the *only* model. Promote it
to one strategy behind a one-method interface, add per-connection-cap as the
realistic object-store default, and resist everything else. That fixes the one
way snailmail currently lies about its own use case (concurrency should help)
without turning a test fixture into ns-3.
