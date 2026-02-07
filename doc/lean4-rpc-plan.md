# Lean4 RPC Backend Plan (C++ Parity)

This plan tracks the gap between the Lean4 backend and Cap'n Proto's C++ RPC support, and defines
how to close it with minimal semantic drift from C++ behavior.

## Goal

Deliver a Lean-facing RPC backend that behaves like C++ two-party RPC by default, with explicit
runtime initialization and shutdown, and a monadic API layer for ergonomic state handling.

## Status Snapshot (2026-02-07)

Implemented now:
- explicit runtime lifecycle (`Runtime.init`, `Runtime.shutdown`, `RuntimeM.runWithNewRuntime`)
- client/server handles (`newClient`, `newServer`, `bootstrap`, `listen`, `accept`, `drain`,
  `onDisconnect`, release APIs)
- capability lifecycle (`releaseTarget`, `retainTarget`, `releaseCapTable`) with retain/release
  tests covering alias lifetimes
- Lean-implemented capability targets (`registerHandlerTarget`) with network roundtrip tests
- capability-table aware raw call path in runtime FFI
- C++ interop in both directions:
  - C++ client -> Lean server (`Interop.cppCallWithAccept`)
  - Lean client -> C++ server (`Interop.cppServeEchoOnce` + `runtime.connect`)
  - capability-table interop for C++ client -> Lean server is now covered by test
- queue/backpressure observability and flow-limit controls for runtime clients
- pending-call API for promise pipelining (`startCall`, `pendingCallGetPipelinedCap`,
  `pendingCallAwait`, `pendingCallRelease`) with Lean tests
- streaming call path (`streamingCall`), capability resolution wait (`targetWhenResolved`), and
  optional capability FD query (`targetGetFd?`)
- explicit tail-call forwarding API (`registerTailCallTarget`) with Lean tests
- FD-capability registration (`registerFdTarget`) with Lean<->Lean network FD passing test
- runtime trace encoder toggles (`enableTraceEncoder` / `disableTraceEncoder`) with remote-trace
  propagation test coverage

Still missing for full C++ RPC parity:
- direct protocol-level control/inspection for ordering-sensitive behavior (`Resolve`,
  `Disembargo`, tail-call forwarding behavior classes) beyond `whenResolved`-level observation
- richer instrumentation parity with C++ internals (custom trace callbacks and deeper diagnostics)
- broader parity matrix against `rpc-test.c++` / `rpc-twoparty-test.c++` behavior classes

## Current Baseline (Implemented)

- Lean runtime already supports serialization, builders, checked decoding, packed mode, and
  capability pointer helpers.
- Lean codegen emits typed interface metadata and client call wrappers (`callFoo`, `callFooM`).
- Lean RPC runtime now has:
  - explicit `Runtime.init` / `Runtime.shutdown`
  - target lifecycle (`registerEchoTarget`, `releaseTarget`)
  - network connect (`connect`)
  - listener lifecycle (`listenEcho`, `acceptEcho`, `releaseListener`)
  - runtime-managed two-party handles (`newClient`, `newServer`, bootstrap/listen/accept/drain)
  - client queue metrics (`queueSize`, `queueCount`, `outgoingWaitNanos`)
  - capability-table roundtrip in FFI raw calls
- `RuntimeM` exists as the higher-level monadic interface over explicit runtime handles.

## Gap Matrix vs C++ RPC

1. Two-party connection/runtime surface:
- C++: `TwoPartyClient`, `TwoPartyServer`, `RpcSystem`, `onDisconnect`, queue/backpressure metrics.
- Lean today: minimal connect/listen/accept helpers for echo-target testing.
- Needed: first-class client/server handles, bootstrap plumbing, disconnect observability.

2. Full protocol coverage:
- C++: robust `Call`, `Return`, `Finish`, `Resolve`, `Release`, `Disembargo`, tail-call/pipeline.
- Lean today: delegates a narrow request/response path via FFI helper calls.
- Needed: expose all operational semantics through stable Lean APIs and tests.

3. Capability lifetime semantics:
- C++: import/export tables, retain/release correctness, cancellation cleanup guarantees.
- Lean today: explicit target/listener release but no exposed retain/release policy surface.
- Needed: clear Lean lifecycle APIs matching C++ behavior under cancellation and failure.

4. Streaming and FD support:
- C++: stream calls and optional FD passing in two-party network.
- Lean today: exposed via runtime APIs (`streamingCall`, `registerFdTarget`, `targetGetFd?`).
- Needed: expand behavioral parity tests and platform-gated coverage in CI.

5. Flow control and observability:
- C++: flow limits, queue size/count/wait-time metrics, trace encoder hooks.
- Lean today: flow limits, queue metrics, and basic trace encoder toggles are exposed.
- Needed: custom trace callback surface and richer telemetry parity.

6. Test parity:
- C++ has extensive `rpc-test.c++` and `rpc-twoparty-test.c++` suites.
- Lean today has smoke/integration coverage but not parity scenarios (tail call, embargo races, etc.).
- Needed: mapped Lean test matrix covering representative cases per C++ behavior class.

## Runtime Architecture Alternatives

### A) Thin C ABI over existing C++ two-party stack (Recommended now)

- Reuse `TwoPartyClient`, `TwoPartyServer`, `RpcSystem`, and KJ event loop directly.
- Keep runtime explicit (Lean handle + lifecycle), mirroring C++ ownership patterns.
- Expose Lean-safe opaque handles for client/server/capability objects.

Why this is best now:
- closest semantic match to C++ backend
- fastest path to interop confidence
- avoids reimplementing complex protocol state machines in Lean immediately

### B) Lower-level C ABI around `RpcSystem` internals

- More control over message/state surfaces, but higher API and maintenance complexity.
- Useful only if we need features the convenience classes cannot expose cleanly.

### C) Pure Lean protocol runtime

- Long-term option for reduced FFI surface and stronger Lean-native reasoning.
- Highest engineering risk; should follow after functional parity exists.

### D) Sidecar process

- Not recommended as default backend.
- Consider only for isolation/deployment constraints that require process boundaries.

## Plan to Close the Gap

### Phase 0: Stabilize Build/Test Harness

- Resolve or isolate Lean `LibrarySuggestions` timeout noise for large generated modules.
- Ensure `lake test` can run deterministically in CI for RPC additions.

Exit criteria:
- reliable Lean RPC test runs in CI without manual artifact cleanup/kill steps.

### Phase 1: Canonical Two-Party Runtime Surface

- Introduce Lean-visible handles and APIs mirroring C++ shape:
  - `Runtime.newClient`, `Runtime.newServer`
  - `server.listen`, `server.accept`, `server.drain`
  - `client.bootstrap`, `client.onDisconnect`
- Keep explicit initialization/shutdown and ownership boundaries.

Exit criteria:
- Lean client can bootstrap a server capability and perform typed calls end-to-end.

### Phase 2: Lifecycle/Failure Semantics

- Add explicit APIs for:
  - disconnect handling
  - cancellation propagation
  - deterministic release behavior for imports/exports
- Align errors and edge-case behavior to C++ semantics as closely as possible.

Exit criteria:
- Lean tests for release-after-cancel, abort/disconnect, and double-release behavior pass.

### Phase 3: Pipeline and Tail-Call Behavior

- Expose protocol features required for:
  - pipelined calls
  - tail-call forwarding
  - resolve/disembargo ordering guarantees
- Add targeted tests mirroring key `rpc-test.c++` scenarios.

Exit criteria:
- Lean reproduces representative C++ cases for pipelining/tail-call/disembargo.

### Phase 4: Flow Control and Runtime Tuning

- Add flow limit and queue metrics wrappers.
- Optionally expose trace encoder plumbing where practical.

Exit criteria:
- Lean runtime can set flow limits and report queue metrics for diagnostics/backpressure.

### Phase 5: Streaming and Optional FD Passing

- Add streaming call support first (platform-agnostic).
- Add FD passing as optional feature behind capability checks/platform guards.

Exit criteria:
- streaming tests pass in Lean; FD tests pass where platform/runtime supports it.

### Phase 6: Interop and Compatibility Lock-In

- Add interop tests against C++ server/client fixtures.
- Freeze Lean API contracts and document compatibility guarantees.

Exit criteria:
- mixed Lean<->C++ roundtrip tests pass for core RPC semantics.

## Test Mapping Strategy

Rather than port every C++ RPC test verbatim, map by behavior class:

- Core call/return: basics, release, cancellation.
- Ordering-sensitive behavior: pipelining, resolve, disembargo.
- Transport behavior: connect/listen/disconnect.
- Reliability behavior: abort paths and failure propagation.
- Advanced features: streaming, FD transfer (optional).

Each Lean behavior test should cite the corresponding C++ test name in a comment/doc note.

## Immediate Next Implementation Slice

1. Add ordering-sensitive parity tests mapped to C++ categories (`Resolve`/`Disembargo` classes).
2. Expand interop tests from smoke checks to parity scenarios (cap transfer, failure/disconnect,
   cancellation behavior).

This keeps implementation aligned with C++ conventions while focusing on the remaining parity
blockers rather than adding unrelated surface area.
