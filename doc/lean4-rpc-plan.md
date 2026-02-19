# Lean4 RPC Backend Plan (C++ Parity)

This plan tracks closure to C++ RPC behavior parity and keeps implementation priorities explicit.

## Goal

Deliver a production-ready Lean RPC stack with:
- C++-shape runtime semantics (two-party first, explicit lifecycle)
- advanced handler control surface for Lean-implemented capabilities
- first-class async composition (`KjAsync` + `Capnp.Async`) without semantic drift
- clear parity matrix against C++ behavior classes

## Status Snapshot (2026-02-19)

Implemented baseline:
- explicit runtime lifecycle (`Runtime.init`, `Runtime.shutdown`, `RuntimeM.runWithNewRuntime`)
- two-party client/server handles (`newClient`, `newServer`, bootstrap/listen/accept/drain)
- disconnect and lifecycle APIs (`onDisconnect`, release APIs, retain/release semantics)
- pending-call pipelining (`startCall`, `pendingCallGetPipelinedCap`, await/release)
- advanced handler controls:
  - deferred async handler replies
  - `sendResultsTo` caller forwarding
  - call hints (`noPromisePipelining`, `onlyPromisePipeline`)
  - `setPipeline` controls and validation
  - typed remote exceptions with detail payloads
- parity-oriented ordering tests for resolve/disembargo/tail-call classes
- Lean<->C++ interop tests (both client and server directions)
- trace encoder support:
  - enable/disable toggles
  - custom callback (`setTraceEncoder`)
- KJ async networking/runtime surface exposed in Lean (`tcp`, `udp/datagram`, `http`, `websocket`)

## Parity Closure Checklist

Legend: `[x]` done, `[~]` partial, `[ ]` open.

- [x] Core two-party runtime lifecycle and handles.
- [x] Core call/return/cancel/disconnect semantics.
- [x] Promise pipelining (`startCall` + pipelined caps).
- [x] Tail-call forwarding controls.
- [x] Resolve/disembargo behavior classes (Lean parity tests present).
- [x] Advanced call-context controls (`sendResultsTo`, call hints, deferred replies, validation).
- [x] Structured remote exception propagation (type + detail payloads).
- [x] Trace encoder toggles and callback surface.
- [x] Multi-vat basics (bootstrap, sturdy refs, forwarding stats, connection checks).
- [~] Full parity matrix coverage against `rpc-test.c++` / `rpc-twoparty-test.c++` (many classes mapped, not yet exhaustively tracked in docs/CI).
- [~] Streaming + FD parity coverage across platforms (implemented, but CI/platform gating still needs formal closure).
- [ ] GC-backed foreign object ownership to remove manual release burden in public APIs.
- [ ] Typed promised-capability codegen surface (`Rpc.Promise`-style generated ergonomics).
- [ ] Zero-copy message/data paths in hot RPC/KJ async flows.
- [ ] C++ bridge modularization (split large bridge translation units into focused modules).

## Open Gaps (Priority Order)

1. Parity matrix hardening.
- Maintain an explicit behavior-class map: C++ test class -> Lean test(s) -> status.
- Wire this map into CI so parity regressions are visible immediately.

2. Lifecycle ergonomics (deep integration track).
- Current public API still requires explicit release patterns in many paths.
- Move toward GC-backed wrappers/foreign objects where practical.
- Keep explicit runtime initialization/shutdown semantics unchanged.

3. Zero-copy/performance track.
- Current RPC/KJ async payload flows are ByteArray-centric.
- Add low-risk zero-copy paths first (borrowing/slice-view style where ownership is clear).
- Add benchmark gates for before/after validation.

4. Bridge maintainability.
- `rpc_bridge_runtime.cpp` and `kj_async_bridge.cpp` remain large and hard to reason about.
- Refactor into modules by concern (runtime lifecycle, advanced handlers, promises, transport).
- Preserve ABI and test behavior during split.

5. Networking scope boundaries and documentation.
- Document exact supported HTTP method surface and transport capabilities.
- Document unsupported/intentional exclusions and expected error modes.

## Networking Scope (Current)

Lean `KjAsync` currently exposes:
- TCP stream connect/listen/read/write helpers.
- UDP datagram bind/send/receive (including promise/task forms).
- HTTP client/server helpers (including streaming request/response bodies).
- WebSocket client/server messaging helpers.

HTTP method enum currently includes:
- `GET`, `HEAD`, `POST`, `PUT`, `DELETE`, `PATCH`, `OPTIONS`, `TRACE`.

Out-of-scope or not yet explicitly planned for parity:
- additional HTTP method variants beyond the enum above
- protocol features not represented in the current KJ bridge method tags

## Milestones and Exit Criteria

### M0: Plan Hygiene (Immediate)

- Update this plan on every parity-affecting merge.
- Add a machine-readable parity checklist artifact in `test/lean4/`.

Exit criteria:
- every parity claim in this document links to concrete Lean test coverage.

### M1: Parity Matrix Closure

- Enumerate all targeted behavior classes from `rpc-test.c++` / `rpc-twoparty-test.c++`.
- Mark each class as implemented/tested/blocked.
- Add missing parity tests for uncovered critical classes.

Exit criteria:
- all P0/P1 behavior classes mapped and passing in Lean CI.

M1 seed matrix status (`2026-02-19`):
- [x] Explicit behavior-class matrix exists in `test/lean4/parity_matrix.json`.
- [x] Core call/return + release/cancel class is `covered`.
- [x] Transport connect/listen/disconnect class is `covered`.
- [ ] Ordering-sensitive class is `partial`.
- [ ] Reliability abort/failure-propagation class is `partial`.
- [ ] Flow control + trace observability class is `partial`.
- [ ] Streaming + FD transfer class is `partial`.

Behavior-class mapping table:

| Behavior class | C++ RPC references | Lean tests | Status | Notes |
| --- | --- | --- | --- | --- |
| Core call/return and release/cancel semantics | `rpc-test.c++`: `basics`, `release capability`, `release capabilities when canceled during return`, `cancellation` | `testRpcReleaseMessageRoundtrip`, `testRpcReturnCanceled`, `testRuntimePendingCallRelease`, `testRuntimeParityAdvancedDeferredReleaseWithoutAllowCancellation` | `covered` | Message-level and runtime-level release/cancel semantics are exercised. |
| Ordering-sensitive pipelining/resolve/disembargo/tail-call | `rpc-test.c++`: `pipelining`, `resolve promise`, `embargo`, `don't embargo null capability`, `tail call`; `rpc-twoparty-test.c++`: `Two-hop embargo` | `testRuntimeParityResolvePipelineOrdering`, `testRuntimeParityDisembargoNullPipelineDoesNotDisconnect`, `testRuntimeParityTailCallPipelineOrdering`, `testRuntimeParityAdvancedDeferredSetPipelineOrdering`, `testRuntimeTwoHopPipelinedResolveOrdering` | `partial` | Behavioral ordering coverage exists, but direct protocol-level `Resolve`/`Disembargo` inspection/control is not exposed. |
| Transport connect/listen/disconnect lifecycle | `rpc-test.c++`: `loopback bootstrap()`, `clean connection shutdown`, `connections set idle when appropriate` | `testRuntimeAsyncClientLifecyclePrimitives`, `testRuntimeClientOnDisconnectAfterServerRelease`, `testRuntimeDisconnectVisibilityViaCallResult`, `testInteropLeanClientObservesCppDisconnectAfterOneShot` | `covered` | Lean runtime and interop tests cover connect/bootstrap/disconnect visibility and cleanup. |
| Reliability abort and failure propagation | `rpc-test.c++`: `abort`, `call promise that later rejects`, `handles exceptions thrown during disconnect`, `disconnection exception retains details`, `method throws exception with detail` | `testRuntimeParityCancelDisconnectSequencing`, `testInteropLeanClientCancelsPendingCallToCppDelayedServer`, `testInteropLeanPendingCallOutcomeCapturesCppException`, `testInteropLeanClientReceivesCppExceptionDetail` | `partial` | Cancellation and exception propagation are covered; explicit abort-path parity scenarios still need expansion. |
| Flow control and trace observability | `rpc-test.c++`: `connections set idle when appropriate`, `method throws exception with trace encoder` | `testRuntimeClientQueueMetrics`, `testRuntimeClientQueueMetricsPreAcceptBacklogDrains`, `testRuntimeClientSetFlowLimit`, `testRuntimeTraceEncoderToggle`, `testRuntimeSetTraceEncoderOnExistingConnection`, `testRuntimeTraceEncoderCallResultVisibility` | `partial` | Queue/flow/trace surfaces are covered, but deeper C++-internal diagnostics parity is still limited. |
| Advanced streaming and FD transfer | `rpc-twoparty-test.c++`: `Streaming over RPC`, `Streaming over RPC no premature cancellation when client dropped`, `send FD over RPC`, `FD per message limit` | `testRuntimeStreamingCall`, `testRuntimeRegisterStreamingHandlerTarget`, `testRuntimeFdPassingOverNetwork`, `testRuntimeFdTargetLocalGetFd` | `partial` | Baseline streaming and FD passing exist; FD-limit and broader platform-gated coverage remain. |

### M2: Lifecycle Ergonomics

- Prototype GC-backed wrapper path for selected handles (`Client` first).
- Reduce required manual release calls in high-level APIs.
- Preserve explicit runtime ownership boundaries.

Exit criteria:
- representative app paths run without manual release calls for common capability usage.

### M3: Zero-Copy + Perf

- Identify top copy hotspots in RPC/KJ async bridging.
- Add low-risk zero-copy variants with clear ownership semantics.
- Benchmark and regress-test these paths.

Exit criteria:
- measurable copy reduction and latency/throughput improvement on benchmark scenarios.

### M4: Bridge Modularization

- Split C++ bridge code by subsystem with unchanged external ABI.
- Keep Lean extern signatures stable during refactor.

Exit criteria:
- bridge code is modular, parity tests still pass, no ABI regressions.

### M5: CI and Docs Lock-In

- Gate parity-critical tests in CI for Linux and macOS.
- Publish user-facing docs for recommended Lean RPC/KjAsync patterns.

Exit criteria:
- deterministic parity CI and documented supported feature matrix.

## Immediate Next Slice

1. Wire `test/lean4/parity_matrix.json` into CI parity checks.
2. Close one `partial` class end-to-end (recommended: ordering-sensitive resolve/disembargo control).
3. Continue M2 with a narrow `Client` lifecycle ergonomics prototype while keeping runtime lifecycle explicit.
