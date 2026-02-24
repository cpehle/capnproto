# Lean4 Plugin Layering Guide

This document describes the current layer boundaries of the Lean4 backend/plugin stack.
It is intended as a contributor-facing map: where each responsibility lives, what each layer
depends on, and where to add new features safely.

## Layer Stack

| Layer | Primary files | Responsibility | Depends on |
| --- | --- | --- | --- |
| 0. Cap'n Proto plugin frontend | `c++/src/capnp/compiler/capnpc-lean4.c++` | Reads `schema::CodeGeneratorRequest`, resolves schema graph, emits Lean source files under `Capnp/Gen/...`. | Cap'n Proto schema loader and compiler internals |
| 1. Generated Lean schema modules | `test/lean4/out/Capnp/Gen/...` (golden under `test/lean4/expected/...`) | Typed schema accessors/builders/method metadata generated from `.capnp` definitions. | `Capnp.Runtime`, `Capnp.Rpc` (for capability schemas) |
| 2. Core wire/runtime library | `lean/Capnp/Runtime.lean` | Pure Lean message/segment model, reader/builder types, packing/unpacking helpers, cap-table envelope primitives. | Lean stdlib |
| 3. Generic async abstraction | `lean/Capnp/Async.lean` | Backend-agnostic async traits (`Awaitable`, `Cancelable`, `Releasable`) and combinator helpers (`Promise`, `withRelease`, etc.). | Lean `Task` / `IO.Promise` |
| 4. KJ async/network binding layer | `lean/Capnp/KjAsync.lean`, `test/lean4/c/kj_async_bridge.cpp` | Lean surface for KJ runtime loop, promises, TCP/UDP, HTTP/WebSocket, stream/payload refs, timeout/cancel/release controls. | KJ async + compat HTTP C++ APIs, Layer 3 |
| 5. RPC binding layer | `lean/Capnp/Rpc.lean`, `test/lean4/c/rpc_bridge_runtime.cpp` | Lean surface for Cap'n Proto RPC runtime, client/server handles, pending calls, pipelining, advanced handler controls, multi-vat primitives. | Cap'n Proto RPC C++ runtime, Layer 2, Layer 3 |
| 6. RPC/KJ runtime bridge | `lean/Capnp/RpcKjAsync.lean` | Shared-runtime helpers so RPC and KJ async operations compose on one runtime handle/event loop. | Layer 4, Layer 5 |
| 7. Validation and parity harness | `test/lean4/Test/*.lean`, `test/lean4/TestDriverRpc.lean`, `test/lean4/parity_matrix.json`, `test/lean4/scripts/validate_parity_matrix.py` | Regression tests, interop tests, parity-critical selection, and matrix validation against C++ behavior classes. | All runtime layers |

## Boundary Rules

1. Layer 0 should not hard-code runtime semantics.
Keep codegen focused on schema shape and generated API signatures; runtime behavior belongs in Layers 2-6.

2. Layer 2 (`Capnp.Runtime`) stays pure Lean.
No KJ/RPC event-loop or network dependencies should leak into this layer.

3. Layer 3 (`Capnp.Async`) is the shared async vocabulary.
If two higher layers need the same lifecycle/await/cancel pattern, add it here instead of duplicating.

4. Layer 4 and Layer 5 own FFI contracts.
`@[extern ...]` signatures in Lean and exported C functions in bridge C++ must evolve together.

5. Layer 6 should only be glue.
Do not duplicate RPC or KJ logic here; keep it as composition helpers on shared runtime handles.

## Ownership and Lifecycle Model

- Runtime lifecycle is explicit:
  - `Runtime.init` / `Runtime.shutdown` remain explicit on both KJ and RPC sides.
- Resource lifecycle is gradually becoming ergonomic:
  - many APIs expose explicit `release`;
  - common scoped helpers (`withRelease`, `awaitAndRelease`, `cancelAndRelease`) are centralized in `Capnp.Async`.
- Cross-runtime safety checks are enforced in Lean wrappers:
  - mixed-handle misuse should fail fast with clear `IO.userError` messages.

## Typical End-to-End Flow

1. `capnp compile -o lean4:...` runs Layer 0 and emits Layer 1 modules.
2. App code imports generated modules and runtime APIs from Layers 2-6.
3. Runtime calls pass through Lean wrappers (Layers 4/5), into C++ bridge implementations.
4. C++ bridge schedules work on KJ/Cap'n Proto runtimes and returns results/promises back to Lean.
5. Tests in Layer 7 validate semantics and C++ parity.

## Where to Add New Features

- New schema-level syntax mapping: Layer 0 (and golden outputs in Layer 1 tests).
- New wire-format utility: Layer 2.
- Shared async helper or lifecycle combinator: Layer 3.
- New network primitive (TCP/UDP/HTTP/WebSocket): Layer 4 + `kj_async_bridge.cpp` + Layer 7 tests.
- New RPC primitive (handlers/pipelining/topology): Layer 5 + `rpc_bridge_runtime.cpp` + Layer 7 tests.
- Mixed RPC + KJ ergonomics: Layer 6.

## Related Planning Docs

- `doc/lean4-rpc-plan.md`: RPC parity and closure plan vs C++ behavior classes.
- `doc/lean4-kjasync-goals.md`: RPC-independent KJ async/runtime expansion goals.
- `doc/lean4-backend.md`: historical backend design sketch and invocation basics.
