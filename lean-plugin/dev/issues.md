# Lean 4 Plugin Code Review Tracker

Last updated: 2026-02-28
Scope: FFI and Runtime Structure Refactoring

Status legend:
- `[ ]` open
- `[~]` in progress
- `[x]` completed

## Working Set (Current)

- [x] `FFI-01` Create a dedicated `native/` or `ffi/` directory at the project root for production C++ bridge code.
- [x] `FFI-02` Move `kj_async_bridge.cpp`, `rpc_bridge_runtime.cpp`, and other core FFI bridges out of `test/lean4/c/` to the new production directory.
- [x] `FFI-03` Update `lakefile.lean` to compile the relocated FFI bridges as part of the core library, separating them from the test harness.
- [x] `FFI-04` Update CMake and Bazel configurations across the codebase to reflect the new paths of the FFI bridge files.
- [x] `FFI-05` Design and implement a shared Lean abstraction (e.g., a common structure or macro) to encapsulate repetitive `runtimeHandle` and `raw` / `handle` fields.
- [x] `FFI-06` Refactor `lean/Capnp/KjAsync.lean` to replace repetitive structure definitions with the new shared FFI handle abstraction.
- [x] `FFI-07` Refactor `lean/Capnp/Rpc.lean` to adopt the shared FFI handle abstraction.
- [x] `FFI-08` Verify that FFI boundary semantics remain intact after refactoring by running the full parity matrix and test suite.

## Open Issues

### High

- [ ] **Misplaced Production Code:** The C++ FFI bridges (`kj_async_bridge.cpp`, `rpc_bridge_runtime.cpp`, etc.) that power the core Lean runtime actually live inside `test/lean4/c/`. Production code and test code are heavily tangled, which makes it confusing to determine what constitutes the core library versus the test harness.
  - *Action:* Execute `FFI-01` through `FFI-04`.

- [ ] **Repetitive FFI Bindings:** The core Lean files like `lean/Capnp/KjAsync.lean` (over 6,400 lines) and `lean/Capnp/Rpc.lean` (over 3,800 lines) have heavily redundant structure definitions repeating identical memory management and handle FFI patterns (e.g., repeating `runtimeHandle` and `raw` / `handle` fields for nearly every struct).
  - *Action:* Execute `FFI-05` through `FFI-08`.

### Medium

### Low

## Completed (This Pass)


### Medium

- [ ] **Compiler Plugin Code Generation Boilerplate:** The `capnpc-lean4.c++` compiler plugin is monolithic and heavily relies on manual string concatenation (over 2000 lines of `out += ...`).
  - *Action:* Introduce a lightweight C++ template engine or AST builder class to cleanly separate Lean 4 syntax generation from Cap'n Proto schema traversal.

- [ ] **FFI Export Boilerplate:** The C++ FFI layers (`ffi/kj_async_bridge.cpp` and `ffi/rpc_bridge_runtime.cpp`) define over 300 `extern "C"` functions, many of which are identical wrappers for object creation, deletion, or task polling.
  - *Action:* Investigate using C++ macros or templates to generate repetitive FFI boundary wrappers (like `release`, `cancel`, `await` functions for different promise types).

### Low

- [ ] **Lean API Repetition (`Capnp.KjAsync`):** There are over 500 explicit definition wrappers (e.g., `connect`, `connectStart`, `connectAsTask`, `connectAsPromise`) manually routing calls to the FFI.
  - *Action:* Explore Lean 4 metaprogramming or macro attributes to auto-generate the task, promise, and IO variants of asynchronous FFI calls.

### Pending Architectural Re-evaluation

- **FFI Boilerplate (Reverted):** Initial plan to use C++ macros to reduce repetitive `extern "C"` FFI wrappers was reverted, as heavy macro usage obfuscates C++ code and makes it harder to debug. A better long-term approach may involve moving towards a unified IDL or Lean-side FFI generation tool instead of manual bindings.

