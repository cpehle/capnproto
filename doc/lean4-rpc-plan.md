# Lean4 RPC Backend Plan

This document captures the current Lean4 backend status and the phased plan to deliver
functional Cap'n Proto RPC support.

## Current Status

- Lean runtime supports wire encoding/decoding, builders, packed encoding, and capability pointer
  read/write helpers.
- Lean codegen currently emits interface types as capability aliases.
- Generated `rpc.capnp`/`rpc-twoparty.capnp` modules are schema-level data APIs only.
- No full RPC session engine exists yet in Lean:
  - no question/answer lifecycle management
  - no import/export table state machine
  - no transport/session abstraction
  - no promise-pipelining runtime

## Scope Target

Initial target is a functional two-party RPC backend with protocol level 1 behavior.

- In scope:
  - Bootstrap, Call, Return, Finish
  - Resolve, Release, Disembargo
  - Capability table marshalling for calls/results
  - Basic flow/cancellation handling suitable for tests and interop
- Out of scope (initially):
  - full level 2/3/4 features (persistent refs, third-party handoff, join)

## Runtime Architecture Options

### Option A: Pure Lean runtime

Pros:
- single-language implementation
- no FFI dependency

Cons:
- highest engineering effort and risk
- longer time to robust protocol compatibility

### Option B: KJ/C++ reuse via thin C ABI bridge

Pros:
- fastest route to robust behavior by reusing `RpcSystem` and `TwoPartyVatNetwork`
- strongest near-term interop confidence

Cons:
- FFI/lifetime/event-loop integration complexity

### Option C: Out-of-process sidecar

Pros:
- process isolation
- simpler language boundary

Cons:
- extra deployment/process management
- IPC overhead

## Recommended Direction

Use Option B for the first functional backend, while keeping a Lean-side backend abstraction so
pure Lean can be developed later without changing generated API surface.

## Phased Commit Plan

1. Runtime surface and backend abstraction in Lean.
2. Interface codegen upgrade from alias-only to generated client wrappers.
3. In-memory/runtime tests for generated client API and request envelopes.
4. C++/KJ bridge skeleton (non-default backend) behind Lean runtime abstraction.
5. Interop tests and hardening.

## Acceptance Criteria (Phase 1-3)

- Generated interfaces provide typed client wrappers and method IDs.
- Lean tests compile and exercise generated client call preparation paths.
- Existing serialization/checked tests remain green.

## Risks

- capability lifetime semantics around cancellation and finish/release ordering
- flow control details for high-throughput calls
- FFI event-loop ownership when integrating KJ async runtime
