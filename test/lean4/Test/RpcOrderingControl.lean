import LeanTest
import Capnp.Rpc
import Capnp.Gen.test.lean4.fixtures.rpc_echo

open LeanTest
open Capnp.Gen.test.lean4.fixtures.rpc_echo

private def mkCapabilityPayload (cap : Capnp.Capability) : Capnp.Rpc.Payload := Id.run do
  let (capTable, builder) :=
    (do
      let root ← Capnp.getRootPointer
      Capnp.writeCapabilityWithTable Capnp.emptyCapTable root cap
    ).run (Capnp.initMessageBuilder 16)
  { msg := Capnp.buildMessage builder, capTable := capTable }

private def mkNullPayload : Capnp.Rpc.Payload := Id.run do
  let (_, builder) :=
    (do
      let root ← Capnp.getRootPointer
      Capnp.clearPointer root
    ).run (Capnp.initMessageBuilder 16)
  { msg := Capnp.buildMessage builder, capTable := Capnp.emptyCapTable }

private def mkUInt64Payload (n : UInt64) : Capnp.Rpc.Payload := Id.run do
  let (_, builder) :=
    (do
      let root ← Capnp.getRootPointer
      let s ← Capnp.initStructPointer root 1 0
      Capnp.setUInt64 s 0 n
    ).run (Capnp.initMessageBuilder 16)
  { msg := Capnp.buildMessage builder, capTable := Capnp.emptyCapTable }

private def readUInt64Payload (payload : Capnp.Rpc.Payload) : IO UInt64 := do
  let root := Capnp.getRoot payload.msg
  match Capnp.readStructChecked root with
  | .ok s =>
      pure (Capnp.getUInt64 s 0)
  | .error err =>
      throw (IO.userError s!"invalid uint64 RPC payload: {err}")

private def registerEchoFooCallOrderTarget (runtime : Capnp.Rpc.Runtime) :
    IO (Capnp.Rpc.Client × IO UInt64) := do
  let nextExpected ← IO.mkRef (UInt64.ofNat 0)
  let target ← runtime.registerHandlerTarget (fun _ method req => do
    if method.interfaceId != Echo.interfaceId || method.methodId != Echo.fooMethodId then
      throw (IO.userError
        s!"unexpected method in call-order target: {method.interfaceId}/{method.methodId}")
    let expected ← readUInt64Payload req
    let current ← nextExpected.get
    if expected != current then
      throw (IO.userError s!"call-order mismatch: expected {current}, got {expected}")
    nextExpected.set (current + (UInt64.ofNat 1))
    pure (mkUInt64Payload current))
  pure (target, nextExpected.get)

private def pumpUntil (runtime : Capnp.Rpc.Runtime) (label : String) (attempts : Nat)
    (check : IO Bool) : IO Unit := do
  let mut remaining := attempts
  while remaining > 0 do
    runtime.pump
    if (← check) then
      return ()
    remaining := remaining - 1
  throw (IO.userError s!"{label}: timeout")

@[test]
def testRuntimeOrderingResolveHoldControlsDisembargo : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let (callOrder, getNextExpected) ← registerEchoFooCallOrderTarget runtime
    let (promiseCap, fulfiller) ← runtime.newPromiseCapability
    let promisedPayload := mkCapabilityPayload promiseCap
    let responder ← runtime.registerAdvancedHandlerTargetAsync (fun _ method _ => do
      if method.interfaceId != Echo.interfaceId || method.methodId != Echo.fooMethodId then
        throw (IO.userError
          s!"unexpected method in ordering-control target: {method.interfaceId}/{method.methodId}")
      let deferred ← Capnp.Rpc.Advanced.defer do
        pure (Capnp.Rpc.Advanced.respond promisedPayload)
      pure (Capnp.Rpc.Advanced.setPipeline promisedPayload deferred))

    runtime.orderingSetResolveHold true
    assertEqual (← runtime.orderingHeldResolveCount) (UInt64.ofNat 0)

    let pending ← runtime.startCall responder Echo.fooMethod payload
    let pipelineCap ← pending.getPipelinedCap

    let call0Pending ← runtime.startCall pipelineCap Echo.fooMethod (mkUInt64Payload (UInt64.ofNat 0))
    let call1Pending ← runtime.startCall pipelineCap Echo.fooMethod (mkUInt64Payload (UInt64.ofNat 1))

    fulfiller.fulfill callOrder
    assertEqual (← runtime.orderingHeldResolveCount) (UInt64.ofNat 1)

    assertEqual (← runtime.orderingFlushResolves) (UInt64.ofNat 1)
    assertEqual (← runtime.orderingHeldResolveCount) (UInt64.ofNat 0)
    pumpUntil runtime "pipelined capability resolve/disembargo" 128 do
      runtime.targetWhenResolvedPoll pipelineCap
    assertEqual (← runtime.targetWhenResolvedPoll pipelineCap) true

    let call0Response ← call0Pending.await
    let call1Response ← call1Pending.await
    assertEqual (← readUInt64Payload call0Response) (UInt64.ofNat 0)
    assertEqual (← readUInt64Payload call1Response) (UInt64.ofNat 1)
    assertEqual (← getNextExpected) (UInt64.ofNat 2)

    let response ← pending.await
    runtime.releaseCapTable response.capTable
    runtime.releaseTarget pipelineCap
    runtime.releaseTarget responder
    fulfiller.release
    runtime.releaseTarget promiseCap
    runtime.releaseTarget callOrder
    runtime.orderingSetResolveHold false
  finally
    runtime.shutdown

@[test]
def testRuntimeOrderingResolveHooksTrackHeldCount : IO Unit := do
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let echo ← runtime.registerEchoTarget
    let (promiseCap, fulfiller) ← runtime.newPromiseCapability
    runtime.orderingSetResolveHold true
    assertEqual (← runtime.orderingHeldResolveCount) (UInt64.ofNat 0)
    assertEqual (← runtime.targetWhenResolvedPoll promiseCap) false

    fulfiller.fulfill echo
    assertEqual (← runtime.orderingHeldResolveCount) (UInt64.ofNat 1)
    assertEqual (← runtime.targetWhenResolvedPoll promiseCap) false

    assertEqual (← runtime.orderingFlushResolves) (UInt64.ofNat 1)
    assertEqual (← runtime.orderingHeldResolveCount) (UInt64.ofNat 0)
    pumpUntil runtime "promise capability resolve poll" 64 do
      runtime.targetWhenResolvedPoll promiseCap
    assertEqual (← runtime.targetWhenResolvedPoll promiseCap) true

    fulfiller.release
    runtime.releaseTarget promiseCap
    runtime.releaseTarget echo
    runtime.orderingSetResolveHold false
  finally
    runtime.shutdown
