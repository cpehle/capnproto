import LeanTest
import Capnp.Rpc
import Capnp.Gen.c__.src.capnp.test

open LeanTest
open Capnp.Gen.c__.src.capnp.test

def mkCapabilityPayload (cap : Capnp.Capability) : Capnp.Rpc.Payload := Id.run do
  let (capTable, builder) :=
    (do
      let root ← Capnp.getRootPointer
      Capnp.writeCapabilityWithTable Capnp.emptyCapTable root cap
    ).run (Capnp.initMessageBuilder 16)
  { msg := Capnp.buildMessage builder, capTable := capTable }

def mkNullPayload : Capnp.Rpc.Payload := Id.run do
  let (_, builder) :=
    (do
      let root ← Capnp.getRootPointer
      Capnp.clearPointer root
    ).run (Capnp.initMessageBuilder 16)
  { msg := Capnp.buildMessage builder, capTable := Capnp.emptyCapTable }

@[test]
def testGeneratedMethodMetadata : IO Unit := do
  assertEqual TestInterface.fooMethodId (UInt16.ofNat 0)
  assertEqual TestInterface.barMethodId (UInt16.ofNat 1)
  assertEqual TestInterface.fooMethod.interfaceId TestInterface.interfaceId
  assertEqual TestInterface.fooMethod.methodId TestInterface.fooMethodId

@[test]
def testDispatchRoutesGeneratedClientCall : IO Unit := do
  let hit ← IO.mkRef false
  let seenTarget ← IO.mkRef (UInt32.ofNat 0)
  let payload : Capnp.Rpc.Payload := Capnp.emptyRpcEnvelope
  let dispatch :=
    Capnp.Rpc.Dispatch.register Capnp.Rpc.Dispatch.empty
      TestInterface.fooMethod
      (fun target req => do
        hit.set true
        seenTarget.set target
        pure req)
  let backend := dispatch.toBackend
  let response ← TestInterface.callFoo backend (UInt32.ofNat 123) payload
  assertEqual (← hit.get) true
  assertEqual (← seenTarget.get) (UInt32.ofNat 123)
  assertEqual (response == payload) true

@[test]
def testDispatchOnMissingReceivesMethod : IO Unit := do
  let seenMethod ← IO.mkRef ({ interfaceId := 0, methodId := 0 } : Capnp.Rpc.Method)
  let payload : Capnp.Rpc.Payload := Capnp.emptyRpcEnvelope
  let backend := Capnp.Rpc.Dispatch.toBackend Capnp.Rpc.Dispatch.empty (onMissing := fun _ method req => do
    seenMethod.set method
    pure req)
  let response ← TestInterface.callBar backend (UInt32.ofNat 5) payload
  let method := (← seenMethod.get)
  assertEqual method.interfaceId TestInterface.interfaceId
  assertEqual method.methodId TestInterface.barMethodId
  assertEqual (response == payload) true

@[test]
def testBackendOfRawCall : IO Unit := do
  let seenMethod ← IO.mkRef ({ interfaceId := 0, methodId := 0 } : Capnp.Rpc.Method)
  let payload : Capnp.Rpc.Payload := Capnp.emptyRpcEnvelope
  let raw : Capnp.Rpc.RawCall := fun _ method requestBytes => do
    seenMethod.set method
    pure requestBytes
  let backend := Capnp.Rpc.Backend.ofRawCall raw
  let response ← TestInterface.callFoo backend (UInt32.ofNat 17) payload
  let method := (← seenMethod.get)
  assertEqual method.interfaceId TestInterface.interfaceId
  assertEqual method.methodId TestInterface.fooMethodId
  assertEqual (response == payload) true

@[test]
def testFfiBackendRawRoundtrip : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let response ← Capnp.Rpc.RuntimeM.runWithNewRuntime do
    assertEqual (← Capnp.Rpc.RuntimeM.isAlive) true
    let target ← Capnp.Rpc.RuntimeM.registerEchoTarget
    let echoed ← TestInterface.callFooM target payload
    assertEqual echoed.capTable.caps.size 0

    let capPayload := mkCapabilityPayload target
    let capResponse ← TestInterface.callFooM target capPayload
    assertEqual capResponse.capTable.caps.size 1

    let returnedCap? := Capnp.readCapabilityFromTable capResponse.capTable (Capnp.getRoot capResponse.msg)
    assertEqual returnedCap?.isSome true
    match returnedCap? with
    | some returnedTarget =>
        assertEqual (returnedTarget == (UInt32.ofNat 0)) false
        TestInterface.callFooM returnedTarget payload
    | none =>
        throw (IO.userError "RPC response is missing expected capability")
  assertEqual response.capTable.caps.size 0

  let runtime ← Capnp.Rpc.Runtime.init
  let target ← runtime.registerEchoTarget
  runtime.shutdown
  assertEqual (← runtime.isAlive) false

  let failedAfterShutdown ←
    try
      let _ ← Capnp.Rpc.RuntimeM.run runtime do
        TestInterface.callFooM target payload
      pure false
    catch _ =>
      pure true
  assertEqual failedAfterShutdown true
