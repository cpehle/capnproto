import LeanTest
import Capnp.Rpc
import Capnp.Gen.test.lean4.fixtures.rpc_echo

open LeanTest
open Capnp.Gen.test.lean4.fixtures.rpc_echo

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

def mkUnixTestAddress : IO (String × String) := do
  let n ← IO.rand 0 1000000000
  let path := s!"/tmp/capnp-lean4-rpc-{n}.sock"
  pure (s!"unix:{path}", path)

def connectRuntimeTargetWithRetry (runtime : Capnp.Rpc.Runtime) (address : String)
    (attempts : Nat := 20) (delayMillis : UInt32 := UInt32.ofNat 10) : IO Capnp.Rpc.Client := do
  let mut target? : Option Capnp.Rpc.Client := none
  let mut attempt := 0
  while target?.isNone && attempt < attempts do
    let nextTarget? ←
      try
        let c ← runtime.connect address
        pure (some (Capnp.Rpc.Client.ofCapability c))
      catch _ =>
        pure none
    target? := nextTarget?
    if target?.isNone then
      IO.sleep delayMillis
    attempt := attempt + 1
  match target? with
  | some target => pure target
  | none => throw (IO.userError s!"failed to connect Lean runtime target to {address}")

@[test]
def testGeneratedMethodMetadata : IO Unit := do
  assertEqual Echo.fooMethodId (UInt16.ofNat 0)
  assertEqual Echo.barMethodId (UInt16.ofNat 1)
  assertEqual Echo.fooMethod.interfaceId Echo.interfaceId
  assertEqual Echo.fooMethod.methodId Echo.fooMethodId

@[test]
def testDispatchRoutesGeneratedClientCall : IO Unit := do
  let hit ← IO.mkRef false
  let seenTarget ← IO.mkRef (UInt32.ofNat 0)
  let payload : Capnp.Rpc.Payload := Capnp.emptyRpcEnvelope
  let dispatch :=
    Capnp.Rpc.Dispatch.register Capnp.Rpc.Dispatch.empty
      Echo.fooMethod
      (fun target req => do
        hit.set true
        seenTarget.set target
        pure req)
  let backend := dispatch.toBackend
  let response ← Echo.callFoo backend (UInt32.ofNat 123) payload
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
  let response ← Echo.callBar backend (UInt32.ofNat 5) payload
  let method := (← seenMethod.get)
  assertEqual method.interfaceId Echo.interfaceId
  assertEqual method.methodId Echo.barMethodId
  assertEqual (response == payload) true

@[test]
def testGeneratedServerBackendDispatch : IO Unit := do
  let payload : Capnp.Rpc.Payload := Capnp.emptyRpcEnvelope
  let seenFoo ← IO.mkRef false
  let seenBar ← IO.mkRef false
  let server : Echo.Server := {
    foo := fun _ req => do
      seenFoo.set true
      pure req
    bar := fun _ req => do
      seenBar.set true
      pure req
  }
  let backend := Echo.backend server
  let response ← Echo.callBar backend (UInt32.ofNat 5) payload
  assertEqual (response == payload) true
  assertEqual (← seenFoo.get) false
  assertEqual (← seenBar.get) true

@[test]
def testGeneratedTypedServerBackendDispatch : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let seenFoo ← IO.mkRef false
  let server : Echo.TypedServer := {
    foo := fun _ req reqCaps => do
      seenFoo.set true
      assertEqual reqCaps.caps.size 0
      assertEqual req.hasPayload false
      pure payload
    bar := fun _ _ _ =>
      throw (IO.userError "unexpected typed bar handler invocation")
  }
  let backend := Echo.typedBackend server
  let (response, responseCaps) ← Echo.callFooTyped backend (UInt32.ofNat 7) payload
  assertEqual responseCaps.caps.size 0
  assertEqual response.hasPayload false
  assertEqual (← seenFoo.get) true

@[test]
def testGeneratedRegisterTargetNetwork : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let seenFoo ← IO.mkRef false
  let seenBar ← IO.mkRef false
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let handler : Echo.Server := {
      foo := fun _ req => do
        seenFoo.set true
        pure req
      bar := fun _ req => do
        seenBar.set true
        pure req
    }
    let bootstrap ← Echo.registerTarget runtime handler
    let server ← runtime.newServer bootstrap
    let listener ← server.listen address
    let client ← runtime.newClient address
    server.accept listener

    let remoteTarget ← client.bootstrap
    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM remoteTarget payload
    assertEqual response.capTable.caps.size 0
    assertEqual (← seenFoo.get) true
    assertEqual (← seenBar.get) false

    client.release
    server.release
    runtime.releaseListener listener
    runtime.releaseTarget bootstrap
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testGeneratedRegisterTypedTargetNetwork : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let seenFoo ← IO.mkRef false
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let handler : Echo.TypedServer := {
      foo := fun _ req reqCaps => do
        seenFoo.set true
        assertEqual reqCaps.caps.size 0
        assertEqual req.hasPayload false
        pure payload
      bar := fun _ _ _ =>
        throw (IO.userError "unexpected typed bar handler invocation")
    }
    let bootstrap ← Echo.registerTypedTarget runtime handler
    let server ← runtime.newServer bootstrap
    let listener ← server.listen address
    let client ← runtime.newClient address
    server.accept listener

    let remoteTarget ← client.bootstrap
    let (response, responseCaps) ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooTypedM remoteTarget payload
    assertEqual responseCaps.caps.size 0
    assertEqual response.hasPayload false
    assertEqual (← seenFoo.get) true

    client.release
    server.release
    runtime.releaseListener listener
    runtime.releaseTarget bootstrap
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testGeneratedAsyncHelpers : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let target ← runtime.registerEchoTarget
    let capPayload := mkCapabilityPayload target
    let pending1 ← Echo.startFoo runtime target capPayload
    let pipelinedCap ← Echo.getFooPipelinedCap pending1
    let pipelinedViaCap ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM pipelinedCap payload
    assertEqual pipelinedViaCap.capTable.caps.size 0
    let response1 ← Echo.awaitFoo pending1
    assertEqual response1.capTable.caps.size 1
    runtime.releaseCapTable response1.capTable
    runtime.releaseTarget pipelinedCap

    let pending2 ← Echo.startFoo runtime target capPayload
    let pipelinedResponse ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooPipelinedM pending2 #[] payload
    assertEqual pipelinedResponse.capTable.caps.size 0
    let response2 ← Echo.awaitFoo pending2
    assertEqual response2.capTable.caps.size 1
    runtime.releaseCapTable response2.capTable

    runtime.releaseTarget target
  finally
    runtime.shutdown

@[test]
def testBackendOfRawCall : IO Unit := do
  let seenMethod ← IO.mkRef ({ interfaceId := 0, methodId := 0 } : Capnp.Rpc.Method)
  let payload : Capnp.Rpc.Payload := Capnp.emptyRpcEnvelope
  let raw : Capnp.Rpc.RawCall := fun _ method requestBytes => do
    seenMethod.set method
    pure requestBytes
  let backend := Capnp.Rpc.Backend.ofRawCall raw
  let response ← Echo.callFoo backend (UInt32.ofNat 17) payload
  let method := (← seenMethod.get)
  assertEqual method.interfaceId Echo.interfaceId
  assertEqual method.methodId Echo.fooMethodId
  assertEqual (response == payload) true

@[test]
def testFfiBackendRawRoundtrip : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let response ← Capnp.Rpc.RuntimeM.runWithNewRuntime do
    assertEqual (← Capnp.Rpc.RuntimeM.isAlive) true
    let target ← Capnp.Rpc.RuntimeM.registerEchoTarget
    let echoed ← Echo.callFooM target payload
    assertEqual echoed.capTable.caps.size 0

    let capPayload := mkCapabilityPayload target
    let capResponse ← Echo.callFooM target capPayload
    assertEqual capResponse.capTable.caps.size 1

    let returnedCap? := Capnp.readCapabilityFromTable capResponse.capTable (Capnp.getRoot capResponse.msg)
    assertEqual returnedCap?.isSome true
    match returnedCap? with
    | some returnedTarget =>
        assertEqual (returnedTarget == (UInt32.ofNat 0)) false
        Echo.callFooM returnedTarget payload
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
        Echo.callFooM target payload
      pure false
    catch _ =>
      pure true
  assertEqual failedAfterShutdown true

@[test]
def testRuntimeReleaseCapTable : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let target ← runtime.registerEchoTarget
    let capPayload := mkCapabilityPayload target
    let capResponse ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM target capPayload
    assertEqual capResponse.capTable.caps.size 1

    let returnedCap? := Capnp.readCapabilityFromTable capResponse.capTable (Capnp.getRoot capResponse.msg)
    assertEqual returnedCap?.isSome true
    match returnedCap? with
    | none =>
        throw (IO.userError "RPC response is missing expected capability")
    | some returnedCap =>
        runtime.releaseCapTable capResponse.capTable
        let failedAfterRelease ←
          try
            let _ ← Capnp.Rpc.RuntimeM.run runtime do
              Echo.callFooM returnedCap payload
            pure false
          catch _ =>
            pure true
        assertEqual failedAfterRelease true

    runtime.releaseTarget target
  finally
    runtime.shutdown

@[test]
def testRuntimeReleaseTarget : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let target ← runtime.registerEchoTarget
    let _ ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM target payload

    runtime.releaseTarget target
    let failedAfterRelease ←
      try
        let _ ← Capnp.Rpc.RuntimeM.run runtime do
          Echo.callFooM target payload
        pure false
      catch _ =>
        pure true
    assertEqual failedAfterRelease true

    let failedDoubleRelease ←
      try
        runtime.releaseTarget target
        pure false
      catch _ =>
        pure true
    assertEqual failedDoubleRelease true
  finally
    runtime.shutdown

@[test]
def testRuntimeRetainTarget : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let target ← runtime.registerEchoTarget
    let retained ← runtime.retainTarget target

    runtime.releaseTarget target
    let _ ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM retained payload

    runtime.releaseTarget retained
    let failedAfterRelease ←
      try
        let _ ← Capnp.Rpc.RuntimeM.run runtime do
          Echo.callFooM retained payload
        pure false
      catch _ =>
        pure true
    assertEqual failedAfterRelease true
  finally
    runtime.shutdown

@[test]
def testRuntimeConnectInvalidAddress : IO Unit := do
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let failedConnect ←
      try
        let _ ← runtime.connect "unix:/tmp/capnp-lean4-rpc-missing.sock"
        pure false
      catch _ =>
        pure true
    assertEqual failedConnect true
  finally
    runtime.shutdown

@[test]
def testRuntimeListenAcceptEcho : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let listener ← runtime.listenEcho address
    let target ← runtime.connect address
    runtime.acceptEcho listener
    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM target payload
    assertEqual response.capTable.caps.size 0

    runtime.releaseListener listener
    let failedAfterRelease ←
      try
        runtime.acceptEcho listener
        pure false
      catch _ =>
        pure true
    assertEqual failedAfterRelease true
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testRuntimeClientServerLifecycle : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let bootstrap ← runtime.registerEchoTarget
    let server ← runtime.newServer bootstrap
    let listener ← server.listen address
    let client ← runtime.newClient address
    server.accept listener

    let target ← client.bootstrap
    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM target payload
    assertEqual response.capTable.caps.size 0

    server.release
    client.onDisconnect
    client.release
    runtime.releaseListener listener
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testRuntimeServerBootstrapFactoryLifecycle : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  let factoryCalls ← IO.mkRef (0 : Nat)
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let bootstrap ← runtime.registerEchoTarget
    let server ← runtime.newServerWithBootstrapFactory (fun _ => do
      factoryCalls.modify (fun n => n + 1)
      pure bootstrap)
    let listener ← server.listen address
    let client ← runtime.newClient address
    server.accept listener

    let target ← client.bootstrap
    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM target payload
    assertEqual response.capTable.caps.size 0
    assertTrue ((← factoryCalls.get) > 0) "bootstrap factory callback was not invoked"

    client.release
    server.release
    runtime.releaseListener listener
    runtime.releaseTarget bootstrap
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testRuntimeServerDrain : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let bootstrap ← runtime.registerEchoTarget
    let server ← runtime.newServer bootstrap
    let listener ← server.listen address
    let client ← runtime.newClient address
    server.accept listener

    let target ← client.bootstrap
    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM target payload
    assertEqual response.capTable.caps.size 0

    client.release
    server.drain
    server.release
    runtime.releaseListener listener
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testRuntimeAsyncConnectAndWhenResolvedStart : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let bootstrap ← runtime.registerEchoTarget
    let server ← runtime.newServer bootstrap
    let listener ← server.listen address
    let connectPromise ← runtime.connectStart address
    server.accept listener
    let target ← connectPromise.awaitTarget
    let whenResolvedPromise ← runtime.targetWhenResolvedStart target
    whenResolvedPromise.await

    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM target payload
    assertEqual response.capTable.caps.size 0

    runtime.releaseTarget target
    server.release
    runtime.releaseListener listener
    runtime.releaseTarget bootstrap
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testRuntimeAsyncClientLifecyclePrimitives : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let bootstrap ← runtime.registerEchoTarget
    let server ← runtime.newServer bootstrap
    let listener ← server.listen address
    let clientPromise ← runtime.newClientStart address
    let acceptPromise ← server.acceptStart listener
    let client ← clientPromise.awaitClient
    acceptPromise.await

    let target ← client.bootstrap
    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM target payload
    assertEqual response.capTable.caps.size 0

    runtime.releaseTarget target
    let disconnectPromise ← client.onDisconnectStart
    client.release
    disconnectPromise.await
    let drainPromise ← server.drainStart
    drainPromise.await

    server.release
    runtime.releaseListener listener
    runtime.releaseTarget bootstrap
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testRuntimeClientQueueMetrics : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let bootstrap ← runtime.registerEchoTarget
    let server ← runtime.newServer bootstrap
    let listener ← server.listen address
    let client ← runtime.newClient address
    server.accept listener

    let target ← client.bootstrap
    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM target payload
    assertEqual response.capTable.caps.size 0

    let queueSize ← client.queueSize
    let queueCount ← client.queueCount
    let outgoingWait ← client.outgoingWaitNanos
    assertEqual queueSize (UInt64.ofNat 0)
    assertEqual queueCount (UInt64.ofNat 0)
    assertTrue (outgoingWait ≤ UInt64.ofNat 1_000_000_000) "outgoing wait exceeded expected bound"

    client.release
    server.release
    runtime.releaseListener listener
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testRuntimeClientSetFlowLimit : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let bootstrap ← runtime.registerEchoTarget
    let server ← runtime.newServer bootstrap
    let listener ← server.listen address
    let client ← runtime.newClient address
    server.accept listener

    client.setFlowLimit (UInt64.ofNat 65_536)
    let target ← client.bootstrap
    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM target payload
    assertEqual response.capTable.caps.size 0

    client.release
    server.release
    runtime.releaseListener listener
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testRuntimeMClientServerLifecycle : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let (response, queueSize, queueCount) ← Capnp.Rpc.RuntimeM.run runtime do
      let bootstrap ← Capnp.Rpc.RuntimeM.registerEchoTarget
      let server ← Capnp.Rpc.RuntimeM.newServer bootstrap
      let listener ← Capnp.Rpc.RuntimeM.serverListen server address
      let client ← Capnp.Rpc.RuntimeM.newClient address
      Capnp.Rpc.RuntimeM.serverAccept server listener
      Capnp.Rpc.RuntimeM.clientSetFlowLimit client (UInt64.ofNat 65_536)

      let target ← Capnp.Rpc.RuntimeM.clientBootstrap client
      let response ← Echo.callFooM target payload
      let queueSize ← Capnp.Rpc.RuntimeM.clientQueueSize client
      let queueCount ← Capnp.Rpc.RuntimeM.clientQueueCount client

      Capnp.Rpc.RuntimeM.serverRelease server
      Capnp.Rpc.RuntimeM.clientOnDisconnect client
      Capnp.Rpc.RuntimeM.clientRelease client
      Capnp.Rpc.RuntimeM.releaseListener listener
      pure (response, queueSize, queueCount)

    assertEqual response.capTable.caps.size 0
    assertEqual queueSize (UInt64.ofNat 0)
    assertEqual queueCount (UInt64.ofNat 0)
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testRuntimeClientReleaseErrors : IO Unit := do
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let bootstrap ← runtime.registerEchoTarget
    let server ← runtime.newServer bootstrap
    let listener ← server.listen address
    let client ← runtime.newClient address
    server.accept listener

    client.release

    let failedBootstrapAfterRelease ←
      try
        let _ ← client.bootstrap
        pure false
      catch _ =>
        pure true
    assertEqual failedBootstrapAfterRelease true

    let failedDoubleRelease ←
      try
        client.release
        pure false
      catch _ =>
        pure true
    assertEqual failedDoubleRelease true

    let failedQueueSizeAfterRelease ←
      try
        let _ ← client.queueSize
        pure false
      catch _ =>
        pure true
    assertEqual failedQueueSizeAfterRelease true

    let failedOnDisconnectAfterRelease ←
      try
        client.onDisconnect
        pure false
      catch _ =>
        pure true
    assertEqual failedOnDisconnectAfterRelease true

    server.release
    runtime.releaseListener listener
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testRuntimeServerReleaseErrors : IO Unit := do
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let bootstrap ← runtime.registerEchoTarget
    let server ← runtime.newServer bootstrap
    let listener ← server.listen address
    let client ← runtime.newClient address
    server.accept listener

    server.release

    let failedDrainAfterRelease ←
      try
        server.drain
        pure false
      catch _ =>
        pure true
    assertEqual failedDrainAfterRelease true

    let failedDoubleRelease ←
      try
        server.release
        pure false
      catch _ =>
        pure true
    assertEqual failedDoubleRelease true

    let failedAcceptAfterRelease ←
      try
        server.accept listener
        pure false
      catch _ =>
        pure true
    assertEqual failedAcceptAfterRelease true

    client.release
    runtime.releaseListener listener
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testRuntimeClientOnDisconnectAfterServerRelease : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let bootstrap ← runtime.registerEchoTarget
    let server ← runtime.newServer bootstrap
    let listener ← server.listen address
    let client ← runtime.newClient address
    server.accept listener

    let target ← client.bootstrap
    let _ ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM target payload

    server.release
    client.onDisconnect

    let failedCallAfterDisconnect ←
      try
        let _ ← Capnp.Rpc.RuntimeM.run runtime do
          Echo.callFooM target payload
        pure false
      catch _ =>
        pure true
    assertEqual failedCallAfterDisconnect true

    client.release
    runtime.releaseListener listener
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testRuntimeMScopedResources : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let response ← Capnp.Rpc.RuntimeM.run runtime do
      let bootstrap ← Capnp.Rpc.RuntimeM.registerEchoTarget
      Capnp.Rpc.RuntimeM.withServer bootstrap fun server => do
        Capnp.Rpc.RuntimeM.withServerListener server address (fun listener => do
          Capnp.Rpc.RuntimeM.withClient address (fun client => do
            Capnp.Rpc.RuntimeM.serverAccept server listener
            let target ← Capnp.Rpc.RuntimeM.clientBootstrap client
            Echo.callFooM target payload))

    assertEqual response.capTable.caps.size 0

    let failedConnectAfterScope ←
      try
        let _ ← runtime.connect address
        pure false
      catch _ =>
        pure true
    assertEqual failedConnectAfterScope true
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testRuntimeScopedResources : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let bootstrap ← runtime.registerEchoTarget
    let response ← runtime.withServer bootstrap fun server => do
      server.withListener address (fun listener => do
        runtime.withClient address (fun client => do
          server.accept listener
          let target ← client.bootstrap
          Capnp.Rpc.RuntimeM.run runtime do
            Echo.callFooM target payload))
    assertEqual response.capTable.caps.size 0

    let failedConnectAfterScope ←
      try
        let _ ← runtime.connect address
        pure false
      catch _ =>
        pure true
    assertEqual failedConnectAfterScope true
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testRuntimeScopedResourcesExplicitPortHintArgOrder : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let bootstrap ← runtime.registerEchoTarget
    let response ← runtime.withServer bootstrap fun server => do
      server.withListener address
        (fun listener => do
          runtime.withClient address
            (fun client => do
              server.accept listener
              let target ← client.bootstrap
              Capnp.Rpc.RuntimeM.run runtime do
                Echo.callFooM target payload)
            0)
        0
    assertEqual response.capTable.caps.size 0
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testRuntimeRegisterHandlerTargetNetwork : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let seenTarget ← IO.mkRef (UInt32.ofNat 0)
  let seenMethod ← IO.mkRef ({ interfaceId := 0, methodId := 0 } : Capnp.Rpc.Method)
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let bootstrap ← runtime.registerHandlerTarget (fun target method req => do
      seenTarget.set target
      seenMethod.set method
      pure req)
    let server ← runtime.newServer bootstrap
    let listener ← server.listen address
    let client ← runtime.newClient address
    server.accept listener

    let remoteTarget ← client.bootstrap
    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM remoteTarget payload
    assertEqual response.capTable.caps.size 0

    assertEqual (← seenTarget.get) bootstrap
    let method := (← seenMethod.get)
    assertEqual method.interfaceId Echo.interfaceId
    assertEqual method.methodId Echo.fooMethodId

    client.release
    server.release
    runtime.releaseListener listener
    runtime.releaseTarget bootstrap
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testInteropCppClientCallsLeanServer : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let seenMethod ← IO.mkRef ({ interfaceId := 0, methodId := 0 } : Capnp.Rpc.Method)
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let bootstrap ← runtime.registerHandlerTarget (fun _ method req => do
      seenMethod.set method
      pure req)
    let server ← runtime.newServer bootstrap
    let listener ← server.listen address
    let response ← Capnp.Rpc.Interop.cppCallWithAccept runtime server listener address Echo.fooMethod
      payload

    assertEqual response.capTable.caps.size 0
    let method := (← seenMethod.get)
    assertEqual method.interfaceId Echo.interfaceId
    assertEqual method.methodId Echo.fooMethodId

    server.release
    runtime.releaseListener listener
    runtime.releaseTarget bootstrap
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testInteropCppClientCallsLeanServerWithCapabilities : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let localCap ← runtime.registerEchoTarget
    let capPayload := mkCapabilityPayload localCap

    let bootstrap ← runtime.registerHandlerTarget (fun _ _ req => pure req)
    let server ← runtime.newServer bootstrap
    let listener ← server.listen address
    let response ← Capnp.Rpc.Interop.cppCallWithAccept runtime server listener address Echo.fooMethod
      capPayload

    assertEqual response.capTable.caps.size 1
    let returnedCap? := Capnp.readCapabilityFromTable response.capTable (Capnp.getRoot response.msg)
    assertEqual returnedCap?.isSome true
    match returnedCap? with
    | none =>
        throw (IO.userError "RPC response is missing expected capability")
    | some returnedCap =>
        let echoed ← Capnp.Rpc.RuntimeM.run runtime do
          Echo.callFooM returnedCap payload
        assertEqual echoed.capTable.caps.size 0
        runtime.releaseCapTable response.capTable

    server.release
    runtime.releaseListener listener
    runtime.releaseTarget bootstrap
    runtime.releaseTarget localCap
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testInteropLeanClientCallsCppServer : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let serveTask ← IO.asTask (Capnp.Rpc.Interop.cppServeEchoOnce address Echo.fooMethod)
    IO.sleep (UInt32.ofNat 20)

    let mut target? : Option Capnp.Rpc.Client := none
    let mut attempts := 0
    while target?.isNone && attempts < 20 do
      let nextTarget? ←
        try
          let c ← runtime.connect address
          pure (some (Capnp.Rpc.Client.ofCapability c))
        catch _ =>
          pure none
      target? := nextTarget?
      if target?.isNone then
        IO.sleep (UInt32.ofNat 10)
      attempts := attempts + 1

    let target ←
      match target? with
      | some c => pure c
      | none => throw (IO.userError "failed to connect Lean runtime target to C++ server")
    let responseResult : Except IO.Error Capnp.Rpc.Payload ←
      try
        let response ← Capnp.Rpc.RuntimeM.run runtime do
          Echo.callFooM target payload
        pure (Except.ok response)
      catch err =>
        pure (Except.error err)

    let response ←
      match responseResult with
      | Except.ok r => pure r
      | Except.error err =>
          match serveTask.get with
          | Except.ok _ =>
              throw err
          | Except.error serveErr =>
              throw (IO.userError s!"call failed ({err}); serve task failed ({serveErr})")
    assertEqual response.capTable.caps.size 0

    runtime.releaseTarget target

    match serveTask.get with
    | .ok observed =>
        assertEqual observed.capTable.caps.size 0
    | .error err =>
        throw err
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testInteropLeanClientObservesCppDisconnectAfterOneShot : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let serveTask ← IO.asTask (Capnp.Rpc.Interop.cppServeEchoOnce address Echo.fooMethod)
    IO.sleep (UInt32.ofNat 20)

    let target ← connectRuntimeTargetWithRetry runtime address
    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM target payload
    assertEqual response.capTable.caps.size 0

    runtime.releaseTarget target
    match serveTask.get with
    | .ok observed =>
        assertEqual observed.capTable.caps.size 0
    | .error err =>
        throw err

    let reconnectFailed ←
      try
        let target2 ← runtime.connect address
        runtime.releaseTarget target2
        pure false
      catch _ =>
        pure true
    assertEqual reconnectFailed true
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testInteropLeanClientCancelsPendingCallToCppDelayedServer : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let serveTask ← IO.asTask (
      Capnp.Rpc.Interop.cppServeDelayedEchoOnce address Echo.fooMethod (UInt32.ofNat 150))
    IO.sleep (UInt32.ofNat 20)

    let target ← connectRuntimeTargetWithRetry runtime address
    let pending ← runtime.startCall target Echo.fooMethod payload
    IO.sleep (UInt32.ofNat 10)
    pending.release

    let doubleReleaseFailed ←
      try
        pending.release
        pure false
      catch _ =>
        pure true
    assertEqual doubleReleaseFailed true

    -- If cancellation drops the in-flight request before send, force a request on
    -- the same connection so the one-shot C++ server task can complete deterministically.
    let _rescueOverSameConnection ←
      try
        let rescueResponse ← Capnp.Rpc.RuntimeM.run runtime do
          Echo.callFooM target payload
        assertEqual rescueResponse.capTable.caps.size 0
        pure true
      catch _ =>
        pure false

    runtime.releaseTarget target

    match serveTask.get with
    | .ok observed =>
        assertEqual observed.capTable.caps.size 0
    | .error err =>
        throw err
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testInteropLeanClientReceivesCppExceptionDetail : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let serveTask ← IO.asTask (Capnp.Rpc.Interop.cppServeThrowOnce address Echo.fooMethod true)
    IO.sleep (UInt32.ofNat 20)

    let target ← connectRuntimeTargetWithRetry runtime address
    let errMsg ←
      try
        let _ ← Capnp.Rpc.RuntimeM.run runtime do
          Echo.callFooM target payload
        pure ""
      catch err =>
        pure (toString err)
    if !(errMsg.containsSubstr "remote exception: test exception") then
      throw (IO.userError s!"missing remote exception text: {errMsg}")
    if !(errMsg.containsSubstr "remote detail[1]: cpp-detail-1") then
      throw (IO.userError s!"missing remote detail text: {errMsg}")

    runtime.releaseTarget target
    match serveTask.get with
    | .ok observed =>
        assertEqual observed.capTable.caps.size 0
    | .error err =>
        throw err
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testInteropCppClientPipeliningThroughLeanTailCallForwarder : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let sink ← runtime.registerEchoTarget
    let returnPayload := mkCapabilityPayload sink
    let returnCapTarget ← runtime.registerHandlerTarget (fun _ _ _ => do
      IO.sleep (UInt32.ofNat 30)
      pure returnPayload)
    let forwarder ← runtime.registerTailCallTarget returnCapTarget
    let server ← runtime.newServer forwarder
    let listener ← server.listen address
    let response ← Capnp.Rpc.Interop.cppCallPipelinedWithAccept
      runtime server listener address Echo.fooMethod payload payload
    assertEqual response.capTable.caps.size 0

    server.release
    runtime.releaseListener listener
    runtime.releaseTarget forwarder
    runtime.releaseTarget returnCapTarget
    runtime.releaseTarget sink
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testRuntimePendingCallPipeline : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let localCap ← runtime.registerEchoTarget
    let capPayload := mkCapabilityPayload localCap
    let pending ← runtime.startCall localCap Echo.fooMethod capPayload
    let pipelinedCap ← pending.getPipelinedCap

    let echoed ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM pipelinedCap payload
    assertEqual echoed.capTable.caps.size 0

    let response ← pending.await
    assertEqual response.capTable.caps.size 1
    runtime.releaseCapTable response.capTable

    runtime.releaseTarget pipelinedCap
    runtime.releaseTarget localCap
  finally
    runtime.shutdown

@[test]
def testRuntimeTargetWhenResolvedPipeline : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let localCap ← runtime.registerEchoTarget
    let returnPayload := mkCapabilityPayload localCap
    let bootstrap ← runtime.registerHandlerTarget (fun _ _ _ => pure returnPayload)
    let server ← runtime.newServer bootstrap
    let listener ← server.listen address
    let client ← runtime.newClient address
    server.accept listener

    let remoteTarget ← client.bootstrap
    let pending ← runtime.startCall remoteTarget Echo.fooMethod payload
    let pipelinedCap ← pending.getPipelinedCap
    runtime.targetWhenResolved pipelinedCap

    let echoed ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM pipelinedCap payload
    assertEqual echoed.capTable.caps.size 0

    let response ← pending.await
    assertEqual response.capTable.caps.size 1
    runtime.releaseCapTable response.capTable

    runtime.releaseTarget pipelinedCap
    runtime.releaseTarget remoteTarget
    client.release
    server.release
    runtime.releaseListener listener
    runtime.releaseTarget bootstrap
    runtime.releaseTarget localCap
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testRuntimePipelinedCallBeforeResolveOrdering : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let sink ← runtime.registerEchoTarget
    let returnPayload := mkCapabilityPayload sink
    let bootstrap ← runtime.registerHandlerTarget (fun _ _ _ => do
      IO.sleep (UInt32.ofNat 50)
      pure returnPayload)
    let server ← runtime.newServer bootstrap
    let listener ← server.listen address
    let client ← runtime.newClient address
    server.accept listener

    let remoteTarget ← client.bootstrap
    let pending ← runtime.startCall remoteTarget Echo.fooMethod payload
    let pipelinedCap ← pending.getPipelinedCap
    let pipelinedCallTask ← IO.asTask (Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM pipelinedCap payload)

    IO.sleep (UInt32.ofNat 10)
    let response ← pending.await
    assertEqual response.capTable.caps.size 1
    runtime.releaseCapTable response.capTable

    match pipelinedCallTask.get with
    | .ok pipelinedResponse =>
        assertEqual pipelinedResponse.capTable.caps.size 0
    | .error err =>
        throw err

    runtime.releaseTarget pipelinedCap
    runtime.releaseTarget remoteTarget
    client.release
    server.release
    runtime.releaseListener listener
    runtime.releaseTarget bootstrap
    runtime.releaseTarget sink
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testRuntimePendingCallRelease : IO Unit := do
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let target ← runtime.registerEchoTarget
    let pending ← runtime.startCall target Echo.fooMethod mkNullPayload
    pending.release
    let awaitFailed ←
      try
        let _ ← pending.await
        pure false
      catch _ =>
        pure true
    assertEqual awaitFailed true
    runtime.releaseTarget target
  finally
    runtime.shutdown

@[test]
def testRuntimeStreamingCall : IO Unit := do
  let seen ← IO.mkRef false
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let target ← runtime.registerHandlerTarget (fun _ method req => do
      if method.interfaceId == Echo.interfaceId && method.methodId == Echo.fooMethodId then
        seen.set true
      pure req)
    runtime.streamingCall target Echo.fooMethod mkNullPayload
    assertEqual (← seen.get) true
    runtime.releaseTarget target
  finally
    runtime.shutdown

@[test]
def testRuntimeTraceEncoderToggle : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.init
  let runFailingCall (traceEnabled : Bool) : IO String := do
    let (address, socketPath) ← mkUnixTestAddress
    try
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

      if traceEnabled then
        runtime.enableTraceEncoder
      else
        runtime.disableTraceEncoder

      let bootstrap ← runtime.registerHandlerTarget (fun _ method _ => do
        if method.interfaceId == Echo.interfaceId && method.methodId == Echo.fooMethodId then
          throw (IO.userError "trace test exception")
        pure mkNullPayload)
      try
        runtime.withServer bootstrap fun server => do
          server.withListener address (fun listener => do
            runtime.withClient address (fun client => do
              server.accept listener
              let target ← client.bootstrap
              try
                let _ ← Capnp.Rpc.RuntimeM.run runtime do
                  Echo.callFooM target payload
                pure ""
              catch err =>
                pure (toString err)
              finally
                runtime.releaseTarget target))
      finally
        runtime.releaseTarget bootstrap
    finally
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

  try
    let disabledMessage ← runFailingCall false
    if !(disabledMessage.containsSubstr "remote exception:") then
      throw (IO.userError s!"unexpected disabled error text: {disabledMessage}")
    if disabledMessage.containsSubstr "remote trace:" then
      throw (IO.userError s!"disabled call unexpectedly included remote trace: {disabledMessage}")

    let enabledMessage ← runFailingCall true
    if !(enabledMessage.containsSubstr "remote exception:") then
      throw (IO.userError s!"unexpected enabled error text: {enabledMessage}")
    if !(enabledMessage.containsSubstr "remote trace: lean4-rpc-trace:") then
      throw (IO.userError s!"enabled call did not include encoded remote trace: {enabledMessage}")
  finally
    runtime.shutdown

@[test]
def testRuntimeSetTraceEncoderOnExistingConnection : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let bootstrap ← runtime.registerHandlerTarget (fun _ method _ => do
      if method.interfaceId == Echo.interfaceId && method.methodId == Echo.fooMethodId then
        throw (IO.userError "dynamic trace test exception")
      pure mkNullPayload)
    let server ← runtime.newServer bootstrap
    let listener ← server.listen address
    let client ← runtime.newClient address
    server.accept listener
    let target ← client.bootstrap

    runtime.disableTraceEncoder
    let disabledErr ←
      try
        let _ ← Capnp.Rpc.RuntimeM.run runtime do
          Echo.callFooM target payload
        pure ""
      catch err =>
        pure (toString err)
    if disabledErr.containsSubstr "remote trace:" then
      throw (IO.userError s!"trace unexpectedly present before enabling callback: {disabledErr}")

    runtime.setTraceEncoder (fun description => pure s!"custom-trace<{description}>")
    let enabledErr ←
      try
        let _ ← Capnp.Rpc.RuntimeM.run runtime do
          Echo.callFooM target payload
        pure ""
      catch err =>
        pure (toString err)
    if !(enabledErr.containsSubstr "remote trace: custom-trace<") then
      throw (IO.userError s!"missing callback trace on existing connection: {enabledErr}")

    runtime.disableTraceEncoder
    let disabledAgainErr ←
      try
        let _ ← Capnp.Rpc.RuntimeM.run runtime do
          Echo.callFooM target payload
        pure ""
      catch err =>
        pure (toString err)
    if disabledAgainErr.containsSubstr "remote trace:" then
      throw (IO.userError s!"trace unexpectedly present after disabling callback: {disabledAgainErr}")

    runtime.releaseTarget target
    client.release
    server.release
    runtime.releaseListener listener
    runtime.releaseTarget bootstrap
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testRuntimeTargetGetFdOption : IO Unit := do
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let target ← runtime.registerEchoTarget
    let fd? ← runtime.targetGetFd? target
    assertEqual fd?.isNone true
    runtime.releaseTarget target
  finally
    runtime.shutdown

@[test]
def testRuntimeTailCallForwardingTarget : IO Unit := do
  let seenMethod ← IO.mkRef ({ interfaceId := 0, methodId := 0 } : Capnp.Rpc.Method)
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let sink ← runtime.registerHandlerTarget (fun _ method req => do
      seenMethod.set method
      pure req)
    let forwarder ← runtime.registerTailCallTarget sink
    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM forwarder mkNullPayload
    assertEqual response.capTable.caps.size 0
    let method := (← seenMethod.get)
    assertEqual method.interfaceId Echo.interfaceId
    assertEqual method.methodId Echo.fooMethodId
    runtime.releaseTarget forwarder
    runtime.releaseTarget sink
  finally
    runtime.shutdown

@[test]
def testRuntimeTailCallHandlerTargetFromRequestCapability : IO Unit := do
  let runtime ← Capnp.Rpc.Runtime.init
  let payload : Capnp.Rpc.Payload := mkNullPayload
  try
    let sink ← runtime.registerEchoTarget
    let capPayload := mkCapabilityPayload sink
    let forwarder ← runtime.registerTailCallHandlerTarget (fun _ method req => do
      assertEqual method.interfaceId Echo.interfaceId
      assertEqual method.methodId Echo.fooMethodId
      let cap? := Capnp.readCapabilityFromTable req.capTable (Capnp.getRoot req.msg)
      match cap? with
      | some cap => pure cap
      | none =>
          throw (IO.userError "tail-call handler expected request capability"))

    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM forwarder capPayload
    assertEqual response.capTable.caps.size 1
    let returnedCap? := Capnp.readCapabilityFromTable response.capTable (Capnp.getRoot response.msg)
    assertEqual returnedCap?.isSome true
    match returnedCap? with
    | none =>
        throw (IO.userError "tail-call response missing expected capability")
    | some returnedCap =>
        let echoed ← Capnp.Rpc.RuntimeM.run runtime do
          Echo.callFooM returnedCap payload
        assertEqual echoed.capTable.caps.size 0

    runtime.releaseCapTable response.capTable
    runtime.releaseTarget forwarder
    runtime.releaseTarget sink
  finally
    runtime.shutdown

@[test]
def testRuntimeInitWithFdLimit : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.initWithFdLimit (UInt32.ofNat 4)
  try
    assertEqual (← runtime.isAlive) true
    let target ← runtime.registerEchoTarget
    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM target payload
    assertEqual response.capTable.caps.size 0
    runtime.releaseTarget target
  finally
    runtime.shutdown

@[test]
def testRuntimeFdTargetLocalGetFd : IO Unit := do
  if System.Platform.isWindows then
    pure ()
  else
    let runtime ← Capnp.Rpc.Runtime.init
    try
      let fdTarget ← runtime.registerFdTarget (UInt32.ofNat 0)
      let fd? ← runtime.targetGetFd? fdTarget
      assertEqual fd?.isSome true
      runtime.releaseTarget fdTarget
    finally
      runtime.shutdown

@[test]
def testRuntimeFdPassingOverNetwork : IO Unit := do
  if System.Platform.isWindows then
    pure ()
  else
    let payload : Capnp.Rpc.Payload := mkNullPayload
    let (address, socketPath) ← mkUnixTestAddress
    let runtime ← Capnp.Rpc.Runtime.init
    try
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

      let fdTarget ← runtime.registerFdTarget (UInt32.ofNat 0)
      let returnPayload := mkCapabilityPayload fdTarget
      let bootstrap ← runtime.registerHandlerTarget (fun _ _ _ => pure returnPayload)
      let server ← runtime.newServer bootstrap
      let listener ← server.listen address
      let client ← runtime.newClient address
      server.accept listener

      let remoteTarget ← client.bootstrap
      let response ← Capnp.Rpc.RuntimeM.run runtime do
        Echo.callFooM remoteTarget payload
      let returnedCap? := Capnp.readCapabilityFromTable response.capTable (Capnp.getRoot response.msg)
      assertEqual returnedCap?.isSome true
      match returnedCap? with
      | none =>
          throw (IO.userError "RPC response is missing expected capability")
      | some returnedCap =>
          let fd? ← runtime.targetGetFd? returnedCap
          assertEqual fd?.isSome true
      runtime.releaseCapTable response.capTable
      runtime.releaseTarget remoteTarget
      client.release
      server.release
      runtime.releaseListener listener
      runtime.releaseTarget bootstrap
      runtime.releaseTarget fdTarget
    finally
      runtime.shutdown
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()
