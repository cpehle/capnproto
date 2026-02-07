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

    client.release
    runtime.releaseListener listener
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()
