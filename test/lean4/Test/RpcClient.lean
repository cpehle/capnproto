import LeanTest
import Capnp.Rpc
import Capnp.KjAsync
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

def mkUInt64Payload (n : UInt64) : Capnp.Rpc.Payload := Id.run do
  let (_, builder) :=
    (do
      let root ← Capnp.getRootPointer
      let s ← Capnp.initStructPointer root 1 0
      Capnp.setUInt64 s 0 n
    ).run (Capnp.initMessageBuilder 16)
  { msg := Capnp.buildMessage builder, capTable := Capnp.emptyCapTable }

def readUInt64Payload (payload : Capnp.Rpc.Payload) : IO UInt64 := do
  let root := Capnp.getRoot payload.msg
  match Capnp.readStructChecked root with
  | .ok s =>
      pure (Capnp.getUInt64 s 0)
  | .error err =>
      throw (IO.userError s!"invalid uint64 RPC payload: {err}")

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

@[extern "capnp_lean_rpc_test_new_socketpair"]
opaque ffiNewSocketPairImpl : IO (UInt32 × UInt32)

@[extern "capnp_lean_rpc_test_close_fd"]
opaque ffiCloseFdImpl (fd : UInt32) : IO Unit

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
def testGeneratedRegisterAdvancedTypedTargetNetwork : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let seenFoo ← IO.mkRef false
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let sink ← runtime.registerEchoTarget
    let handler : Echo.AdvancedTypedServer := {
      foo := fun _ req reqCaps => do
        seenFoo.set true
        assertEqual reqCaps.caps.size 0
        assertEqual req.hasPayload false
        pure (Capnp.Rpc.Advanced.now
          (Capnp.Rpc.Advanced.forwardToCaller sink Echo.fooMethod payload
            Capnp.Rpc.AdvancedCallHints.withNoPromisePipelining))
      bar := fun _ _ _ =>
        pure (Capnp.Rpc.Advanced.now
          (Capnp.Rpc.Advanced.throwRemote "unexpected advanced typed bar handler invocation"))
    }
    let bootstrap ← Echo.registerAdvancedTypedTarget runtime handler
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
    runtime.releaseTarget sink
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testGeneratedRegisterStreamingTypedTargetNetwork : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let seenFoo ← IO.mkRef false
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let handler : Echo.StreamingTypedServer := {
      foo := fun _ req reqCaps => do
        seenFoo.set true
        assertEqual reqCaps.caps.size 0
        assertEqual req.hasPayload false
        pure (Capnp.Rpc.Advanced.now (Capnp.Rpc.Advanced.respond Capnp.emptyRpcEnvelope))
      bar := fun _ _ _ =>
        pure (Capnp.Rpc.Advanced.now
          (Capnp.Rpc.Advanced.throwRemote "unexpected streaming typed bar handler invocation"))
    }
    let bootstrap ← Echo.registerStreamingTypedTarget runtime handler
    let server ← runtime.newServer bootstrap
    let listener ← server.listen address
    let client ← runtime.newClient address
    server.accept listener

    let remoteTarget ← client.bootstrap
    runtime.streamingCall remoteTarget Echo.fooMethod payload
    let mut seen := (← seenFoo.get)
    let mut attempts := 0
    while !seen && attempts < 200 do
      IO.sleep (UInt32.ofNat 5)
      seen := (← seenFoo.get)
      attempts := attempts + 1
    assertEqual seen true

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
def testRuntimePromiseCapabilityPipelining : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let sink ← runtime.registerEchoTarget
    let (promiseCap, fulfiller) ← runtime.newPromiseCapability

    let handler : Echo.AdvancedTypedServer := {
      foo := fun _ req reqCaps => do
        let _ := req
        let _ := reqCaps
        let pipeline := mkCapabilityPayload promiseCap
        let deferred ← Capnp.Rpc.Advanced.defer (next := do
          IO.sleep (UInt32.ofNat 50)
          fulfiller.fulfill sink
          pure (Capnp.Rpc.Advanced.respond pipeline))
        pure (Capnp.Rpc.Advanced.setPipeline pipeline deferred)
      bar := fun _ _ _ =>
        pure (Capnp.Rpc.Advanced.now (Capnp.Rpc.Advanced.respond Capnp.emptyRpcEnvelope))
    }

    let bootstrap ← Echo.registerAdvancedTypedTarget runtime handler
    let server ← runtime.newServer bootstrap
    let listener ← server.listen address
    let client ← runtime.newClient address
    server.accept listener

    let remoteTarget ← client.bootstrap
    let pending ← Echo.startFoo runtime remoteTarget payload
    let pipelinedCap ← Echo.getFooPipelinedCap pending

    let pipelinedResponse ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM pipelinedCap payload
    assertEqual pipelinedResponse.capTable.caps.size 0
    assertEqual (Capnp.isNullPointer (Capnp.getRoot pipelinedResponse.msg)) true

    let response ← Echo.awaitFoo pending
    assertEqual response.capTable.caps.size 1
    runtime.releaseCapTable response.capTable
    runtime.releaseTarget pipelinedCap

    client.release
    server.release
    runtime.releaseListener listener
    runtime.releaseTarget bootstrap
    runtime.releaseTarget promiseCap
    fulfiller.release
    runtime.releaseTarget sink
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

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
def testRuntimeServerBootstrapFactoryFailure : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let server ← runtime.newServerWithBootstrapFactory (fun _ => do
      throw (IO.userError "expected bootstrap factory failure"))
    let listener ← server.listen address
    let client ← runtime.newClient address
    server.accept listener

    let target ← client.bootstrap
    let errMsg ← try
      let _ ← Capnp.Rpc.RuntimeM.run runtime do
        Echo.callFooM target payload
      pure ""
    catch err =>
      pure (toString err)
    if !(errMsg.containsSubstr "Lean bootstrap factory returned IO error") then
      throw (IO.userError s!"missing bootstrap factory error text: {errMsg}")

    runtime.releaseTarget target
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
def testRuntimeResourceCounts : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    assertEqual (← runtime.targetCount) (UInt64.ofNat 0)
    assertEqual (← runtime.listenerCount) (UInt64.ofNat 0)
    assertEqual (← runtime.clientCount) (UInt64.ofNat 0)
    assertEqual (← runtime.serverCount) (UInt64.ofNat 0)
    assertEqual (← runtime.pendingCallCount) (UInt64.ofNat 0)

    let bootstrap ← runtime.registerEchoTarget
    assertEqual (← runtime.targetCount) (UInt64.ofNat 1)

    let retained ← runtime.retainTarget bootstrap
    assertEqual (← runtime.targetCount) (UInt64.ofNat 2)
    runtime.releaseTarget retained
    assertEqual (← runtime.targetCount) (UInt64.ofNat 1)

    let server ← runtime.newServer bootstrap
    assertEqual (← runtime.serverCount) (UInt64.ofNat 1)
    let listener ← server.listen address
    assertEqual (← runtime.listenerCount) (UInt64.ofNat 1)
    let client ← runtime.newClient address
    assertEqual (← runtime.clientCount) (UInt64.ofNat 1)

    server.accept listener
    let target ← client.bootstrap
    assertEqual (← runtime.targetCount) (UInt64.ofNat 2)

    let pending ← runtime.startCall target Echo.fooMethod payload
    assertEqual (← runtime.pendingCallCount) (UInt64.ofNat 1)
    let response ← pending.await
    assertEqual response.capTable.caps.size 0
    assertEqual (← runtime.pendingCallCount) (UInt64.ofNat 0)

    runtime.releaseTarget target
    assertEqual (← runtime.targetCount) (UInt64.ofNat 1)
    client.release
    assertEqual (← runtime.clientCount) (UInt64.ofNat 0)
    server.release
    assertEqual (← runtime.serverCount) (UInt64.ofNat 0)
    runtime.releaseListener listener
    assertEqual (← runtime.listenerCount) (UInt64.ofNat 0)
    runtime.releaseTarget bootstrap
    assertEqual (← runtime.targetCount) (UInt64.ofNat 0)
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testRuntimeMCallAndPendingResultHelpers : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let target ← Capnp.Rpc.RuntimeM.run runtime do
      Capnp.Rpc.RuntimeM.registerAdvancedHandlerTarget (fun _ _ _ => do
        pure (.throwRemote "runtimem call failed" "runtimem-detail".toUTF8))

    let callRes ← Capnp.Rpc.RuntimeM.run runtime do
      Capnp.Rpc.RuntimeM.callResult target Echo.fooMethod payload
    match callRes with
    | .ok _ =>
        throw (IO.userError "expected RuntimeM.callResult to return a remote exception")
    | .error ex =>
        assertEqual ex.type .failed
        if !(ex.description.containsSubstr "runtimem call failed") then
          throw (IO.userError s!"missing RuntimeM.callResult exception text: {ex.description}")
        assertEqual ex.detail "runtimem-detail".toUTF8

    let pendingRes ← Capnp.Rpc.RuntimeM.run runtime do
      let pending ← Capnp.Rpc.RuntimeM.startCall target Echo.fooMethod payload
      Capnp.Rpc.RuntimeM.pendingCallAwaitResult pending
    match pendingRes with
    | .ok _ =>
        throw (IO.userError "expected RuntimeM.pendingCallAwaitResult to return a remote exception")
    | .error ex =>
        assertEqual ex.type .failed
        if !(ex.description.containsSubstr "runtimem call failed") then
          throw (IO.userError s!"missing RuntimeM.pendingCallAwaitResult exception text: {ex.description}")
        assertEqual ex.detail "runtimem-detail".toUTF8

    let pendingOutcome ← Capnp.Rpc.RuntimeM.run runtime do
      let pending ← Capnp.Rpc.RuntimeM.startCall target Echo.fooMethod payload
      Capnp.Rpc.RuntimeM.pendingCallAwaitOutcome pending
    match pendingOutcome with
    | .ok _ _ =>
        throw (IO.userError "expected RuntimeM.pendingCallAwaitOutcome to report an exception")
    | .error ex =>
        assertEqual ex.type .failed
        if !(ex.description.containsSubstr "runtimem call failed") then
          throw (IO.userError s!"missing RuntimeM.pendingCallAwaitOutcome exception text: {ex.description}")
        assertEqual ex.detail "runtimem-detail".toUTF8

    runtime.releaseTarget target
  finally
    runtime.shutdown

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
def testRuntimeListenerRuntimeMismatchErrors : IO Unit := do
  let (address, socketPath) ← mkUnixTestAddress
  let runtimeA ← Capnp.Rpc.Runtime.init
  let runtimeB ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let bootstrapA ← runtimeA.registerEchoTarget
    let serverA ← runtimeA.newServer bootstrapA
    let listenerA ← serverA.listen address

    let bootstrapB ← runtimeB.registerEchoTarget
    let serverB ← runtimeB.newServer bootstrapB

    let releaseErr ←
      try
        runtimeB.releaseListener listenerA
        pure ""
      catch e =>
        pure (toString e)
    if !(releaseErr.containsSubstr "different Capnp.Rpc runtime") then
      throw (IO.userError s!"expected listener runtime mismatch error, got: {releaseErr}")

    let acceptErr ←
      try
        serverB.accept listenerA
        pure ""
      catch e =>
        pure (toString e)
    if !(acceptErr.containsSubstr "different Capnp.Rpc runtime") then
      throw (IO.userError s!"expected server/listener runtime mismatch error, got: {acceptErr}")

    runtimeA.releaseListener listenerA
    serverA.release
    runtimeA.releaseTarget bootstrapA
    serverB.release
    runtimeB.releaseTarget bootstrapB
  finally
    runtimeA.shutdown
    runtimeB.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testRuntimeTransportRuntimeMismatchErrors : IO Unit := do
  let runtimeA ← Capnp.Rpc.Runtime.init
  let runtimeB ← Capnp.Rpc.Runtime.init
  try
    let (transportA, transportB) ← runtimeA.newTransportPipe

    let connectErr ←
      try
        let _ ← runtimeB.connectTransport transportA
        pure ""
      catch e =>
        pure (toString e)
    if !(connectErr.containsSubstr "different Capnp.Rpc runtime") then
      throw (IO.userError s!"expected transport runtime mismatch error, got: {connectErr}")

    let getFdErr ←
      try
        let _ ← runtimeB.transportGetFd? transportA
        pure ""
      catch e =>
        pure (toString e)
    if !(getFdErr.containsSubstr "different Capnp.Rpc runtime") then
      throw (IO.userError s!"expected transport fd runtime mismatch error, got: {getFdErr}")

    let releaseErr ←
      try
        runtimeB.releaseTransport transportA
        pure ""
      catch e =>
        pure (toString e)
    if !(releaseErr.containsSubstr "different Capnp.Rpc runtime") then
      throw (IO.userError s!"expected transport release runtime mismatch error, got: {releaseErr}")

    runtimeA.releaseTransport transportA
    runtimeA.releaseTransport transportB
  finally
    runtimeA.shutdown
    runtimeB.shutdown

@[test]
def testRuntimeMHandleRuntimeMismatchErrors : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtimeA ← Capnp.Rpc.Runtime.init
  let runtimeB ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let bootstrapA ← runtimeA.registerEchoTarget
    let serverA ← runtimeA.newServer bootstrapA
    let listenerA ← serverA.listen address

    let registerPromiseA ← runtimeA.connectStart address
    let unitPromiseA ← serverA.acceptStart listenerA
    let pendingA ← runtimeA.startCall bootstrapA Echo.fooMethod payload

    let registerErr ←
      try
        let _ ← Capnp.Rpc.RuntimeM.run runtimeB do
          Capnp.Rpc.RuntimeM.registerPromiseAwait registerPromiseA
        pure ""
      catch e =>
        pure (toString e)
    if !(registerErr.containsSubstr "different Capnp.Rpc runtime") then
      throw (IO.userError s!"expected RuntimeM register promise mismatch error, got: {registerErr}")

    let unitErr ←
      try
        let _ ← Capnp.Rpc.RuntimeM.run runtimeB do
          Capnp.Rpc.RuntimeM.unitPromiseAwait unitPromiseA
        pure ""
      catch e =>
        pure (toString e)
    if !(unitErr.containsSubstr "different Capnp.Rpc runtime") then
      throw (IO.userError s!"expected RuntimeM unit promise mismatch error, got: {unitErr}")

    let pendingErr ←
      try
        let _ ← Capnp.Rpc.RuntimeM.run runtimeB do
          Capnp.Rpc.RuntimeM.pendingCallRelease pendingA
        pure ""
      catch e =>
        pure (toString e)
    if !(pendingErr.containsSubstr "different Capnp.Rpc runtime") then
      throw (IO.userError s!"expected RuntimeM pending call mismatch error, got: {pendingErr}")

    registerPromiseA.cancel
    registerPromiseA.release
    unitPromiseA.cancel
    unitPromiseA.release
    pendingA.release
    runtimeA.releaseListener listenerA
    serverA.release
    runtimeA.releaseTarget bootstrapA
  finally
    runtimeA.shutdown
    runtimeB.shutdown
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
def testRuntimeHandlerIoErrorCleansRequestCaps : IO Unit := do
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let sink ← runtime.registerEchoTarget
    let broken ← runtime.registerHandlerTarget (fun _ _ _ => do
      throw (IO.userError "expected handler failure"))
    let loopback ← runtime.registerLoopbackTarget broken
    let baselineTargets := (← runtime.targetCount)

    let errMsg ←
      try
        let _ ← Capnp.Rpc.RuntimeM.run runtime do
          Echo.callFooM loopback (mkCapabilityPayload sink)
        pure ""
      catch err =>
        pure (toString err)
    if !(errMsg.containsSubstr "Lean RPC handler returned IO error") then
      throw (IO.userError s!"missing handler IO error text: {errMsg}")

    let rec waitForTargetCount (attempts : Nat) : IO Unit := do
      runtime.pump
      let current ← runtime.targetCount
      if current == baselineTargets then
        pure ()
      else
        match attempts with
        | 0 =>
            throw (IO.userError s!"request capability cleanup did not converge: {current} vs {baselineTargets}")
        | attempts + 1 =>
            IO.sleep (UInt32.ofNat 10)
            waitForTargetCount attempts
    waitForTargetCount 200

    runtime.releaseTarget loopback
    runtime.releaseTarget broken
    runtime.releaseTarget sink
  finally
    runtime.shutdown

@[test]
def testRuntimeTailCallHandlerIoErrorCleansRequestCaps : IO Unit := do
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let sink ← runtime.registerEchoTarget
    let broken ← runtime.registerTailCallHandlerTarget (fun _ _ _ => do
      throw (IO.userError "expected tail-call handler failure"))
    let loopback ← runtime.registerLoopbackTarget broken
    let baselineTargets := (← runtime.targetCount)

    let errMsg ←
      try
        let _ ← Capnp.Rpc.RuntimeM.run runtime do
          Echo.callFooM loopback (mkCapabilityPayload sink)
        pure ""
      catch err =>
        pure (toString err)
    if !(errMsg.containsSubstr "Lean RPC tail-call handler returned IO error") then
      throw (IO.userError s!"missing tail-call handler IO error text: {errMsg}")

    let rec waitForTargetCount (attempts : Nat) : IO Unit := do
      runtime.pump
      let current ← runtime.targetCount
      if current == baselineTargets then
        pure ()
      else
        match attempts with
        | 0 =>
            throw (IO.userError s!"request capability cleanup did not converge: {current} vs {baselineTargets}")
        | attempts + 1 =>
            IO.sleep (UInt32.ofNat 10)
            waitForTargetCount attempts
    waitForTargetCount 200

    runtime.releaseTarget loopback
    runtime.releaseTarget broken
    runtime.releaseTarget sink
  finally
    runtime.shutdown

@[test]
def testRuntimeAdvancedHandlerIoErrorCleansRequestCaps : IO Unit := do
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let sink ← runtime.registerEchoTarget
    let broken ← runtime.registerAdvancedHandlerTarget (fun _ _ _ => do
      throw (IO.userError "expected advanced handler failure"))
    let loopback ← runtime.registerLoopbackTarget broken
    let baselineTargets := (← runtime.targetCount)

    let errMsg ←
      try
        let _ ← Capnp.Rpc.RuntimeM.run runtime do
          Echo.callFooM loopback (mkCapabilityPayload sink)
        pure ""
      catch err =>
        pure (toString err)
    if !(errMsg.containsSubstr "Lean RPC advanced handler returned IO error") then
      throw (IO.userError s!"missing advanced handler IO error text: {errMsg}")

    let rec waitForTargetCount (attempts : Nat) : IO Unit := do
      runtime.pump
      let current ← runtime.targetCount
      if current == baselineTargets then
        pure ()
      else
        match attempts with
        | 0 =>
            throw (IO.userError s!"request capability cleanup did not converge: {current} vs {baselineTargets}")
        | attempts + 1 =>
            IO.sleep (UInt32.ofNat 10)
            waitForTargetCount attempts
    waitForTargetCount 200

    runtime.releaseTarget loopback
    runtime.releaseTarget broken
    runtime.releaseTarget sink
  finally
    runtime.shutdown

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
    let res ← runtime.callResult target Echo.fooMethod payload
    match res with
    | .ok _ =>
        throw (IO.userError "expected C++ one-shot server to throw")
    | .error ex =>
        assertEqual ex.type .failed
        if !(ex.description.containsSubstr "remote exception: test exception") then
          throw (IO.userError s!"missing remote exception text: {ex.description}")
        assertEqual ex.detail "cpp-detail-1".toUTF8

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
def testInteropCppClientPipeliningThroughLeanAdvancedAsyncForwarder : IO Unit := do
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
    let forwarder ← runtime.registerAdvancedHandlerTarget (fun _ method req => do
      pure (.asyncCall returnCapTarget method req))
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
def testInteropCppClientPipeliningThroughLeanAdvancedTailCallForwarder : IO Unit := do
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
    let forwarder ← runtime.registerAdvancedHandlerTarget (fun _ method req => do
      pure (.tailCall returnCapTarget method req))
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
def testInteropCppClientReceivesLeanAdvancedRemoteDetail : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let throwing ← runtime.registerAdvancedHandlerTarget (fun _ _ _ => do
      pure (.throwRemote "lean advanced failure" "lean-detail-1".toUTF8))
    let server ← runtime.newServer throwing
    let listener ← server.listen address
    let errMsg ←
      try
        let _ ← Capnp.Rpc.Interop.cppCallWithAccept
          runtime server listener address Echo.fooMethod payload
        pure ""
      catch err =>
        pure (toString err)
    if !(errMsg.containsSubstr "remote exception: lean advanced failure") then
      throw (IO.userError s!"missing remote exception text: {errMsg}")
    if !(errMsg.containsSubstr "remote detail[1]: lean-detail-1") then
      throw (IO.userError s!"missing remote detail text: {errMsg}")

    server.release
    runtime.releaseListener listener
    runtime.releaseTarget throwing
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testInteropCppClientReceivesLeanAdvancedRemoteExceptionType : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let throwing ← runtime.registerAdvancedHandlerTarget (fun _ _ _ => do
      pure <| Capnp.Rpc.Advanced.throwRemoteWithType
        Capnp.Rpc.RemoteExceptionType.overloaded "lean overloaded failure")
    let server ← runtime.newServer throwing
    let listener ← server.listen address
    let errMsg ←
      try
        let _ ← Capnp.Rpc.Interop.cppCallWithAccept
          runtime server listener address Echo.fooMethod payload
        pure ""
      catch err =>
        pure (toString err)
    if !(errMsg.containsSubstr "exception type: OVERLOADED") then
      throw (IO.userError s!"missing exception type text: {errMsg}")

    server.release
    runtime.releaseListener listener
    runtime.releaseTarget throwing
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
def testRuntimeTwoHopPipelinedResolveOrdering : IO Unit := do
  let (frontAddress, frontSocketPath) ← mkUnixTestAddress
  let (backAddress, backSocketPath) ← mkUnixTestAddress
  let runtime ← Capnp.Rpc.Runtime.init
  let cleanupSocket (path : String) : IO Unit := do
    try
      IO.FS.removeFile path
    catch _ =>
      pure ()
  let step {α : Type} (label : String) (action : IO α) : IO α := do
    try
      action
    catch err =>
      throw (IO.userError s!"{label}: {err}")
  try
    cleanupSocket frontSocketPath
    cleanupSocket backSocketPath

    let nextExpected ← IO.mkRef (UInt64.ofNat 0)
    let callOrder ← step "register call-order target" <| runtime.registerHandlerTarget (fun _ method req => do
      if method.interfaceId != Echo.interfaceId || method.methodId != Echo.fooMethodId then
        throw (IO.userError
          s!"unexpected method in call-order target: {method.interfaceId}/{method.methodId}")
      let expected ← readUInt64Payload req
      let current ← nextExpected.get
      if expected != current then
        throw (IO.userError s!"call-order mismatch: expected {current}, got {expected}")
      nextExpected.set (current + (UInt64.ofNat 1))
      pure (mkUInt64Payload current))

    let backBootstrap ← step "register back bootstrap" <| runtime.registerEchoTarget
    let backServer ← step "new back server" <| runtime.newServer backBootstrap
    let backListener ← step "listen back server" <| backServer.listen backAddress

    let proxyBootstrap ← step "connect proxy bootstrap to back" <| runtime.connect backAddress
    step "accept back server connection" <| backServer.accept backListener

    let frontServer ← step "new front server" <| runtime.newServer proxyBootstrap
    let frontListener ← step "listen front server" <| frontServer.listen frontAddress
    let frontClient ← step "new front client" <| runtime.newClient frontAddress
    step "accept front server connection" <| frontServer.accept frontListener

    let remoteBootstrap ← step "bootstrap front client" <| frontClient.bootstrap
    let pendingEcho ← step "start echo call through front" <|
      runtime.startCall remoteBootstrap Echo.fooMethod (mkCapabilityPayload callOrder)
    let pipelineCap ← step "get pipelined cap" <| pendingEcho.getPipelinedCap

    let call0Pending ← step "start call0" <|
      runtime.startCall pipelineCap Echo.fooMethod (mkUInt64Payload (UInt64.ofNat 0))
    let call1Pending ← step "start call1" <|
      runtime.startCall pipelineCap Echo.fooMethod (mkUInt64Payload (UInt64.ofNat 1))

    let earlyResponse ← step "early bar call" <| Capnp.Rpc.RuntimeM.run runtime do
      Echo.callBarM remoteBootstrap mkNullPayload
    assertEqual earlyResponse.capTable.caps.size 0

    let call2Pending ← step "start call2" <|
      runtime.startCall pipelineCap Echo.fooMethod (mkUInt64Payload (UInt64.ofNat 2))

    let echoResponse ← step "await echo call" <| pendingEcho.await
    assertEqual echoResponse.capTable.caps.size 1
    let resolvedCap? := Capnp.readCapabilityFromTable echoResponse.capTable (Capnp.getRoot echoResponse.msg)
    assertEqual resolvedCap?.isSome true

    let call3Pending ← step "start call3" <|
      runtime.startCall pipelineCap Echo.fooMethod (mkUInt64Payload (UInt64.ofNat 3))
    let call4Pending ← step "start call4" <|
      runtime.startCall pipelineCap Echo.fooMethod (mkUInt64Payload (UInt64.ofNat 4))
    let call5Pending ← step "start call5" <|
      runtime.startCall pipelineCap Echo.fooMethod (mkUInt64Payload (UInt64.ofNat 5))

    let call0Response ← step "await call0" <| call0Pending.await
    let call1Response ← step "await call1" <| call1Pending.await
    let call2Response ← step "await call2" <| call2Pending.await
    let call3Response ← step "await call3" <| call3Pending.await
    let call4Response ← step "await call4" <| call4Pending.await
    let call5Response ← step "await call5" <| call5Pending.await

    assertEqual (← readUInt64Payload call0Response) (UInt64.ofNat 0)
    assertEqual (← readUInt64Payload call1Response) (UInt64.ofNat 1)
    assertEqual (← readUInt64Payload call2Response) (UInt64.ofNat 2)
    assertEqual (← readUInt64Payload call3Response) (UInt64.ofNat 3)
    assertEqual (← readUInt64Payload call4Response) (UInt64.ofNat 4)
    assertEqual (← readUInt64Payload call5Response) (UInt64.ofNat 5)
    assertEqual (← nextExpected.get) (UInt64.ofNat 6)
  finally
    runtime.shutdown
    cleanupSocket frontSocketPath
    cleanupSocket backSocketPath

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
def testRuntimeRegisterStreamingHandlerTarget : IO Unit := do
  let seen ← IO.mkRef false
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let target ← runtime.registerStreamingHandlerTarget (fun _ method _ => do
      if method.interfaceId == Echo.interfaceId && method.methodId == Echo.fooMethodId then
        seen.set true)
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
def testRuntimeRegisterLoopbackTargetUsesBootstrap : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let seenFoo ← IO.mkRef false
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let bootstrap ← runtime.registerHandlerTarget (fun _ method req => do
      if method.interfaceId == Echo.interfaceId && method.methodId == Echo.fooMethodId then
        seenFoo.set true
      pure req)
    let loopback ← runtime.registerLoopbackTarget bootstrap
    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM loopback payload
    assertEqual response.capTable.caps.size 0
    assertEqual (← seenFoo.get) true
    runtime.releaseTarget loopback
    runtime.releaseTarget bootstrap
  finally
    runtime.shutdown

@[test]
def testRuntimeAdvancedHandlerAsyncCallPipeline : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let sink ← runtime.registerEchoTarget
    let capPayload := mkCapabilityPayload sink
    let forwarder ← runtime.registerAdvancedHandlerTarget (fun _ method req => do
      pure (.asyncCall sink method req))

    let pending ← runtime.startCall forwarder Echo.fooMethod capPayload
    let pipelinedCap ← pending.getPipelinedCap
    let pipelinedResponse ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM pipelinedCap payload
    assertEqual pipelinedResponse.capTable.caps.size 0
    let response ← pending.await
    assertEqual response.capTable.caps.size 1
    runtime.releaseCapTable response.capTable
    runtime.releaseTarget pipelinedCap
    runtime.releaseTarget forwarder
    runtime.releaseTarget sink
  finally
    runtime.shutdown

@[test]
def testRuntimeAdvancedHandlerTailCall : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let seenMethod ← IO.mkRef ({ interfaceId := 0, methodId := 0 } : Capnp.Rpc.Method)
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let sink ← runtime.registerHandlerTarget (fun _ method req => do
      seenMethod.set method
      pure req)
    let forwarder ← runtime.registerAdvancedHandlerTarget (fun _ method req => do
      pure (.tailCall sink method req))
    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM forwarder payload
    assertEqual response.capTable.caps.size 0
    let method := (← seenMethod.get)
    assertEqual method.interfaceId Echo.interfaceId
    assertEqual method.methodId Echo.fooMethodId
    runtime.releaseTarget forwarder
    runtime.releaseTarget sink
  finally
    runtime.shutdown

@[test]
def testRuntimeAdvancedHandlerForwardCallSendResultsToCallerWithHints : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let seenMethod ← IO.mkRef ({ interfaceId := 0, methodId := 0 } : Capnp.Rpc.Method)
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let sink ← runtime.registerHandlerTarget (fun _ method req => do
      seenMethod.set method
      pure req)
    let forwarder ← runtime.registerAdvancedHandlerTarget (fun _ method req => do
      let hints : Capnp.Rpc.AdvancedCallHints := {
        noPromisePipelining := true
        onlyPromisePipeline := true
      }
      pure (Capnp.Rpc.Advanced.forwardToCaller sink method req hints))
    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM forwarder payload
    assertEqual response.capTable.caps.size 0
    let method := (← seenMethod.get)
    assertEqual method.interfaceId Echo.interfaceId
    assertEqual method.methodId Echo.fooMethodId
    runtime.releaseTarget forwarder
    runtime.releaseTarget sink
  finally
    runtime.shutdown

@[test]
def testRuntimeAdvancedHandlerStartsKjAsyncPromisesOnSameRuntime : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let sink ← runtime.registerEchoTarget
    let forwarder ← runtime.registerAdvancedHandlerTarget (fun _ method req => do
      let kjRuntime : Capnp.KjAsync.Runtime := { handle := runtime.handle }
      let first ← kjRuntime.sleepMillisStart (UInt32.ofNat 1)
      let second ← kjRuntime.sleepMillisStart (UInt32.ofNat 1)
      let seq ← kjRuntime.promiseThenStart first second
      seq.release
      pure (Capnp.Rpc.Advanced.forward sink method req))
    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM forwarder payload
    assertEqual response.capTable.caps.size 0
    runtime.releaseTarget forwarder
    runtime.releaseTarget sink
  finally
    runtime.shutdown

@[test]
def testRuntimeAdvancedHandlerRejectsKjAsyncAwaitOnWorkerThread : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let sink ← runtime.registerEchoTarget
    let seenError ← IO.mkRef ""
    let forwarder ← runtime.registerAdvancedHandlerTarget (fun _ method req => do
      let kjRuntime : Capnp.KjAsync.Runtime := { handle := runtime.handle }
      let promise ← kjRuntime.sleepMillisStart (UInt32.ofNat 1)
      let errMsg ←
        try
          promise.await
          pure ""
        catch err =>
          pure (toString err)
      promise.release
      seenError.set errMsg
      pure (Capnp.Rpc.Advanced.forward sink method req))
    let _ ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM forwarder payload
    let errMsg ← seenError.get
    if !(errMsg.containsSubstr "not allowed from the Capnp.Rpc worker thread") then
      throw (IO.userError s!"missing worker-thread await rejection text: {errMsg}")
    runtime.releaseTarget forwarder
    runtime.releaseTarget sink
  finally
    runtime.shutdown

@[test]
def testRuntimeAdvancedHandlerForwardCallOnlyPromisePipelineRequiresCaller : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let sink ← runtime.registerEchoTarget
    let forwarder ← runtime.registerAdvancedHandlerTarget (fun _ method req => do
      let opts : Capnp.Rpc.AdvancedForwardOptions :=
        Capnp.Rpc.AdvancedForwardOptions.setOnlyPromisePipeline {}
      pure (Capnp.Rpc.Advanced.forward sink method req opts))
    let errMsg ←
      try
        let _ ← Capnp.Rpc.RuntimeM.run runtime do
          Echo.callFooM forwarder payload
        pure ""
      catch err =>
        pure (toString err)
    if !(errMsg.containsSubstr "onlyPromisePipeline requires sendResultsTo.caller") then
      throw (IO.userError s!"missing onlyPromisePipeline validation error text: {errMsg}")
    runtime.releaseTarget forwarder
    runtime.releaseTarget sink
  finally
    runtime.shutdown

@[test]
def testRuntimeAdvancedHandlerSetPipelineValidationCleansRequestCaps : IO Unit := do
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let sink ← runtime.registerEchoTarget
    let invalid ← runtime.registerAdvancedHandlerTargetAsync (fun _ _ req => do
      let pipeline := mkCapabilityPayload sink
      pure (Capnp.Rpc.Advanced.setPipeline pipeline
        (Capnp.Rpc.Advanced.now (Capnp.Rpc.Advanced.respond req))))
    let loopback ← runtime.registerLoopbackTarget invalid
    let baselineTargets := (← runtime.targetCount)

    let errMsg ←
      try
        let _ ← Capnp.Rpc.RuntimeM.run runtime do
          Echo.callFooM loopback (mkCapabilityPayload sink)
        pure ""
      catch err =>
        pure (toString err)
    if !(errMsg.containsSubstr "setPipeline is only valid with defer") then
      throw (IO.userError s!"missing setPipeline validation error text: {errMsg}")

    let rec waitForTargetCount (attempts : Nat) : IO Unit := do
      runtime.pump
      let current ← runtime.targetCount
      if current == baselineTargets then
        pure ()
      else
        match attempts with
        | 0 =>
            throw (IO.userError s!"request capability cleanup did not converge: {current} vs {baselineTargets}")
        | attempts + 1 =>
            IO.sleep (UInt32.ofNat 10)
            waitForTargetCount attempts
    waitForTargetCount 200

    runtime.releaseTarget loopback
    runtime.releaseTarget invalid
    runtime.releaseTarget sink
  finally
    runtime.shutdown

@[test]
def testRuntimeAdvancedHandlerDuplicateSetPipelineCleansRequestCaps : IO Unit := do
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let sink ← runtime.registerEchoTarget
    let invalid ← runtime.registerAdvancedHandlerTargetAsync (fun _ _ req => do
      let pipeline := mkCapabilityPayload sink
      pure (Capnp.Rpc.Advanced.setPipeline pipeline
        (Capnp.Rpc.Advanced.setPipeline pipeline
          (Capnp.Rpc.Advanced.now (Capnp.Rpc.Advanced.respond req)))))
    let loopback ← runtime.registerLoopbackTarget invalid
    let baselineTargets := (← runtime.targetCount)

    let errMsg ←
      try
        let _ ← Capnp.Rpc.RuntimeM.run runtime do
          Echo.callFooM loopback (mkCapabilityPayload sink)
        pure ""
      catch err =>
        pure (toString err)
    if !(errMsg.containsSubstr "setPipeline may only be specified once") then
      throw (IO.userError s!"missing duplicate setPipeline validation error text: {errMsg}")

    let rec waitForTargetCount (attempts : Nat) : IO Unit := do
      runtime.pump
      let current ← runtime.targetCount
      if current == baselineTargets then
        pure ()
      else
        match attempts with
        | 0 =>
            throw (IO.userError s!"request capability cleanup did not converge: {current} vs {baselineTargets}")
        | attempts + 1 =>
            IO.sleep (UInt32.ofNat 10)
            waitForTargetCount attempts
    waitForTargetCount 200

    runtime.releaseTarget loopback
    runtime.releaseTarget invalid
    runtime.releaseTarget sink
  finally
    runtime.shutdown

@[test]
def testRuntimeAdvancedHandlerUnknownForwardTargetCleansRequestCaps : IO Unit := do
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let sink ← runtime.registerEchoTarget
    let unknownTarget : Capnp.Rpc.Client := UInt32.ofNat 424242
    let forwarder ← runtime.registerAdvancedHandlerTarget (fun _ method req => do
      pure (Capnp.Rpc.Advanced.forward unknownTarget method req))
    let loopback ← runtime.registerLoopbackTarget forwarder
    let baselineTargets := (← runtime.targetCount)

    let errMsg ←
      try
        let _ ← Capnp.Rpc.RuntimeM.run runtime do
          Echo.callFooM loopback (mkCapabilityPayload sink)
        pure ""
      catch err =>
        pure (toString err)
    if !(errMsg.containsSubstr "unknown RPC async-call target capability id from Lean handler") then
      throw (IO.userError s!"missing forward-target validation error text: {errMsg}")

    let rec waitForTargetCount (attempts : Nat) : IO Unit := do
      runtime.pump
      let current ← runtime.targetCount
      if current == baselineTargets then
        pure ()
      else
        match attempts with
        | 0 =>
            throw (IO.userError s!"request capability cleanup did not converge: {current} vs {baselineTargets}")
        | attempts + 1 =>
            IO.sleep (UInt32.ofNat 10)
            waitForTargetCount attempts
    waitForTargetCount 200

    runtime.releaseTarget loopback
    runtime.releaseTarget forwarder
    runtime.releaseTarget sink
  finally
    runtime.shutdown

@[test]
def testRuntimeAdvancedHandlerUnknownTailTargetCleansRequestCaps : IO Unit := do
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let sink ← runtime.registerEchoTarget
    let unknownTarget : Capnp.Rpc.Client := UInt32.ofNat 424243
    let forwarder ← runtime.registerAdvancedHandlerTarget (fun _ method req => do
      pure (Capnp.Rpc.Advanced.tailForward unknownTarget method req))
    let loopback ← runtime.registerLoopbackTarget forwarder
    let baselineTargets := (← runtime.targetCount)

    let errMsg ←
      try
        let _ ← Capnp.Rpc.RuntimeM.run runtime do
          Echo.callFooM loopback (mkCapabilityPayload sink)
        pure ""
      catch err =>
        pure (toString err)
    if !(errMsg.containsSubstr "unknown RPC tail-call target capability id from Lean advanced handler") then
      throw (IO.userError s!"missing tail-target validation error text: {errMsg}")

    let rec waitForTargetCount (attempts : Nat) : IO Unit := do
      runtime.pump
      let current ← runtime.targetCount
      if current == baselineTargets then
        pure ()
      else
        match attempts with
        | 0 =>
            throw (IO.userError s!"request capability cleanup did not converge: {current} vs {baselineTargets}")
        | attempts + 1 =>
            IO.sleep (UInt32.ofNat 10)
            waitForTargetCount attempts
    waitForTargetCount 200

    runtime.releaseTarget loopback
    runtime.releaseTarget forwarder
    runtime.releaseTarget sink
  finally
    runtime.shutdown

@[test]
def testRuntimeAdvancedHandlerDeferredCancellationReleasesRequestCaps : IO Unit := do
  let canceled ← IO.mkRef false
  let entered ← IO.mkRef false
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let sink ← runtime.registerEchoTarget
    let handler ← runtime.registerAdvancedHandlerTargetAsync (fun _ _ _ => do
      entered.set true
      let deferred ← Capnp.Rpc.Advanced.defer do
        let rec waitForCancel (attempts : Nat) : IO Capnp.Rpc.AdvancedHandlerResult := do
          if (← IO.checkCanceled) then
            canceled.set true
            pure (Capnp.Rpc.Advanced.respond mkNullPayload)
          else
            match attempts with
            | 0 =>
                pure (Capnp.Rpc.Advanced.respond mkNullPayload)
            | attempts + 1 =>
                IO.sleep (UInt32.ofNat 5)
                waitForCancel attempts
        waitForCancel 10000
      pure (.control { releaseParams := true, allowCancellation := true } deferred))
    let loopback ← runtime.registerLoopbackTarget handler

    let baselineTargets := (← runtime.targetCount)
    assertEqual baselineTargets (UInt64.ofNat 3)

    let pending ← runtime.startCall loopback Echo.fooMethod (mkCapabilityPayload sink)
    let rec waitForEntered (attempts : Nat) : IO Unit := do
      runtime.pump
      if (← entered.get) then
        pure ()
      else
        match attempts with
        | 0 =>
            throw (IO.userError "deferred handler was not entered")
        | attempts + 1 =>
            IO.sleep (UInt32.ofNat 10)
            waitForEntered attempts
    waitForEntered 500
    pending.release

    let rec waitForTargetCount (attempts : Nat) : IO Unit := do
      runtime.pump
      let current ← runtime.targetCount
      if current == baselineTargets then
        pure ()
      else
        match attempts with
        | 0 =>
            throw (IO.userError s!"request capability cleanup did not converge: {current} vs {baselineTargets}")
        | attempts + 1 =>
            IO.sleep (UInt32.ofNat 10)
            waitForTargetCount attempts

    let rec waitForCanceled (attempts : Nat) : IO Unit := do
      runtime.pump
      if (← canceled.get) then
        pure ()
      else
        match attempts with
        | 0 =>
            throw (IO.userError "deferred handler task was not canceled")
        | attempts + 1 =>
            IO.sleep (UInt32.ofNat 10)
            waitForCanceled attempts

    waitForTargetCount 200
    waitForCanceled 200

    runtime.releaseTarget loopback
    runtime.releaseTarget handler
    runtime.releaseTarget sink
    assertEqual (← runtime.targetCount) (UInt64.ofNat 0)
  finally
    runtime.shutdown

@[test]
def testRuntimeAdvancedHandlerDeferredCancellationDoesNotCarryAcrossCalls : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let sawCanceled ← IO.mkRef false
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let echo ← runtime.registerEchoTarget
    let canceledPending ← runtime.startCall echo Echo.fooMethod payload
    canceledPending.release
    runtime.releaseTarget echo

    let deferred ← runtime.registerAdvancedHandlerTargetAsync (fun _ _ req => do
      Capnp.Rpc.Advanced.defer (opts := { allowCancellation := true }) do
        IO.sleep (UInt32.ofNat 25)
        if (← IO.checkCanceled) then
          sawCanceled.set true
          pure (Capnp.Rpc.Advanced.throwRemote "unexpected deferred cancellation")
        else
          pure (Capnp.Rpc.Advanced.respond req))

    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM deferred payload
    assertEqual response.capTable.caps.size 0
    assertEqual (← sawCanceled.get) false
    runtime.releaseTarget deferred
  finally
    runtime.shutdown

@[test]
def testRuntimeAdvancedHandlerDeferredRespond : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let seenRespond ← IO.mkRef false
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let deferred ← runtime.registerAdvancedHandlerTargetAsync (fun _ _ req => do
      Capnp.Rpc.Advanced.defer do
        IO.sleep (UInt32.ofNat 25)
        seenRespond.set true
        pure (Capnp.Rpc.Advanced.respond req))
    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM deferred payload
    assertEqual response.capTable.caps.size 0
    assertEqual (← seenRespond.get) true
    runtime.releaseTarget deferred
  finally
    runtime.shutdown

@[test]
def testRuntimeAdvancedHandlerDeferredSetPipeline : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let (address, socketPath) ← mkUnixTestAddress
  let sinkSeen ← IO.mkRef false
  let runtime ← Capnp.Rpc.Runtime.init
  try
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

    let sink ← runtime.registerHandlerTarget (fun _ _ _ => do
      sinkSeen.set true
      pure payload)

    let delayedPayload := mkCapabilityPayload sink
    let handler ← runtime.registerAdvancedHandlerTargetAsync (fun _ _ _ => do
      let deferred ← Capnp.Rpc.Advanced.defer do
        IO.sleep (UInt32.ofNat 200)
        pure (Capnp.Rpc.Advanced.respond delayedPayload)
      pure (Capnp.Rpc.Advanced.setPipeline delayedPayload deferred))

    let server ← runtime.newServer handler
    let listener ← server.listen address
    let callTask ← IO.asTask (Capnp.Rpc.Interop.cppCallPipelinedCapOneShot
      address Echo.fooMethod payload payload)
    server.accept listener

    let rec waitForSink (attempts : Nat) : IO Unit := do
      if (← sinkSeen.get) then
        pure ()
      else
        match attempts with
        | 0 =>
            throw (IO.userError "pipelined call did not reach sink before handler finished")
        | attempts + 1 =>
            IO.sleep (UInt32.ofNat 5)
            waitForSink attempts
    waitForSink 20

    match callTask.get with
    | .ok res =>
        assertEqual res.capTable.caps.size 0
    | .error err =>
        throw err

    server.release
    runtime.releaseListener listener
    runtime.releaseTarget handler
    runtime.releaseTarget sink
  finally
    runtime.shutdown
    try
      IO.FS.removeFile socketPath
    catch _ =>
      pure ()

@[test]
def testRuntimeAdvancedHandlerDeferredWithControl : IO Unit := do
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let sink ← runtime.registerEchoTarget
    let forwarder ← runtime.registerAdvancedHandlerTargetAsync (fun _ _ _ => do
      let deferred ← Capnp.Rpc.Advanced.defer
        (next := do
          IO.sleep (UInt32.ofNat 25)
          pure (Capnp.Rpc.Advanced.respond mkNullPayload))
        (opts := {
          releaseParams := true
          allowCancellation := true
        })
      pure deferred)
    let baselineTargets := (← runtime.targetCount)
    assertEqual baselineTargets (UInt64.ofNat 2)

    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM forwarder (mkCapabilityPayload sink)
    assertEqual response.capTable.caps.size 0
    assertEqual (← runtime.targetCount) baselineTargets

    runtime.releaseTarget forwarder
    runtime.releaseTarget sink
    assertEqual (← runtime.targetCount) (UInt64.ofNat 0)
  finally
    runtime.shutdown

@[test]
def testRuntimeAdvancedHandlerDeferredLateAllowCancellationRejected : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let deferred ← runtime.registerAdvancedHandlerTargetAsync (fun _ _ req => do
      Capnp.Rpc.Advanced.defer do
        IO.sleep (UInt32.ofNat 25)
        pure (Capnp.Rpc.AdvancedHandlerResult.allowCancellation (Capnp.Rpc.Advanced.respond req)))
    let errMsg ←
      try
        let _ ← Capnp.Rpc.RuntimeM.run runtime do
          Echo.callFooM deferred payload
        pure ""
      catch err =>
        pure (toString err)
    if !(errMsg.containsSubstr "allowCancellation must be set before defer") then
      throw (IO.userError s!"missing deferred allowCancellation ordering error text: {errMsg}")
    runtime.releaseTarget deferred
  finally
    runtime.shutdown

@[test]
def testRuntimeAdvancedHandlerThrowRemote : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let throwing ← runtime.registerAdvancedHandlerTarget (fun _ _ _ => do
      pure (.throwRemote "advanced test exception" (ByteArray.mk #[97, 98])))
    let loopback ← runtime.registerLoopbackTarget throwing
    let errMsg ←
      try
        let _ ← Capnp.Rpc.RuntimeM.run runtime do
          Echo.callFooM loopback payload
        pure ""
      catch err =>
        pure (toString err)
    if !(errMsg.containsSubstr "remote exception: advanced test exception") then
      throw (IO.userError s!"missing remote exception text: {errMsg}")
    if !(errMsg.containsSubstr "remote detail[1]: ab") then
      throw (IO.userError s!"missing remote detail text: {errMsg}")
    runtime.releaseTarget loopback
    runtime.releaseTarget throwing
  finally
    runtime.shutdown

@[test]
def testRuntimeConnectFdAndServerAcceptFd : IO Unit := do
  if System.Platform.isWindows then
    pure ()
  else
    let payload : Capnp.Rpc.Payload := mkNullPayload
    let (clientFd, serverFd) ← ffiNewSocketPairImpl
    let runtime ← Capnp.Rpc.Runtime.init
    try
      let bootstrap ← runtime.registerEchoTarget
      let server ← runtime.newServer bootstrap
      server.acceptFd serverFd
      let target ← runtime.connectFd clientFd
      let response ← Capnp.Rpc.RuntimeM.run runtime do
        Echo.callFooM target payload
      assertEqual response.capTable.caps.size 0
      runtime.releaseTarget target
      server.release
      runtime.releaseTarget bootstrap
    finally
      runtime.shutdown
      try
        ffiCloseFdImpl clientFd
      catch _ =>
        pure ()
      try
        ffiCloseFdImpl serverFd
      catch _ =>
        pure ()

@[test]
def testRuntimeConnectTransportAndServerAcceptTransport : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let bootstrap ← runtime.registerEchoTarget
    let server ← runtime.newServer bootstrap
    let (clientTransport, serverTransport) ← runtime.newTransportPipe
    server.acceptTransport serverTransport
    let target ← runtime.connectTransport clientTransport
    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM target payload
    assertEqual response.capTable.caps.size 0
    runtime.releaseTarget target
    server.release
    runtime.releaseTarget bootstrap
  finally
    runtime.shutdown

@[test]
def testRuntimeTransportInjectionFromFd : IO Unit := do
  if System.Platform.isWindows then
    pure ()
  else
    let payload : Capnp.Rpc.Payload := mkNullPayload
    let (clientFd, serverFd) ← ffiNewSocketPairImpl
    let runtime ← Capnp.Rpc.Runtime.init
    try
      let bootstrap ← runtime.registerEchoTarget
      let server ← runtime.newServer bootstrap
      let clientTransport ← runtime.newTransportFromFd clientFd
      let serverTransport ← runtime.newTransportFromFd serverFd
      assertEqual (Option.isSome (← runtime.transportGetFd? clientTransport)) true
      assertEqual (Option.isSome (← runtime.transportGetFd? serverTransport)) true
      server.acceptTransport serverTransport
      let target ← runtime.connectTransport clientTransport
      let response ← Capnp.Rpc.RuntimeM.run runtime do
        Echo.callFooM target payload
      assertEqual response.capTable.caps.size 0
      runtime.releaseTarget target
      server.release
      runtime.releaseTarget bootstrap
    finally
      runtime.shutdown
      try
        ffiCloseFdImpl clientFd
      catch _ =>
        pure ()
      try
        ffiCloseFdImpl serverFd
      catch _ =>
        pure ()

@[test]
def testRuntimeTransportInjectionFromKjAsyncCapabilityPipe : IO Unit := do
  if System.Platform.isWindows then
    pure ()
  else
    let payload : Capnp.Rpc.Payload := mkNullPayload
    let kjRuntime ← Capnp.KjAsync.Runtime.init
    let runtime ← Capnp.Rpc.Runtime.init
    try
      let (clientConn, serverConn) ← kjRuntime.newCapabilityPipe
      let clientFd? ← clientConn.dupFd?
      let serverFd? ← serverConn.dupFd?
      clientConn.release
      serverConn.release

      match (clientFd?, serverFd?) with
      | (some clientFd, some serverFd) =>
          let bootstrap ← runtime.registerEchoTarget
          let server ← runtime.newServer bootstrap
          let clientTransport ← runtime.newTransportFromFdTake clientFd
          let serverTransport ← runtime.newTransportFromFdTake serverFd
          server.acceptTransport serverTransport
          let target ← runtime.connectTransport clientTransport

          let response ← Capnp.Rpc.RuntimeM.run runtime do
            Echo.callFooM target payload
          assertEqual response.capTable.caps.size 0

          runtime.releaseTarget target
          server.release
          runtime.releaseTarget bootstrap
      | _ =>
          throw (IO.userError
            "expected Capnp.KjAsync capability pipe connections to expose an fd")
    finally
      runtime.shutdown
      kjRuntime.shutdown

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

@[test]
def testRuntimeSharedAsyncHelpersForRegisterAndUnitPromises : IO Unit := do
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

      let bootstrap ← runtime.registerEchoTarget
      let server ← runtime.newServer bootstrap
      let listener ← server.listen address
      let acceptPromise ← server.acceptStart listener
      let connectPromise ← runtime.connectStart address

      let connectTask ← connectPromise.awaitAsTask
      let target ←
        match (← IO.wait connectTask) with
        | .ok connectedTarget => pure connectedTarget
        | .error err =>
            throw (IO.userError s!"register promise awaitAsTask failed: {err}")

      let acceptIoPromise ← acceptPromise.toIOPromise
      let acceptResult? ← IO.wait acceptIoPromise.result?
      match acceptResult? with
      | some (.ok ()) => pure ()
      | some (.error err) =>
          throw (IO.userError s!"unit promise toIOPromise failed: {err}")
      | none =>
          throw (IO.userError "unit promise toIOPromise dropped without a result")

      let response ← Capnp.Rpc.RuntimeM.run runtime do
        Echo.callFooM target payload
      assertEqual response.capTable.caps.size 0

      runtime.releaseTarget target
      runtime.releaseListener listener
      server.release
      runtime.releaseTarget bootstrap
    finally
      runtime.shutdown
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

@[test]
def testRuntimePendingCallSharedAsyncHelpers : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let target ← runtime.registerEchoTarget

    let pending ← runtime.startCall target Echo.fooMethod payload
    let pendingTask ← pending.awaitAsTask
    let response ←
      match (← IO.wait pendingTask) with
      | .ok rsp => pure rsp
      | .error err =>
          throw (IO.userError s!"pending call awaitAsTask failed: {err}")
    assertEqual response.capTable.caps.size 0

    let pending2 ← runtime.startCall target Echo.fooMethod payload
    let pendingIoPromise ← pending2.toIOPromise
    let pendingIoResult? ← IO.wait pendingIoPromise.result?
    match pendingIoResult? with
    | some (.ok rsp) =>
        assertEqual rsp.capTable.caps.size 0
    | some (.error err) =>
        throw (IO.userError s!"pending call toIOPromise failed: {err}")
    | none =>
        throw (IO.userError "pending call toIOPromise dropped without a result")

    runtime.releaseTarget target
  finally
    runtime.shutdown

@[test]
def testRuntimeMultiVatBasicThreePartyHandoff : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.init
  let bobCallCount ← IO.mkRef (0 : Nat)
  let heldCap ← IO.mkRef (none : Option Capnp.Rpc.Client)
  try
    let bobBootstrap ← runtime.registerHandlerTarget (fun _ method req => do
      if method.interfaceId == Echo.interfaceId && method.methodId == Echo.fooMethodId then
        bobCallCount.modify (fun n => n + 1)
      pure req)
    let carolBootstrap ← runtime.registerAdvancedHandlerTarget (fun _ method req => do
      if method.interfaceId == Echo.interfaceId && method.methodId == Echo.fooMethodId then
        let cap? := Capnp.readCapabilityFromTable req.capTable (Capnp.getRoot req.msg)
        match cap? with
        | some cap =>
            let retained ← runtime.retainTarget cap
            match (← heldCap.get) with
            | some previous => runtime.releaseTarget previous
            | none => pure ()
            heldCap.set (some retained)
        | none => heldCap.set none
        pure (Capnp.Rpc.Advanced.respond mkNullPayload)
      else if method.interfaceId == Echo.interfaceId && method.methodId == Echo.barMethodId then
        match (← heldCap.get) with
        | some target =>
            pure (Capnp.Rpc.Advanced.asyncForward target Echo.fooMethod payload)
        | none =>
            throw (IO.userError "Carol has no held capability")
      else
        pure (Capnp.Rpc.Advanced.respond req))

    let alice ← runtime.newMultiVatClient "alice"
    let bob ← runtime.newMultiVatServer "bob" bobBootstrap
    let carol ← runtime.newMultiVatServer "carol" carolBootstrap

    let bobCap ← alice.bootstrap { host := "bob", unique := false }
    let carolCap ← alice.bootstrap { host := "carol", unique := false }
    assertEqual (← runtime.multiVatHasConnection carol bob) false

    let _ ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM carolCap (mkCapabilityPayload bobCap)
    assertEqual (← bobCallCount.get) 0

    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callBarM carolCap payload
    assertEqual response.capTable.caps.size 0
    assertEqual (← bobCallCount.get) 1
    assertEqual (← runtime.multiVatHasConnection carol bob) true

    runtime.releaseTarget bobCap
    runtime.releaseTarget carolCap
    alice.release
    bob.release
    carol.release
    match (← heldCap.get) with
    | some cap => runtime.releaseTarget cap
    | none => pure ()
    runtime.releaseTarget bobBootstrap
    runtime.releaseTarget carolBootstrap
  finally
    runtime.shutdown

@[test]
def testRuntimeMultiVatBootstrapFactoryAuth : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.init
  let seenCaller ← IO.mkRef ({ host := "", unique := false } : Capnp.Rpc.VatId)
  try
    let bootstrap ← runtime.registerEchoTarget
    let alice ← runtime.newMultiVatClient "alice"
    let bob ← runtime.newMultiVatServerWithBootstrapFactory "bob" (fun caller => do
      seenCaller.set caller
      pure bootstrap)

    let target ← alice.bootstrap { host := "bob", unique := true }
    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM target payload
    assertEqual response.capTable.caps.size 0

    let caller := (← seenCaller.get)
    assertEqual caller.host "alice"
    assertEqual caller.unique true

    runtime.releaseTarget target
    alice.release
    bob.release
    runtime.releaseTarget bootstrap
  finally
    runtime.shutdown

@[test]
def testRuntimeMultiVatSturdyRefRestoreCallback : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.init
  let seenCaller ← IO.mkRef ({ host := "", unique := false } : Capnp.Rpc.VatId)
  let seenObjectId ← IO.mkRef ByteArray.empty
  try
    let bootstrap ← runtime.registerEchoTarget
    let alice ← runtime.newMultiVatClient "alice"
    let bob ← runtime.newMultiVatServer "bob" bootstrap
    bob.setRestorer (fun caller objectId => do
      seenCaller.set caller
      seenObjectId.set objectId
      pure bootstrap)

    let sturdyRef : Capnp.Rpc.SturdyRef := {
      vat := { host := "bob", unique := false }
      objectId := ByteArray.mk #[10, 20, 30, 40]
    }
    let target ← alice.restoreSturdyRef sturdyRef
    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM target payload
    assertEqual response.capTable.caps.size 0

    let caller := (← seenCaller.get)
    assertEqual caller.host "alice"
    assertEqual caller.unique false
    assertEqual ((← seenObjectId.get) == sturdyRef.objectId) true

    runtime.releaseTarget target
    bob.clearRestorer
    alice.release
    bob.release
    runtime.releaseTarget bootstrap
  finally
    runtime.shutdown

@[test]
def testRuntimeMultiVatPublishedSturdyRefAndStats : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let bootstrap ← runtime.registerEchoTarget
    let alice ← runtime.newMultiVatClient "alice"
    let bob ← runtime.newMultiVatServer "bob" bootstrap

    runtime.multiVatSetForwardingEnabled false
    assertEqual (← runtime.multiVatForwardCount) (UInt64.ofNat 0)
    assertEqual (← runtime.multiVatDeniedForwardCount) (UInt64.ofNat 0)

    bob.publishSturdyRef (ByteArray.mk #[1, 2, 3]) bootstrap
    let restored ← alice.restoreSturdyRef
      { vat := { host := "bob", unique := false }, objectId := ByteArray.mk #[1, 2, 3] }
    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM restored payload
    assertEqual response.capTable.caps.size 0

    let bootTarget ← alice.bootstrap { host := "bob", unique := false }
    assertEqual (← runtime.multiVatHasConnection alice bob) true

    runtime.releaseTarget bootTarget
    runtime.releaseTarget restored
    alice.release
    bob.release
    runtime.releaseTarget bootstrap
  finally
    runtime.shutdown

@[test]
def testRuntimeVatNetworkBootstrapPeer : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.init
  try
    let network := runtime.vatNetwork
    let bootstrap ← runtime.registerEchoTarget
    let alice ← network.newClient "alice-network"
    let bob ← network.newServer "bob-network" bootstrap
    let target ← network.bootstrap alice bob
    let response ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM target payload
    assertEqual response.capTable.caps.size 0
    assertEqual (← network.hasConnection alice bob) true
    runtime.releaseTarget target
    network.releasePeer alice
    network.releasePeer bob
    runtime.releaseTarget bootstrap
  finally
    runtime.shutdown

@[test]
def testRuntimeVatNetworkSturdyRefLifecycleHelpers : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.init
  let seenCaller ← IO.mkRef ({ host := "", unique := false } : Capnp.Rpc.VatId)
  let seenObjectId ← IO.mkRef ByteArray.empty
  try
    let network := runtime.vatNetwork
    let bootstrap ← runtime.registerEchoTarget
    let alice ← network.newClient "alice-network-sturdy"
    let bob ← network.newServer "bob-network-sturdy" bootstrap

    let restoredObjectId := ByteArray.mk #[9, 8, 7, 6]
    network.setRestorer bob (fun caller objectId => do
      seenCaller.set caller
      seenObjectId.set objectId
      pure bootstrap)
    let restoredViaRestorer ← network.restoreSturdyRef alice {
      vat := { host := "bob-network-sturdy", unique := false }
      objectId := restoredObjectId
    }
    let response1 ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM restoredViaRestorer payload
    assertEqual response1.capTable.caps.size 0
    let caller := (← seenCaller.get)
    assertEqual caller.host "alice-network-sturdy"
    assertEqual caller.unique false
    assertEqual ((← seenObjectId.get) == restoredObjectId) true
    runtime.releaseTarget restoredViaRestorer

    network.clearRestorer bob
    let publishedObjectId := ByteArray.mk #[1, 2, 3, 4]
    network.publishSturdyRef bob publishedObjectId bootstrap
    let restoredViaPublish ← network.restoreSturdyRef alice {
      vat := { host := "bob-network-sturdy", unique := false }
      objectId := publishedObjectId
    }
    let response2 ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM restoredViaPublish payload
    assertEqual response2.capTable.caps.size 0
    runtime.releaseTarget restoredViaPublish

    network.releasePeer alice
    network.releasePeer bob
    runtime.releaseTarget bootstrap
  finally
    runtime.shutdown

@[test]
def testRuntimeVatNetworkPeerRuntimeMismatchForSturdyRefOps : IO Unit := do
  let runtimeA ← Capnp.Rpc.Runtime.init
  let runtimeB ← Capnp.Rpc.Runtime.init
  try
    let networkA := runtimeA.vatNetwork
    let networkB := runtimeB.vatNetwork
    let bootstrapA ← runtimeA.registerEchoTarget
    let bootstrapB ← runtimeB.registerEchoTarget
    let aliceA ← networkA.newClient "alice-network-mismatch-a"
    let bobB ← networkB.newServer "bob-network-mismatch-b" bootstrapB

    let bootstrapErr ←
      try
        let _ ← networkA.bootstrap aliceA bobB
        pure ""
      catch err =>
        pure (toString err)
    if !(bootstrapErr.containsSubstr "VatNetwork.bootstrap: peer belongs to a different runtime") then
      throw (IO.userError s!"expected vat-network bootstrap mismatch error, got: {bootstrapErr}")

    let hasConnectionErr ←
      try
        let _ ← networkA.hasConnection aliceA bobB
        pure ""
      catch err =>
        pure (toString err)
    if !(hasConnectionErr.containsSubstr "VatNetwork.hasConnection: peer belongs to a different runtime") then
      throw (IO.userError s!"expected vat-network hasConnection mismatch error, got: {hasConnectionErr}")

    let setRestorerErr ←
      try
        networkA.setRestorer bobB (fun _ _ => pure bootstrapA)
        pure ""
      catch err =>
        pure (toString err)
    if !(setRestorerErr.containsSubstr "VatNetwork.setRestorer: peer belongs to a different runtime") then
      throw (IO.userError s!"expected vat-network setRestorer mismatch error, got: {setRestorerErr}")

    let clearRestorerErr ←
      try
        networkA.clearRestorer bobB
        pure ""
      catch err =>
        pure (toString err)
    if !(clearRestorerErr.containsSubstr "VatNetwork.clearRestorer: peer belongs to a different runtime") then
      throw (IO.userError s!"expected vat-network clearRestorer mismatch error, got: {clearRestorerErr}")

    let publishErr ←
      try
        networkA.publishSturdyRef bobB ByteArray.empty bootstrapA
        pure ""
      catch err =>
        pure (toString err)
    if !(publishErr.containsSubstr "VatNetwork.publishSturdyRef: peer belongs to a different runtime") then
      throw (IO.userError s!"expected vat-network publishSturdyRef mismatch error, got: {publishErr}")

    let restoreErr ←
      try
        let _ ← networkA.restoreSturdyRef bobB
          { vat := { host := "bob-network-mismatch-b", unique := false }, objectId := ByteArray.empty }
        pure ""
      catch err =>
        pure (toString err)
    if !(restoreErr.containsSubstr "VatNetwork.restoreSturdyRef: peer belongs to a different runtime") then
      throw (IO.userError s!"expected vat-network restoreSturdyRef mismatch error, got: {restoreErr}")

    let releaseErr ←
      try
        networkA.releasePeer bobB
        pure ""
      catch err =>
        pure (toString err)
    if !(releaseErr.containsSubstr "VatNetwork.releasePeer: peer belongs to a different runtime") then
      throw (IO.userError s!"expected vat-network releasePeer mismatch error, got: {releaseErr}")

    networkA.releasePeer aliceA
    networkB.releasePeer bobB
    runtimeA.releaseTarget bootstrapA
    runtimeB.releaseTarget bootstrapB
  finally
    runtimeA.shutdown
    runtimeB.shutdown

@[test]
def testRuntimeMultiVatThirdPartyTokenStats : IO Unit := do
  let payload : Capnp.Rpc.Payload := mkNullPayload
  let runtime ← Capnp.Rpc.Runtime.init
  let heldCap ← IO.mkRef (none : Option Capnp.Rpc.Client)
  try
    let bobBootstrap ← runtime.registerHandlerTarget (fun _ _ req => pure req)
    let carolBootstrap ← runtime.registerAdvancedHandlerTarget (fun _ method req => do
      if method.interfaceId == Echo.interfaceId && method.methodId == Echo.fooMethodId then
        let cap? := Capnp.readCapabilityFromTable req.capTable (Capnp.getRoot req.msg)
        match cap? with
        | some cap =>
            let retained ← runtime.retainTarget cap
            match (← heldCap.get) with
            | some previous => runtime.releaseTarget previous
            | none => pure ()
            heldCap.set (some retained)
        | none => heldCap.set none
        pure (Capnp.Rpc.Advanced.respond mkNullPayload)
      else if method.interfaceId == Echo.interfaceId && method.methodId == Echo.barMethodId then
        match (← heldCap.get) with
        | some target =>
            pure (Capnp.Rpc.Advanced.asyncForward target Echo.fooMethod payload)
        | none =>
            throw (IO.userError "Carol has no held capability")
      else
        pure (Capnp.Rpc.Advanced.respond req))

    let network := runtime.vatNetwork
    let alice ← network.newClient "alice-token"
    let bob ← network.newServer "bob-token" bobBootstrap
    let carol ← network.newServer "carol-token" carolBootstrap

    network.resetForwardingStats
    let stats0 ← network.stats
    assertEqual stats0.forwardCount (UInt64.ofNat 0)
    assertEqual stats0.deniedForwardCount (UInt64.ofNat 0)
    assertEqual stats0.thirdPartyTokenCount (UInt64.ofNat 0)

    let bobCap ← network.bootstrap alice bob
    let carolCap ← network.bootstrap alice carol
    let _ ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callFooM carolCap (mkCapabilityPayload bobCap)
    let _ ← Capnp.Rpc.RuntimeM.run runtime do
      Echo.callBarM carolCap payload

    let stats1 ← network.stats
    assertTrue (stats1.thirdPartyTokenCount > (UInt64.ofNat 0))
      "expected third-party handoff token count to increase"

    runtime.releaseTarget bobCap
    runtime.releaseTarget carolCap
    match (← heldCap.get) with
    | some cap => runtime.releaseTarget cap
    | none => pure ()
    network.releasePeer alice
    network.releasePeer bob
    network.releasePeer carol
    runtime.releaseTarget bobBootstrap
    runtime.releaseTarget carolBootstrap
  finally
    runtime.shutdown
