import Capnp.Runtime

namespace Capnp
namespace Rpc

abbrev Payload := Capnp.RpcEnvelope

structure Method where
  interfaceId : UInt64
  methodId : UInt16
  deriving Inhabited, BEq, Repr

abbrev Client := Capnp.Capability
abbrev Listener := UInt32
abbrev RuntimeClient := UInt32
abbrev RuntimeServer := UInt32

@[inline] def Client.ofCapability (cap : Capnp.Capability) : Client := cap

@[inline] def Client.toCapability (client : Client) : Capnp.Capability := client

structure Backend where
  call : Client -> Method -> Payload -> IO Payload

@[inline] def call (backend : Backend) (target : Client) (method : Method)
    (payload : Payload := Capnp.emptyRpcEnvelope) : IO Payload :=
  backend.call target method payload

abbrev RawCall := Client -> Method -> ByteArray -> IO ByteArray

@[inline] def Payload.toBytes (payload : Payload) : ByteArray :=
  Capnp.writeMessage payload.msg

@[inline] def Payload.ofBytes (bytes : ByteArray) : Payload :=
  { msg := Capnp.readMessage bytes, capTable := Capnp.emptyCapTable }

def Backend.ofRawCall (rawCall : RawCall) : Backend where
  call := fun target method payload => do
    let requestBytes := payload.toBytes
    let responseBytes ← rawCall target method requestBytes
    return Payload.ofBytes responseBytes

@[extern "capnp_lean_rpc_raw_call_on_runtime"]
opaque ffiRawCallOnRuntimeImpl
    (runtime : UInt64) (target : UInt32) (interfaceId : UInt64) (methodId : UInt16)
    (request : @& ByteArray) : IO ByteArray

@[extern "capnp_lean_rpc_raw_call_with_caps_on_runtime"]
opaque ffiRawCallWithCapsOnRuntimeImpl
    (runtime : UInt64) (target : UInt32) (interfaceId : UInt64) (methodId : UInt16)
    (request : @& ByteArray) (requestCaps : @& ByteArray) : IO (ByteArray × ByteArray)

@[extern "capnp_lean_rpc_runtime_new"]
opaque ffiRuntimeNewImpl : IO UInt64

@[extern "capnp_lean_rpc_runtime_release"]
opaque ffiRuntimeReleaseImpl (runtime : UInt64) : IO Unit

@[extern "capnp_lean_rpc_runtime_is_alive"]
opaque ffiRuntimeIsAliveImpl (runtime : UInt64) : IO Bool

@[extern "capnp_lean_rpc_runtime_register_echo_target"]
opaque ffiRuntimeRegisterEchoTargetImpl (runtime : UInt64) : IO UInt32

@[extern "capnp_lean_rpc_runtime_release_target"]
opaque ffiRuntimeReleaseTargetImpl (runtime : UInt64) (target : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_connect"]
opaque ffiRuntimeConnectImpl (runtime : UInt64) (address : @& String) (portHint : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_listen_echo"]
opaque ffiRuntimeListenEchoImpl (runtime : UInt64) (address : @& String) (portHint : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_accept_echo"]
opaque ffiRuntimeAcceptEchoImpl (runtime : UInt64) (listener : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_release_listener"]
opaque ffiRuntimeReleaseListenerImpl (runtime : UInt64) (listener : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_new_client"]
opaque ffiRuntimeNewClientImpl
    (runtime : UInt64) (address : @& String) (portHint : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_release_client"]
opaque ffiRuntimeReleaseClientImpl (runtime : UInt64) (client : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_client_bootstrap"]
opaque ffiRuntimeClientBootstrapImpl (runtime : UInt64) (client : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_client_on_disconnect"]
opaque ffiRuntimeClientOnDisconnectImpl (runtime : UInt64) (client : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_client_set_flow_limit"]
opaque ffiRuntimeClientSetFlowLimitImpl
    (runtime : UInt64) (client : UInt32) (words : UInt64) : IO Unit

@[extern "capnp_lean_rpc_runtime_client_queue_size"]
opaque ffiRuntimeClientQueueSizeImpl (runtime : UInt64) (client : UInt32) : IO UInt64

@[extern "capnp_lean_rpc_runtime_client_queue_count"]
opaque ffiRuntimeClientQueueCountImpl (runtime : UInt64) (client : UInt32) : IO UInt64

@[extern "capnp_lean_rpc_runtime_client_outgoing_wait_nanos"]
opaque ffiRuntimeClientOutgoingWaitNanosImpl (runtime : UInt64) (client : UInt32) : IO UInt64

@[extern "capnp_lean_rpc_runtime_new_server"]
opaque ffiRuntimeNewServerImpl (runtime : UInt64) (bootstrap : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_release_server"]
opaque ffiRuntimeReleaseServerImpl (runtime : UInt64) (server : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_server_listen"]
opaque ffiRuntimeServerListenImpl
    (runtime : UInt64) (server : UInt32) (address : @& String) (portHint : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_server_accept"]
opaque ffiRuntimeServerAcceptImpl (runtime : UInt64) (server : UInt32) (listener : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_server_drain"]
opaque ffiRuntimeServerDrainImpl (runtime : UInt64) (server : UInt32) : IO Unit

structure Runtime where
  handle : UInt64
  deriving Inhabited, BEq, Repr

structure RuntimeClientRef where
  runtime : Runtime
  handle : RuntimeClient
  deriving Inhabited, BEq, Repr

structure RuntimeServerRef where
  runtime : Runtime
  handle : RuntimeServer
  deriving Inhabited, BEq, Repr

namespace CapTable

@[inline] def toBytes (t : Capnp.CapTable) : ByteArray := Id.run do
  let mut out := ByteArray.emptyWithCapacity (t.caps.size * 4)
  for cap in t.caps do
    out := Capnp.appendUInt32LE out cap
  return out

@[inline] def ofBytes (bytes : ByteArray) : Capnp.CapTable := Id.run do
  let mut caps : Array Capnp.Capability := #[]
  let mut i := 0
  while i + 4 ≤ bytes.size do
    caps := caps.push (Capnp.readUInt32LE bytes i)
    i := i + 4
  return { caps := caps }

end CapTable

namespace Runtime

@[inline] def init : IO Runtime := do
  return { handle := (← ffiRuntimeNewImpl) }

@[inline] def shutdown (runtime : Runtime) : IO Unit :=
  ffiRuntimeReleaseImpl runtime.handle

@[inline] def isAlive (runtime : Runtime) : IO Bool :=
  ffiRuntimeIsAliveImpl runtime.handle

@[inline] def registerEchoTarget (runtime : Runtime) : IO Client :=
  ffiRuntimeRegisterEchoTargetImpl runtime.handle

@[inline] def releaseTarget (runtime : Runtime) (target : Client) : IO Unit :=
  ffiRuntimeReleaseTargetImpl runtime.handle target

@[inline] def connect (runtime : Runtime) (address : String) (portHint : UInt32 := 0) : IO Client :=
  ffiRuntimeConnectImpl runtime.handle address portHint

@[inline] def listenEcho (runtime : Runtime) (address : String) (portHint : UInt32 := 0) :
    IO Listener :=
  ffiRuntimeListenEchoImpl runtime.handle address portHint

@[inline] def acceptEcho (runtime : Runtime) (listener : Listener) : IO Unit :=
  ffiRuntimeAcceptEchoImpl runtime.handle listener

@[inline] def releaseListener (runtime : Runtime) (listener : Listener) : IO Unit :=
  ffiRuntimeReleaseListenerImpl runtime.handle listener

@[inline] def newClient (runtime : Runtime) (address : String) (portHint : UInt32 := 0) :
    IO RuntimeClientRef := do
  return { runtime := runtime, handle := (← ffiRuntimeNewClientImpl runtime.handle address portHint) }

@[inline] def newServer (runtime : Runtime) (bootstrap : Client) : IO RuntimeServerRef := do
  return { runtime := runtime, handle := (← ffiRuntimeNewServerImpl runtime.handle bootstrap) }

@[inline] def rawCall (runtime : Runtime) : RawCall :=
  fun target method request =>
    ffiRawCallOnRuntimeImpl runtime.handle target method.interfaceId method.methodId request

@[inline] def backend (runtime : Runtime) : Backend where
  call := fun target method payload => do
    let requestBytes := payload.toBytes
    let requestCaps := CapTable.toBytes payload.capTable
    let (responseBytes, responseCaps) ← ffiRawCallWithCapsOnRuntimeImpl
      runtime.handle target method.interfaceId method.methodId requestBytes requestCaps
    return { msg := Capnp.readMessage responseBytes, capTable := CapTable.ofBytes responseCaps }

def withRuntime (action : Runtime -> IO α) : IO α := do
  let runtime ← init
  try
    action runtime
  finally
    runtime.shutdown

end Runtime

namespace RuntimeClientRef

@[inline] def release (client : RuntimeClientRef) : IO Unit :=
  ffiRuntimeReleaseClientImpl client.runtime.handle client.handle

@[inline] def bootstrap (client : RuntimeClientRef) : IO Client :=
  ffiRuntimeClientBootstrapImpl client.runtime.handle client.handle

@[inline] def onDisconnect (client : RuntimeClientRef) : IO Unit :=
  ffiRuntimeClientOnDisconnectImpl client.runtime.handle client.handle

@[inline] def setFlowLimit (client : RuntimeClientRef) (words : UInt64) : IO Unit :=
  ffiRuntimeClientSetFlowLimitImpl client.runtime.handle client.handle words

@[inline] def queueSize (client : RuntimeClientRef) : IO UInt64 :=
  ffiRuntimeClientQueueSizeImpl client.runtime.handle client.handle

@[inline] def queueCount (client : RuntimeClientRef) : IO UInt64 :=
  ffiRuntimeClientQueueCountImpl client.runtime.handle client.handle

@[inline] def outgoingWaitNanos (client : RuntimeClientRef) : IO UInt64 :=
  ffiRuntimeClientOutgoingWaitNanosImpl client.runtime.handle client.handle

end RuntimeClientRef

namespace RuntimeServerRef

@[inline] def release (server : RuntimeServerRef) : IO Unit :=
  ffiRuntimeReleaseServerImpl server.runtime.handle server.handle

@[inline] def listen (server : RuntimeServerRef) (address : String) (portHint : UInt32 := 0) :
    IO Listener :=
  ffiRuntimeServerListenImpl server.runtime.handle server.handle address portHint

@[inline] def accept (server : RuntimeServerRef) (listener : Listener) : IO Unit :=
  ffiRuntimeServerAcceptImpl server.runtime.handle server.handle listener

@[inline] def drain (server : RuntimeServerRef) : IO Unit :=
  ffiRuntimeServerDrainImpl server.runtime.handle server.handle

end RuntimeServerRef

abbrev RuntimeM := ReaderT Runtime IO

namespace RuntimeM

@[inline] def run (runtime : Runtime) (action : RuntimeM α) : IO α :=
  action runtime

@[inline] def runWithNewRuntime (action : RuntimeM α) : IO α :=
  Runtime.withRuntime fun runtime => action runtime

@[inline] def runtime : RuntimeM Runtime := read

@[inline] def backend : RuntimeM Backend := do
  return Runtime.backend (← runtime)

@[inline] def isAlive : RuntimeM Bool := do
  Runtime.isAlive (← runtime)

@[inline] def registerEchoTarget : RuntimeM Client := do
  Runtime.registerEchoTarget (← runtime)

@[inline] def releaseTarget (target : Client) : RuntimeM Unit := do
  Runtime.releaseTarget (← runtime) target

@[inline] def connect (address : String) (portHint : UInt32 := 0) : RuntimeM Client := do
  Runtime.connect (← runtime) address portHint

@[inline] def listenEcho (address : String) (portHint : UInt32 := 0) : RuntimeM Listener := do
  Runtime.listenEcho (← runtime) address portHint

@[inline] def acceptEcho (listener : Listener) : RuntimeM Unit := do
  Runtime.acceptEcho (← runtime) listener

@[inline] def releaseListener (listener : Listener) : RuntimeM Unit := do
  Runtime.releaseListener (← runtime) listener

@[inline] def newClient (address : String) (portHint : UInt32 := 0) : RuntimeM RuntimeClientRef := do
  Runtime.newClient (← runtime) address portHint

@[inline] def newServer (bootstrap : Client) : RuntimeM RuntimeServerRef := do
  Runtime.newServer (← runtime) bootstrap

@[inline] def clientRelease (client : RuntimeClientRef) : RuntimeM Unit := do
  client.release

@[inline] def clientBootstrap (client : RuntimeClientRef) : RuntimeM Client := do
  client.bootstrap

@[inline] def clientOnDisconnect (client : RuntimeClientRef) : RuntimeM Unit := do
  client.onDisconnect

@[inline] def clientSetFlowLimit (client : RuntimeClientRef) (words : UInt64) : RuntimeM Unit := do
  client.setFlowLimit words

@[inline] def clientQueueSize (client : RuntimeClientRef) : RuntimeM UInt64 := do
  client.queueSize

@[inline] def clientQueueCount (client : RuntimeClientRef) : RuntimeM UInt64 := do
  client.queueCount

@[inline] def clientOutgoingWaitNanos (client : RuntimeClientRef) : RuntimeM UInt64 := do
  client.outgoingWaitNanos

@[inline] def serverRelease (server : RuntimeServerRef) : RuntimeM Unit := do
  server.release

@[inline] def serverListen (server : RuntimeServerRef) (address : String)
    (portHint : UInt32 := 0) : RuntimeM Listener := do
  server.listen address portHint

@[inline] def serverAccept (server : RuntimeServerRef) (listener : Listener) : RuntimeM Unit := do
  server.accept listener

@[inline] def serverDrain (server : RuntimeServerRef) : RuntimeM Unit := do
  server.drain

@[inline] def call (target : Client) (method : Method)
    (payload : Payload := Capnp.emptyRpcEnvelope) : RuntimeM Payload := do
  Capnp.Rpc.call (← backend) target method payload

@[inline] def withBackend (f : Backend -> IO α) : RuntimeM α := do
  f (← backend)

end RuntimeM

@[inline] def ffiBackend (runtime : Runtime) : Backend :=
  runtime.backend

abbrev Handler := Client -> Payload -> IO Payload

structure Route where
  method : Method
  handler : Handler

structure Dispatch where
  routes : Array Route := #[]

@[inline] def Dispatch.empty : Dispatch := { routes := #[] }

@[inline] def Dispatch.register (d : Dispatch) (method : Method) (handler : Handler) : Dispatch :=
  { routes := d.routes.push { method := method, handler := handler } }

def Dispatch.findHandler? (d : Dispatch) (method : Method) : Option Handler := Id.run do
  let mut found : Option Handler := none
  for route in d.routes do
    if route.method == method then
      found := some route.handler
  return found

def Dispatch.toBackend (d : Dispatch)
    (onMissing : Client -> Method -> Payload -> IO Payload := fun _ _ _ => pure Capnp.emptyRpcEnvelope) :
    Backend where
  call := fun target method payload =>
    match d.findHandler? method with
    | some handler => handler target payload
    | none => onMissing target method payload

def echoBackend : Backend where
  call := fun _ _ payload => pure payload

def emptyBackend : Backend where
  call := fun _ _ _ => pure Capnp.emptyRpcEnvelope

end Rpc
end Capnp
