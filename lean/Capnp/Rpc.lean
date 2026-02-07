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
abbrev RuntimePendingCall := UInt32

@[inline] def Client.ofCapability (cap : Capnp.Capability) : Client := cap

@[inline] def Client.toCapability (client : Client) : Capnp.Capability := client

structure Backend where
  call : Client -> Method -> Payload -> IO Payload

@[inline] def call (backend : Backend) (target : Client) (method : Method)
    (payload : Payload := Capnp.emptyRpcEnvelope) : IO Payload :=
  backend.call target method payload

abbrev RawCall := Client -> Method -> ByteArray -> IO ByteArray
abbrev RawHandlerCall := Client -> UInt64 -> UInt16 -> ByteArray -> ByteArray ->
    IO (ByteArray × ByteArray)

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

@[extern "capnp_lean_rpc_runtime_start_call_with_caps"]
opaque ffiRuntimeStartCallWithCapsImpl
    (runtime : UInt64) (target : UInt32) (interfaceId : UInt64) (methodId : UInt16)
    (request : @& ByteArray) (requestCaps : @& ByteArray) : IO UInt32

@[extern "capnp_lean_rpc_runtime_pending_call_await"]
opaque ffiRuntimePendingCallAwaitImpl
    (runtime : UInt64) (pendingCall : UInt32) : IO (ByteArray × ByteArray)

@[extern "capnp_lean_rpc_runtime_pending_call_release"]
opaque ffiRuntimePendingCallReleaseImpl (runtime : UInt64) (pendingCall : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_pending_call_get_pipelined_cap"]
opaque ffiRuntimePendingCallGetPipelinedCapImpl
    (runtime : UInt64) (pendingCall : UInt32) (pipelineOps : @& ByteArray) : IO UInt32

@[extern "capnp_lean_rpc_runtime_streaming_call_with_caps"]
opaque ffiRuntimeStreamingCallWithCapsImpl
    (runtime : UInt64) (target : UInt32) (interfaceId : UInt64) (methodId : UInt16)
    (request : @& ByteArray) (requestCaps : @& ByteArray) : IO Unit

@[extern "capnp_lean_rpc_runtime_target_get_fd"]
opaque ffiRuntimeTargetGetFdImpl (runtime : UInt64) (target : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_target_when_resolved"]
opaque ffiRuntimeTargetWhenResolvedImpl (runtime : UInt64) (target : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_enable_trace_encoder"]
opaque ffiRuntimeEnableTraceEncoderImpl (runtime : UInt64) : IO Unit

@[extern "capnp_lean_rpc_runtime_disable_trace_encoder"]
opaque ffiRuntimeDisableTraceEncoderImpl (runtime : UInt64) : IO Unit

@[extern "capnp_lean_rpc_runtime_new"]
opaque ffiRuntimeNewImpl : IO UInt64

@[extern "capnp_lean_rpc_runtime_release"]
opaque ffiRuntimeReleaseImpl (runtime : UInt64) : IO Unit

@[extern "capnp_lean_rpc_runtime_is_alive"]
opaque ffiRuntimeIsAliveImpl (runtime : UInt64) : IO Bool

@[extern "capnp_lean_rpc_runtime_register_echo_target"]
opaque ffiRuntimeRegisterEchoTargetImpl (runtime : UInt64) : IO UInt32

@[extern "capnp_lean_rpc_runtime_register_handler_target"]
opaque ffiRuntimeRegisterHandlerTargetImpl (runtime : UInt64) (handler : @& RawHandlerCall) :
    IO UInt32

@[extern "capnp_lean_rpc_runtime_register_tailcall_target"]
opaque ffiRuntimeRegisterTailCallTargetImpl (runtime : UInt64) (target : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_register_fd_target"]
opaque ffiRuntimeRegisterFdTargetImpl (runtime : UInt64) (fd : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_release_target"]
opaque ffiRuntimeReleaseTargetImpl (runtime : UInt64) (target : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_retain_target"]
opaque ffiRuntimeRetainTargetImpl (runtime : UInt64) (target : UInt32) : IO UInt32

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

@[extern "capnp_lean_rpc_cpp_call_one_shot"]
opaque ffiCppCallOneShotImpl
    (address : @& String) (portHint : UInt32) (interfaceId : UInt64) (methodId : UInt16)
    (request : @& ByteArray) (requestCaps : @& ByteArray) : IO (ByteArray × ByteArray)

@[extern "capnp_lean_rpc_runtime_cpp_call_with_accept"]
opaque ffiRuntimeCppCallWithAcceptImpl
    (runtime : UInt64) (server : UInt32) (listener : UInt32)
    (address : @& String) (portHint : UInt32) (interfaceId : UInt64) (methodId : UInt16)
    (request : @& ByteArray) (requestCaps : @& ByteArray) : IO (ByteArray × ByteArray)

@[extern "capnp_lean_rpc_cpp_serve_echo_once"]
opaque ffiCppServeEchoOnceImpl
    (address : @& String) (portHint : UInt32) (interfaceId : UInt64) (methodId : UInt16) :
    IO (ByteArray × ByteArray)

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

structure RuntimePendingCallRef where
  runtime : Runtime
  handle : RuntimePendingCall
  deriving Inhabited, BEq, Repr

namespace PipelinePath

@[inline] def toBytes (ops : Array UInt16) : ByteArray := Id.run do
  let mut out := ByteArray.emptyWithCapacity (ops.size * 2)
  for op in ops do
    let n := op.toNat
    out := out.push (UInt8.ofNat (n &&& 0xff))
    out := out.push (UInt8.ofNat ((n >>> 8) &&& 0xff))
  return out

end PipelinePath

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

@[inline] private def toRawHandlerCall
    (handler : Client -> Method -> Payload -> IO Payload) : RawHandlerCall :=
  fun target interfaceId methodId requestBytes requestCaps => do
    let request : Payload :=
      { msg := Capnp.readMessage requestBytes, capTable := CapTable.ofBytes requestCaps }
    let response ← handler target { interfaceId := interfaceId, methodId := methodId } request
    return (response.toBytes, CapTable.toBytes response.capTable)

namespace Runtime

@[inline] def init : IO Runtime := do
  return { handle := (← ffiRuntimeNewImpl) }

@[inline] def shutdown (runtime : Runtime) : IO Unit :=
  ffiRuntimeReleaseImpl runtime.handle

@[inline] def isAlive (runtime : Runtime) : IO Bool :=
  ffiRuntimeIsAliveImpl runtime.handle

@[inline] def registerEchoTarget (runtime : Runtime) : IO Client :=
  ffiRuntimeRegisterEchoTargetImpl runtime.handle

@[inline] def registerHandlerTarget (runtime : Runtime)
    (handler : Client -> Method -> Payload -> IO Payload) : IO Client :=
  ffiRuntimeRegisterHandlerTargetImpl runtime.handle (toRawHandlerCall handler)

@[inline] def registerTailCallTarget (runtime : Runtime) (target : Client) : IO Client :=
  ffiRuntimeRegisterTailCallTargetImpl runtime.handle target

@[inline] def registerFdTarget (runtime : Runtime) (fd : UInt32) : IO Client :=
  ffiRuntimeRegisterFdTargetImpl runtime.handle fd

@[inline] def releaseTarget (runtime : Runtime) (target : Client) : IO Unit :=
  ffiRuntimeReleaseTargetImpl runtime.handle target

@[inline] def retainTarget (runtime : Runtime) (target : Client) : IO Client :=
  ffiRuntimeRetainTargetImpl runtime.handle target

@[inline] def releaseCapTable (runtime : Runtime) (capTable : Capnp.CapTable) : IO Unit := do
  for cap in capTable.caps do
    runtime.releaseTarget cap

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

@[inline] def withClient (runtime : Runtime) (address : String)
    (action : RuntimeClientRef -> IO α) (portHint : UInt32 := 0) : IO α := do
  let client ← runtime.newClient address portHint
  try
    action client
  finally
    ffiRuntimeReleaseClientImpl runtime.handle client.handle

@[inline] def withServer (runtime : Runtime) (bootstrap : Client)
    (action : RuntimeServerRef -> IO α) : IO α := do
  let server ← runtime.newServer bootstrap
  try
    action server
  finally
    ffiRuntimeReleaseServerImpl runtime.handle server.handle

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

@[inline] def startCall (runtime : Runtime) (target : Client) (method : Method)
    (payload : Payload := Capnp.emptyRpcEnvelope) : IO RuntimePendingCallRef := do
  let requestBytes := payload.toBytes
  let requestCaps := CapTable.toBytes payload.capTable
  return {
    runtime := runtime
    handle := (← ffiRuntimeStartCallWithCapsImpl runtime.handle target method.interfaceId
      method.methodId requestBytes requestCaps)
  }

@[inline] def pendingCallAwait (pendingCall : RuntimePendingCallRef) : IO Payload := do
  let (responseBytes, responseCaps) ←
    ffiRuntimePendingCallAwaitImpl pendingCall.runtime.handle pendingCall.handle
  return { msg := Capnp.readMessage responseBytes, capTable := CapTable.ofBytes responseCaps }

@[inline] def pendingCallRelease (pendingCall : RuntimePendingCallRef) : IO Unit :=
  ffiRuntimePendingCallReleaseImpl pendingCall.runtime.handle pendingCall.handle

@[inline] def pendingCallGetPipelinedCap (pendingCall : RuntimePendingCallRef)
    (pointerPath : Array UInt16 := #[]) : IO Client := do
  ffiRuntimePendingCallGetPipelinedCapImpl pendingCall.runtime.handle pendingCall.handle
    (PipelinePath.toBytes pointerPath)

@[inline] def streamingCall (runtime : Runtime) (target : Client) (method : Method)
    (payload : Payload := Capnp.emptyRpcEnvelope) : IO Unit := do
  let requestBytes := payload.toBytes
  let requestCaps := CapTable.toBytes payload.capTable
  ffiRuntimeStreamingCallWithCapsImpl runtime.handle target method.interfaceId method.methodId
    requestBytes requestCaps

@[inline] def targetGetFd? (runtime : Runtime) (target : Client) : IO (Option UInt32) := do
  let noneSentinel := UInt32.ofNat 4294967295
  let fd ← ffiRuntimeTargetGetFdImpl runtime.handle target
  if fd == noneSentinel then
    return none
  else
    return some fd

@[inline] def targetWhenResolved (runtime : Runtime) (target : Client) : IO Unit :=
  ffiRuntimeTargetWhenResolvedImpl runtime.handle target

@[inline] def enableTraceEncoder (runtime : Runtime) : IO Unit :=
  ffiRuntimeEnableTraceEncoderImpl runtime.handle

@[inline] def disableTraceEncoder (runtime : Runtime) : IO Unit :=
  ffiRuntimeDisableTraceEncoderImpl runtime.handle

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

@[inline] def withListener (server : RuntimeServerRef) (address : String)
    (action : Listener -> IO α) (portHint : UInt32 := 0) : IO α := do
  let listener ← server.listen address portHint
  try
    action listener
  finally
    server.runtime.releaseListener listener

end RuntimeServerRef

namespace RuntimePendingCallRef

@[inline] def await (pendingCall : RuntimePendingCallRef) : IO Payload :=
  Runtime.pendingCallAwait pendingCall

@[inline] def release (pendingCall : RuntimePendingCallRef) : IO Unit :=
  Runtime.pendingCallRelease pendingCall

@[inline] def getPipelinedCap (pendingCall : RuntimePendingCallRef)
    (pointerPath : Array UInt16 := #[]) : IO Client :=
  Runtime.pendingCallGetPipelinedCap pendingCall pointerPath

end RuntimePendingCallRef

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

@[inline] def registerHandlerTarget
    (handler : Client -> Method -> Payload -> IO Payload) : RuntimeM Client := do
  Runtime.registerHandlerTarget (← runtime) handler

@[inline] def registerTailCallTarget (target : Client) : RuntimeM Client := do
  Runtime.registerTailCallTarget (← runtime) target

@[inline] def registerFdTarget (fd : UInt32) : RuntimeM Client := do
  Runtime.registerFdTarget (← runtime) fd

@[inline] def releaseTarget (target : Client) : RuntimeM Unit := do
  Runtime.releaseTarget (← runtime) target

@[inline] def retainTarget (target : Client) : RuntimeM Client := do
  Runtime.retainTarget (← runtime) target

@[inline] def releaseCapTable (capTable : Capnp.CapTable) : RuntimeM Unit := do
  Runtime.releaseCapTable (← runtime) capTable

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

@[inline] def withClient (address : String)
    (action : RuntimeClientRef -> RuntimeM α) (portHint : UInt32 := 0) : RuntimeM α := do
  let client ← newClient address portHint
  try
    action client
  finally
    client.release

@[inline] def withServer (bootstrap : Client)
    (action : RuntimeServerRef -> RuntimeM α) : RuntimeM α := do
  let server ← newServer bootstrap
  try
    action server
  finally
    server.release

@[inline] def withServerListener (server : RuntimeServerRef) (address : String)
    (action : Listener -> RuntimeM α) (portHint : UInt32 := 0) : RuntimeM α := do
  let listener ← serverListen server address portHint
  try
    action listener
  finally
    releaseListener listener

@[inline] def call (target : Client) (method : Method)
    (payload : Payload := Capnp.emptyRpcEnvelope) : RuntimeM Payload := do
  Capnp.Rpc.call (← backend) target method payload

@[inline] def startCall (target : Client) (method : Method)
    (payload : Payload := Capnp.emptyRpcEnvelope) : RuntimeM RuntimePendingCallRef := do
  Runtime.startCall (← runtime) target method payload

@[inline] def pendingCallAwait (pendingCall : RuntimePendingCallRef) : RuntimeM Payload := do
  Runtime.pendingCallAwait pendingCall

@[inline] def pendingCallRelease (pendingCall : RuntimePendingCallRef) : RuntimeM Unit := do
  Runtime.pendingCallRelease pendingCall

@[inline] def pendingCallGetPipelinedCap (pendingCall : RuntimePendingCallRef)
    (pointerPath : Array UInt16 := #[]) : RuntimeM Client := do
  Runtime.pendingCallGetPipelinedCap pendingCall pointerPath

@[inline] def streamingCall (target : Client) (method : Method)
    (payload : Payload := Capnp.emptyRpcEnvelope) : RuntimeM Unit := do
  Runtime.streamingCall (← runtime) target method payload

@[inline] def targetGetFd? (target : Client) : RuntimeM (Option UInt32) := do
  Runtime.targetGetFd? (← runtime) target

@[inline] def targetWhenResolved (target : Client) : RuntimeM Unit := do
  Runtime.targetWhenResolved (← runtime) target

@[inline] def enableTraceEncoder : RuntimeM Unit := do
  Runtime.enableTraceEncoder (← runtime)

@[inline] def disableTraceEncoder : RuntimeM Unit := do
  Runtime.disableTraceEncoder (← runtime)

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

namespace Runtime

@[inline] def registerBackendTarget (runtime : Runtime) (backend : Backend) : IO Client :=
  runtime.registerHandlerTarget (fun target method payload => backend.call target method payload)

@[inline] def registerDispatchTarget (runtime : Runtime) (dispatch : Dispatch)
    (onMissing : Client -> Method -> Payload -> IO Payload := fun _ _ _ => pure Capnp.emptyRpcEnvelope) :
    IO Client :=
  runtime.registerBackendTarget (dispatch.toBackend (onMissing := onMissing))

end Runtime

namespace RuntimeM

@[inline] def registerBackendTarget (backend : Backend) : RuntimeM Client := do
  Runtime.registerBackendTarget (← runtime) backend

@[inline] def registerDispatchTarget (dispatch : Dispatch)
    (onMissing : Client -> Method -> Payload -> IO Payload := fun _ _ _ => pure Capnp.emptyRpcEnvelope) :
    RuntimeM Client := do
  Runtime.registerDispatchTarget (← runtime) dispatch (onMissing := onMissing)

end RuntimeM

namespace Interop

@[inline] def cppCall (address : String) (method : Method)
    (payload : Payload := Capnp.emptyRpcEnvelope) (portHint : UInt32 := 0) : IO Payload := do
  let requestBytes := payload.toBytes
  let requestCaps := CapTable.toBytes payload.capTable
  let (responseBytes, responseCaps) ←
    ffiCppCallOneShotImpl address portHint method.interfaceId method.methodId requestBytes requestCaps
  return { msg := Capnp.readMessage responseBytes, capTable := CapTable.ofBytes responseCaps }

@[inline] def cppCallWithAccept (runtime : Runtime) (server : RuntimeServerRef) (listener : Listener)
    (address : String) (method : Method)
    (payload : Payload := Capnp.emptyRpcEnvelope) (portHint : UInt32 := 0) : IO Payload := do
  let requestBytes := payload.toBytes
  let requestCaps := CapTable.toBytes payload.capTable
  let (responseBytes, responseCaps) ← ffiRuntimeCppCallWithAcceptImpl runtime.handle server.handle
    listener address portHint method.interfaceId method.methodId requestBytes requestCaps
  return { msg := Capnp.readMessage responseBytes, capTable := CapTable.ofBytes responseCaps }

@[inline] def cppServeEchoOnce (address : String) (method : Method)
    (portHint : UInt32 := 0) : IO Payload := do
  let (requestBytes, requestCaps) ←
    ffiCppServeEchoOnceImpl address portHint method.interfaceId method.methodId
  return { msg := Capnp.readMessage requestBytes, capTable := CapTable.ofBytes requestCaps }

end Interop

def echoBackend : Backend where
  call := fun _ _ payload => pure payload

def emptyBackend : Backend where
  call := fun _ _ _ => pure Capnp.emptyRpcEnvelope

end Rpc
end Capnp
