import Capnp.Runtime

namespace Capnp
namespace Rpc

abbrev Payload := Capnp.RpcEnvelope

structure Method where
  interfaceId : UInt64
  methodId : UInt16
  deriving Inhabited, BEq, Repr

abbrev Client := Capnp.Capability

structure Listener where
  raw : UInt32
  deriving Inhabited, BEq, Repr

structure RuntimeClient where
  raw : UInt32
  deriving Inhabited, BEq, Repr

structure RuntimeServer where
  raw : UInt32
  deriving Inhabited, BEq, Repr

structure RuntimePendingCall where
  raw : UInt32
  deriving Inhabited, BEq, Repr

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
abbrev RawTailCallHandlerCall := Client -> UInt64 -> UInt16 -> ByteArray -> ByteArray ->
    IO UInt32
inductive RawAdvancedHandlerResult where
  | returnPayload (response : ByteArray) (responseCaps : ByteArray)
  | asyncCall (target : UInt32) (interfaceId : UInt64) (methodId : UInt16)
      (request : ByteArray) (requestCaps : ByteArray)
  | tailCall (target : UInt32) (interfaceId : UInt64) (methodId : UInt16)
      (request : ByteArray) (requestCaps : ByteArray)
  | throwRemote (message : String) (detail : ByteArray)

abbrev RawAdvancedHandlerCall := Client -> UInt64 -> UInt16 -> ByteArray -> ByteArray ->
    IO RawAdvancedHandlerResult
abbrev RawBootstrapFactoryCall := UInt16 -> IO UInt32
abbrev RawTraceEncoder := String -> IO String

inductive AdvancedHandlerResult where
  | respond (payload : Payload)
  | asyncCall (target : Client) (method : Method)
      (payload : Payload := Capnp.emptyRpcEnvelope)
  | tailCall (target : Client) (method : Method)
      (payload : Payload := Capnp.emptyRpcEnvelope)
  | throwRemote (message : String) (detail : ByteArray := ByteArray.empty)

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

@[extern "capnp_lean_rpc_runtime_register_promise_await"]
opaque ffiRuntimeRegisterPromiseAwaitImpl
    (runtime : UInt64) (promise : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_register_promise_cancel"]
opaque ffiRuntimeRegisterPromiseCancelImpl
    (runtime : UInt64) (promise : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_register_promise_release"]
opaque ffiRuntimeRegisterPromiseReleaseImpl
    (runtime : UInt64) (promise : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_unit_promise_await"]
opaque ffiRuntimeUnitPromiseAwaitImpl
    (runtime : UInt64) (promise : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_unit_promise_cancel"]
opaque ffiRuntimeUnitPromiseCancelImpl
    (runtime : UInt64) (promise : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_unit_promise_release"]
opaque ffiRuntimeUnitPromiseReleaseImpl
    (runtime : UInt64) (promise : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_streaming_call_with_caps"]
opaque ffiRuntimeStreamingCallWithCapsImpl
    (runtime : UInt64) (target : UInt32) (interfaceId : UInt64) (methodId : UInt16)
    (request : @& ByteArray) (requestCaps : @& ByteArray) : IO Unit

@[extern "capnp_lean_rpc_runtime_target_get_fd"]
opaque ffiRuntimeTargetGetFdImpl (runtime : UInt64) (target : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_target_when_resolved"]
opaque ffiRuntimeTargetWhenResolvedImpl (runtime : UInt64) (target : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_target_when_resolved_start"]
opaque ffiRuntimeTargetWhenResolvedStartImpl (runtime : UInt64) (target : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_enable_trace_encoder"]
opaque ffiRuntimeEnableTraceEncoderImpl (runtime : UInt64) : IO Unit

@[extern "capnp_lean_rpc_runtime_disable_trace_encoder"]
opaque ffiRuntimeDisableTraceEncoderImpl (runtime : UInt64) : IO Unit

@[extern "capnp_lean_rpc_runtime_set_trace_encoder"]
opaque ffiRuntimeSetTraceEncoderImpl (runtime : UInt64) (encoder : @& RawTraceEncoder) : IO Unit

@[extern "capnp_lean_rpc_runtime_new"]
opaque ffiRuntimeNewImpl : IO UInt64

@[extern "capnp_lean_rpc_runtime_new_with_fd_limit"]
opaque ffiRuntimeNewWithFdLimitImpl (maxFdsPerMessage : UInt32) : IO UInt64

@[extern "capnp_lean_rpc_runtime_release"]
opaque ffiRuntimeReleaseImpl (runtime : UInt64) : IO Unit

@[extern "capnp_lean_rpc_runtime_is_alive"]
opaque ffiRuntimeIsAliveImpl (runtime : UInt64) : IO Bool

@[extern "capnp_lean_rpc_runtime_register_echo_target"]
opaque ffiRuntimeRegisterEchoTargetImpl (runtime : UInt64) : IO UInt32

@[extern "capnp_lean_rpc_runtime_register_handler_target"]
opaque ffiRuntimeRegisterHandlerTargetImpl (runtime : UInt64) (handler : @& RawHandlerCall) :
    IO UInt32

@[extern "capnp_lean_rpc_runtime_register_advanced_handler_target"]
opaque ffiRuntimeRegisterAdvancedHandlerTargetImpl
    (runtime : UInt64) (handler : @& RawAdvancedHandlerCall) : IO UInt32

@[extern "capnp_lean_rpc_runtime_register_tailcall_handler_target"]
opaque ffiRuntimeRegisterTailCallHandlerTargetImpl
    (runtime : UInt64) (handler : @& RawTailCallHandlerCall) : IO UInt32

@[extern "capnp_lean_rpc_runtime_register_loopback_target"]
opaque ffiRuntimeRegisterLoopbackTargetImpl (runtime : UInt64) (bootstrap : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_register_tailcall_target"]
opaque ffiRuntimeRegisterTailCallTargetImpl (runtime : UInt64) (target : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_register_fd_target"]
opaque ffiRuntimeRegisterFdTargetImpl (runtime : UInt64) (fd : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_release_target"]
opaque ffiRuntimeReleaseTargetImpl (runtime : UInt64) (target : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_release_targets"]
opaque ffiRuntimeReleaseTargetsImpl (runtime : UInt64) (targets : @& ByteArray) : IO Unit

@[extern "capnp_lean_rpc_runtime_retain_target"]
opaque ffiRuntimeRetainTargetImpl (runtime : UInt64) (target : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_connect"]
opaque ffiRuntimeConnectImpl (runtime : UInt64) (address : @& String) (portHint : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_connect_start"]
opaque ffiRuntimeConnectStartImpl
    (runtime : UInt64) (address : @& String) (portHint : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_connect_fd"]
opaque ffiRuntimeConnectFdImpl (runtime : UInt64) (fd : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_listen_echo"]
opaque ffiRuntimeListenEchoImpl (runtime : UInt64) (address : @& String) (portHint : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_accept_echo"]
opaque ffiRuntimeAcceptEchoImpl (runtime : UInt64) (listener : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_release_listener"]
opaque ffiRuntimeReleaseListenerImpl (runtime : UInt64) (listener : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_new_client"]
opaque ffiRuntimeNewClientImpl
    (runtime : UInt64) (address : @& String) (portHint : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_new_client_start"]
opaque ffiRuntimeNewClientStartImpl
    (runtime : UInt64) (address : @& String) (portHint : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_release_client"]
opaque ffiRuntimeReleaseClientImpl (runtime : UInt64) (client : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_client_bootstrap"]
opaque ffiRuntimeClientBootstrapImpl (runtime : UInt64) (client : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_client_on_disconnect"]
opaque ffiRuntimeClientOnDisconnectImpl (runtime : UInt64) (client : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_client_on_disconnect_start"]
opaque ffiRuntimeClientOnDisconnectStartImpl (runtime : UInt64) (client : UInt32) : IO UInt32

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

@[extern "capnp_lean_rpc_runtime_new_server_with_bootstrap_factory"]
opaque ffiRuntimeNewServerWithBootstrapFactoryImpl
    (runtime : UInt64) (bootstrapFactory : @& RawBootstrapFactoryCall) : IO UInt32

@[extern "capnp_lean_rpc_runtime_release_server"]
opaque ffiRuntimeReleaseServerImpl (runtime : UInt64) (server : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_server_listen"]
opaque ffiRuntimeServerListenImpl
    (runtime : UInt64) (server : UInt32) (address : @& String) (portHint : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_server_accept"]
opaque ffiRuntimeServerAcceptImpl (runtime : UInt64) (server : UInt32) (listener : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_server_accept_start"]
opaque ffiRuntimeServerAcceptStartImpl
    (runtime : UInt64) (server : UInt32) (listener : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_runtime_server_accept_fd"]
opaque ffiRuntimeServerAcceptFdImpl
    (runtime : UInt64) (server : UInt32) (fd : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_server_drain"]
opaque ffiRuntimeServerDrainImpl (runtime : UInt64) (server : UInt32) : IO Unit

@[extern "capnp_lean_rpc_runtime_server_drain_start"]
opaque ffiRuntimeServerDrainStartImpl (runtime : UInt64) (server : UInt32) : IO UInt32

@[extern "capnp_lean_rpc_cpp_call_one_shot"]
opaque ffiCppCallOneShotImpl
    (address : @& String) (portHint : UInt32) (interfaceId : UInt64) (methodId : UInt16)
    (request : @& ByteArray) (requestCaps : @& ByteArray) : IO (ByteArray × ByteArray)

@[extern "capnp_lean_rpc_runtime_cpp_call_with_accept"]
opaque ffiRuntimeCppCallWithAcceptImpl
    (runtime : UInt64) (server : UInt32) (listener : UInt32)
    (address : @& String) (portHint : UInt32) (interfaceId : UInt64) (methodId : UInt16)
    (request : @& ByteArray) (requestCaps : @& ByteArray) : IO (ByteArray × ByteArray)

@[extern "capnp_lean_rpc_runtime_cpp_call_pipelined_with_accept"]
opaque ffiRuntimeCppCallPipelinedWithAcceptImpl
    (runtime : UInt64) (server : UInt32) (listener : UInt32)
    (address : @& String) (portHint : UInt32) (interfaceId : UInt64) (methodId : UInt16)
    (request : @& ByteArray) (requestCaps : @& ByteArray)
    (pipelinedRequest : @& ByteArray) (pipelinedRequestCaps : @& ByteArray) :
    IO (ByteArray × ByteArray)

@[extern "capnp_lean_rpc_cpp_serve_echo_once"]
opaque ffiCppServeEchoOnceImpl
    (address : @& String) (portHint : UInt32) (interfaceId : UInt64) (methodId : UInt16) :
    IO (ByteArray × ByteArray)

@[extern "capnp_lean_rpc_cpp_serve_throw_once"]
opaque ffiCppServeThrowOnceImpl
    (address : @& String) (portHint : UInt32) (interfaceId : UInt64) (methodId : UInt16)
    (withDetail : UInt8) : IO (ByteArray × ByteArray)

@[extern "capnp_lean_rpc_cpp_serve_delayed_echo_once"]
opaque ffiCppServeDelayedEchoOnceImpl
    (address : @& String) (portHint : UInt32) (interfaceId : UInt64) (methodId : UInt16)
    (delayMillis : UInt32) : IO (ByteArray × ByteArray)

@[extern "capnp_lean_rpc_cpp_call_pipelined_cap_one_shot"]
opaque ffiCppCallPipelinedCapOneShotImpl
    (address : @& String) (portHint : UInt32) (interfaceId : UInt64) (methodId : UInt16)
    (request : @& ByteArray) (requestCaps : @& ByteArray)
    (pipelinedRequest : @& ByteArray) (pipelinedRequestCaps : @& ByteArray) :
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

structure RuntimeRegisterPromiseRef where
  runtime : Runtime
  handle : UInt32
  deriving Inhabited, BEq, Repr

structure RuntimeUnitPromiseRef where
  runtime : Runtime
  handle : UInt32
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

@[inline] def ofBytesChecked (bytes : ByteArray) : Except String Capnp.CapTable :=
  if bytes.size % 4 != 0 then
    Except.error "capability table payload must be a multiple of 4 bytes"
  else
    Except.ok (CapTable.ofBytes bytes)

end CapTable

@[inline] private def expectChecked (what : String) (value : Except String α) : IO α :=
  match value with
  | Except.ok v => pure v
  | Except.error e => throw (IO.userError s!"{what}: {e}")

@[inline] private def decodePayloadChecked (msgBytes capBytes : ByteArray) : IO Payload := do
  let opts : Capnp.ReaderOptions := {}
  let msg ← expectChecked "invalid RPC message" (Capnp.readMessageChecked opts msgBytes)
  let capTable ← expectChecked "invalid RPC capability table" (CapTable.ofBytesChecked capBytes)
  return { msg := msg, capTable := capTable }

@[inline] private def toRawHandlerCall
    (handler : Client -> Method -> Payload -> IO Payload) : RawHandlerCall :=
  fun target interfaceId methodId requestBytes requestCaps => do
    let request ← decodePayloadChecked requestBytes requestCaps
    let response ← handler target { interfaceId := interfaceId, methodId := methodId } request
    return (response.toBytes, CapTable.toBytes response.capTable)

@[inline] private def toRawTailCallHandlerCall
    (handler : Client -> Method -> Payload -> IO Client) : RawTailCallHandlerCall :=
  fun target interfaceId methodId requestBytes requestCaps => do
    let request ← decodePayloadChecked requestBytes requestCaps
    handler target { interfaceId := interfaceId, methodId := methodId } request

@[inline] private def toRawAdvancedHandlerCall
    (handler : Client -> Method -> Payload -> IO AdvancedHandlerResult) : RawAdvancedHandlerCall :=
  fun target interfaceId methodId requestBytes requestCaps => do
    let request ← decodePayloadChecked requestBytes requestCaps
    let method : Method := { interfaceId := interfaceId, methodId := methodId }
    match (← handler target method request) with
    | .respond response =>
        pure (.returnPayload response.toBytes (CapTable.toBytes response.capTable))
    | .asyncCall nextTarget nextMethod nextPayload =>
        pure (.asyncCall nextTarget nextMethod.interfaceId nextMethod.methodId
          nextPayload.toBytes (CapTable.toBytes nextPayload.capTable))
    | .tailCall nextTarget nextMethod nextPayload =>
        pure (.tailCall nextTarget nextMethod.interfaceId nextMethod.methodId
          nextPayload.toBytes (CapTable.toBytes nextPayload.capTable))
    | .throwRemote message detail =>
        pure (.throwRemote message detail)

namespace Runtime

@[inline] def init : IO Runtime := do
  return { handle := (← ffiRuntimeNewImpl) }

@[inline] def initWithFdLimit (maxFdsPerMessage : UInt32) : IO Runtime := do
  return { handle := (← ffiRuntimeNewWithFdLimitImpl maxFdsPerMessage) }

@[inline] def shutdown (runtime : Runtime) : IO Unit :=
  ffiRuntimeReleaseImpl runtime.handle

@[inline] def isAlive (runtime : Runtime) : IO Bool :=
  ffiRuntimeIsAliveImpl runtime.handle

@[inline] def registerEchoTarget (runtime : Runtime) : IO Client :=
  ffiRuntimeRegisterEchoTargetImpl runtime.handle

@[inline] def registerLoopbackTarget (runtime : Runtime) (bootstrap : Client) : IO Client :=
  ffiRuntimeRegisterLoopbackTargetImpl runtime.handle bootstrap

@[inline] def registerHandlerTarget (runtime : Runtime)
    (handler : Client -> Method -> Payload -> IO Payload) : IO Client :=
  ffiRuntimeRegisterHandlerTargetImpl runtime.handle (toRawHandlerCall handler)

@[inline] def registerAdvancedHandlerTarget (runtime : Runtime)
    (handler : Client -> Method -> Payload -> IO AdvancedHandlerResult) : IO Client :=
  ffiRuntimeRegisterAdvancedHandlerTargetImpl runtime.handle (toRawAdvancedHandlerCall handler)

@[inline] def registerTailCallHandlerTarget (runtime : Runtime)
    (handler : Client -> Method -> Payload -> IO Client) : IO Client :=
  ffiRuntimeRegisterTailCallHandlerTargetImpl runtime.handle (toRawTailCallHandlerCall handler)

@[inline] def registerTailCallTarget (runtime : Runtime) (target : Client) : IO Client :=
  ffiRuntimeRegisterTailCallTargetImpl runtime.handle target

@[inline] def registerFdTarget (runtime : Runtime) (fd : UInt32) : IO Client :=
  ffiRuntimeRegisterFdTargetImpl runtime.handle fd

@[inline] def releaseTarget (runtime : Runtime) (target : Client) : IO Unit :=
  ffiRuntimeReleaseTargetImpl runtime.handle target

@[inline] def retainTarget (runtime : Runtime) (target : Client) : IO Client :=
  ffiRuntimeRetainTargetImpl runtime.handle target

@[inline] def releaseCapTable (runtime : Runtime) (capTable : Capnp.CapTable) : IO Unit := do
  if capTable.caps.isEmpty then
    pure ()
  else
    ffiRuntimeReleaseTargetsImpl runtime.handle (CapTable.toBytes capTable)

@[inline] def connect (runtime : Runtime) (address : String) (portHint : UInt32 := 0) : IO Client :=
  ffiRuntimeConnectImpl runtime.handle address portHint

@[inline] def connectStart (runtime : Runtime) (address : String) (portHint : UInt32 := 0) :
    IO RuntimeRegisterPromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeConnectStartImpl runtime.handle address portHint)
  }

@[inline] def connectFd (runtime : Runtime) (fd : UInt32) : IO Client :=
  ffiRuntimeConnectFdImpl runtime.handle fd

@[inline] def listenEcho (runtime : Runtime) (address : String) (portHint : UInt32 := 0) :
    IO Listener :=
  return { raw := (← ffiRuntimeListenEchoImpl runtime.handle address portHint) }

@[inline] def acceptEcho (runtime : Runtime) (listener : Listener) : IO Unit :=
  ffiRuntimeAcceptEchoImpl runtime.handle listener.raw

@[inline] def releaseListener (runtime : Runtime) (listener : Listener) : IO Unit :=
  ffiRuntimeReleaseListenerImpl runtime.handle listener.raw

@[inline] def newClient (runtime : Runtime) (address : String) (portHint : UInt32 := 0) :
    IO RuntimeClientRef := do
  return {
    runtime := runtime
    handle := { raw := (← ffiRuntimeNewClientImpl runtime.handle address portHint) }
  }

@[inline] def newClientStart (runtime : Runtime) (address : String) (portHint : UInt32 := 0) :
    IO RuntimeRegisterPromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeNewClientStartImpl runtime.handle address portHint)
  }

@[inline] def newServer (runtime : Runtime) (bootstrap : Client) : IO RuntimeServerRef := do
  return { runtime := runtime, handle := { raw := (← ffiRuntimeNewServerImpl runtime.handle bootstrap) } }

@[inline] def newServerWithBootstrapFactory (runtime : Runtime)
    (bootstrapFactory : UInt16 -> IO Client) : IO RuntimeServerRef := do
  return {
    runtime := runtime
    handle := {
      raw := (← ffiRuntimeNewServerWithBootstrapFactoryImpl runtime.handle bootstrapFactory)
    }
  }

@[inline] def withClient (runtime : Runtime) (address : String)
    (action : RuntimeClientRef -> IO α) (portHint : UInt32 := 0) : IO α := do
  let client ← runtime.newClient address portHint
  try
    action client
  finally
    ffiRuntimeReleaseClientImpl runtime.handle client.handle.raw

@[inline] def withServer (runtime : Runtime) (bootstrap : Client)
    (action : RuntimeServerRef -> IO α) : IO α := do
  let server ← runtime.newServer bootstrap
  try
    action server
  finally
    ffiRuntimeReleaseServerImpl runtime.handle server.handle.raw

@[inline] def withServerWithBootstrapFactory (runtime : Runtime)
    (bootstrapFactory : UInt16 -> IO Client)
    (action : RuntimeServerRef -> IO α) : IO α := do
  let server ← runtime.newServerWithBootstrapFactory bootstrapFactory
  try
    action server
  finally
    ffiRuntimeReleaseServerImpl runtime.handle server.handle.raw

@[inline] def rawCall (runtime : Runtime) : RawCall :=
  fun target method request =>
    ffiRawCallOnRuntimeImpl runtime.handle target method.interfaceId method.methodId request

@[inline] def backend (runtime : Runtime) : Backend where
  call := fun target method payload => do
    let requestBytes := payload.toBytes
    let requestCaps := CapTable.toBytes payload.capTable
    let (responseBytes, responseCaps) ← ffiRawCallWithCapsOnRuntimeImpl
      runtime.handle target method.interfaceId method.methodId requestBytes requestCaps
    decodePayloadChecked responseBytes responseCaps

@[inline] def startCall (runtime : Runtime) (target : Client) (method : Method)
    (payload : Payload := Capnp.emptyRpcEnvelope) : IO RuntimePendingCallRef := do
  let requestBytes := payload.toBytes
  let requestCaps := CapTable.toBytes payload.capTable
  return {
    runtime := runtime
    handle := {
      raw := (← ffiRuntimeStartCallWithCapsImpl runtime.handle target method.interfaceId
        method.methodId requestBytes requestCaps)
    }
  }

@[inline] def pendingCallAwait (pendingCall : RuntimePendingCallRef) : IO Payload := do
  let (responseBytes, responseCaps) ←
    ffiRuntimePendingCallAwaitImpl pendingCall.runtime.handle pendingCall.handle.raw
  decodePayloadChecked responseBytes responseCaps

@[inline] def pendingCallRelease (pendingCall : RuntimePendingCallRef) : IO Unit :=
  ffiRuntimePendingCallReleaseImpl pendingCall.runtime.handle pendingCall.handle.raw

@[inline] def pendingCallGetPipelinedCap (pendingCall : RuntimePendingCallRef)
    (pointerPath : Array UInt16 := #[]) : IO Client := do
  ffiRuntimePendingCallGetPipelinedCapImpl pendingCall.runtime.handle pendingCall.handle.raw
    (PipelinePath.toBytes pointerPath)

@[inline] def registerPromiseAwait (promise : RuntimeRegisterPromiseRef) : IO UInt32 :=
  ffiRuntimeRegisterPromiseAwaitImpl promise.runtime.handle promise.handle

@[inline] def registerPromiseCancel (promise : RuntimeRegisterPromiseRef) : IO Unit :=
  ffiRuntimeRegisterPromiseCancelImpl promise.runtime.handle promise.handle

@[inline] def registerPromiseRelease (promise : RuntimeRegisterPromiseRef) : IO Unit :=
  ffiRuntimeRegisterPromiseReleaseImpl promise.runtime.handle promise.handle

@[inline] def unitPromiseAwait (promise : RuntimeUnitPromiseRef) : IO Unit :=
  ffiRuntimeUnitPromiseAwaitImpl promise.runtime.handle promise.handle

@[inline] def unitPromiseCancel (promise : RuntimeUnitPromiseRef) : IO Unit :=
  ffiRuntimeUnitPromiseCancelImpl promise.runtime.handle promise.handle

@[inline] def unitPromiseRelease (promise : RuntimeUnitPromiseRef) : IO Unit :=
  ffiRuntimeUnitPromiseReleaseImpl promise.runtime.handle promise.handle

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

@[inline] def targetWhenResolvedStart (runtime : Runtime) (target : Client) :
    IO RuntimeUnitPromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeTargetWhenResolvedStartImpl runtime.handle target)
  }

@[inline] def enableTraceEncoder (runtime : Runtime) : IO Unit :=
  ffiRuntimeEnableTraceEncoderImpl runtime.handle

@[inline] def disableTraceEncoder (runtime : Runtime) : IO Unit :=
  ffiRuntimeDisableTraceEncoderImpl runtime.handle

@[inline] def setTraceEncoder (runtime : Runtime) (encoder : RawTraceEncoder) : IO Unit :=
  ffiRuntimeSetTraceEncoderImpl runtime.handle encoder

def withRuntime (action : Runtime -> IO α) : IO α := do
  let runtime ← init
  try
    action runtime
  finally
    runtime.shutdown

def withRuntimeWithFdLimit (maxFdsPerMessage : UInt32) (action : Runtime -> IO α) : IO α := do
  let runtime ← initWithFdLimit maxFdsPerMessage
  try
    action runtime
  finally
    runtime.shutdown

end Runtime

namespace RuntimeClientRef

@[inline] def release (client : RuntimeClientRef) : IO Unit :=
  ffiRuntimeReleaseClientImpl client.runtime.handle client.handle.raw

@[inline] def bootstrap (client : RuntimeClientRef) : IO Client :=
  ffiRuntimeClientBootstrapImpl client.runtime.handle client.handle.raw

@[inline] def onDisconnect (client : RuntimeClientRef) : IO Unit :=
  ffiRuntimeClientOnDisconnectImpl client.runtime.handle client.handle.raw

@[inline] def onDisconnectStart (client : RuntimeClientRef) : IO RuntimeUnitPromiseRef := do
  return {
    runtime := client.runtime
    handle := (← ffiRuntimeClientOnDisconnectStartImpl client.runtime.handle client.handle.raw)
  }

@[inline] def setFlowLimit (client : RuntimeClientRef) (words : UInt64) : IO Unit :=
  ffiRuntimeClientSetFlowLimitImpl client.runtime.handle client.handle.raw words

@[inline] def queueSize (client : RuntimeClientRef) : IO UInt64 :=
  ffiRuntimeClientQueueSizeImpl client.runtime.handle client.handle.raw

@[inline] def queueCount (client : RuntimeClientRef) : IO UInt64 :=
  ffiRuntimeClientQueueCountImpl client.runtime.handle client.handle.raw

@[inline] def outgoingWaitNanos (client : RuntimeClientRef) : IO UInt64 :=
  ffiRuntimeClientOutgoingWaitNanosImpl client.runtime.handle client.handle.raw

end RuntimeClientRef

namespace RuntimeServerRef

@[inline] def release (server : RuntimeServerRef) : IO Unit :=
  ffiRuntimeReleaseServerImpl server.runtime.handle server.handle.raw

@[inline] def listen (server : RuntimeServerRef) (address : String) (portHint : UInt32 := 0) :
    IO Listener :=
  return {
    raw := (← ffiRuntimeServerListenImpl server.runtime.handle server.handle.raw address portHint)
  }

@[inline] def accept (server : RuntimeServerRef) (listener : Listener) : IO Unit :=
  ffiRuntimeServerAcceptImpl server.runtime.handle server.handle.raw listener.raw

@[inline] def acceptStart (server : RuntimeServerRef) (listener : Listener) :
    IO RuntimeUnitPromiseRef := do
  return {
    runtime := server.runtime
    handle := (← ffiRuntimeServerAcceptStartImpl
      server.runtime.handle server.handle.raw listener.raw)
  }

@[inline] def acceptFd (server : RuntimeServerRef) (fd : UInt32) : IO Unit :=
  ffiRuntimeServerAcceptFdImpl server.runtime.handle server.handle.raw fd

@[inline] def drain (server : RuntimeServerRef) : IO Unit :=
  ffiRuntimeServerDrainImpl server.runtime.handle server.handle.raw

@[inline] def drainStart (server : RuntimeServerRef) : IO RuntimeUnitPromiseRef := do
  return {
    runtime := server.runtime
    handle := (← ffiRuntimeServerDrainStartImpl server.runtime.handle server.handle.raw)
  }

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

namespace RuntimeRegisterPromiseRef

@[inline] def await (promise : RuntimeRegisterPromiseRef) : IO UInt32 :=
  Runtime.registerPromiseAwait promise

@[inline] def cancel (promise : RuntimeRegisterPromiseRef) : IO Unit :=
  Runtime.registerPromiseCancel promise

@[inline] def release (promise : RuntimeRegisterPromiseRef) : IO Unit :=
  Runtime.registerPromiseRelease promise

@[inline] def awaitAndRelease (promise : RuntimeRegisterPromiseRef) : IO UInt32 := do
  let value ← promise.await
  promise.release
  return value

@[inline] def awaitTarget (promise : RuntimeRegisterPromiseRef) : IO Client :=
  promise.await

@[inline] def awaitClient (promise : RuntimeRegisterPromiseRef) : IO RuntimeClientRef := do
  return {
    runtime := promise.runtime
    handle := { raw := (← promise.await) }
  }

@[inline] def awaitListener (promise : RuntimeRegisterPromiseRef) : IO Listener := do
  return { raw := (← promise.await) }

@[inline] def awaitServer (promise : RuntimeRegisterPromiseRef) : IO RuntimeServerRef := do
  return {
    runtime := promise.runtime
    handle := { raw := (← promise.await) }
  }

end RuntimeRegisterPromiseRef

namespace RuntimeUnitPromiseRef

@[inline] def await (promise : RuntimeUnitPromiseRef) : IO Unit :=
  Runtime.unitPromiseAwait promise

@[inline] def cancel (promise : RuntimeUnitPromiseRef) : IO Unit :=
  Runtime.unitPromiseCancel promise

@[inline] def release (promise : RuntimeUnitPromiseRef) : IO Unit :=
  Runtime.unitPromiseRelease promise

@[inline] def awaitAndRelease (promise : RuntimeUnitPromiseRef) : IO Unit := do
  promise.await
  promise.release

end RuntimeUnitPromiseRef

abbrev RuntimeM := ReaderT Runtime IO

namespace RuntimeM

@[inline] def run (runtime : Runtime) (action : RuntimeM α) : IO α :=
  action runtime

@[inline] def runWithNewRuntime (action : RuntimeM α) : IO α :=
  Runtime.withRuntime fun runtime => action runtime

@[inline] def runWithNewRuntimeWithFdLimit
    (maxFdsPerMessage : UInt32) (action : RuntimeM α) : IO α :=
  Runtime.withRuntimeWithFdLimit maxFdsPerMessage fun runtime => action runtime

@[inline] def runtime : RuntimeM Runtime := read

@[inline] def backend : RuntimeM Backend := do
  return Runtime.backend (← runtime)

@[inline] def isAlive : RuntimeM Bool := do
  Runtime.isAlive (← runtime)

@[inline] def registerEchoTarget : RuntimeM Client := do
  Runtime.registerEchoTarget (← runtime)

@[inline] def registerLoopbackTarget (bootstrap : Client) : RuntimeM Client := do
  Runtime.registerLoopbackTarget (← runtime) bootstrap

@[inline] def registerHandlerTarget
    (handler : Client -> Method -> Payload -> IO Payload) : RuntimeM Client := do
  Runtime.registerHandlerTarget (← runtime) handler

@[inline] def registerAdvancedHandlerTarget
    (handler : Client -> Method -> Payload -> IO AdvancedHandlerResult) : RuntimeM Client := do
  Runtime.registerAdvancedHandlerTarget (← runtime) handler

@[inline] def registerTailCallHandlerTarget
    (handler : Client -> Method -> Payload -> IO Client) : RuntimeM Client := do
  Runtime.registerTailCallHandlerTarget (← runtime) handler

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

@[inline] def connectStart (address : String) (portHint : UInt32 := 0) :
    RuntimeM RuntimeRegisterPromiseRef := do
  Runtime.connectStart (← runtime) address portHint

@[inline] def connectFd (fd : UInt32) : RuntimeM Client := do
  Runtime.connectFd (← runtime) fd

@[inline] def listenEcho (address : String) (portHint : UInt32 := 0) : RuntimeM Listener := do
  Runtime.listenEcho (← runtime) address portHint

@[inline] def acceptEcho (listener : Listener) : RuntimeM Unit := do
  Runtime.acceptEcho (← runtime) listener

@[inline] def releaseListener (listener : Listener) : RuntimeM Unit := do
  Runtime.releaseListener (← runtime) listener

@[inline] def newClient (address : String) (portHint : UInt32 := 0) : RuntimeM RuntimeClientRef := do
  Runtime.newClient (← runtime) address portHint

@[inline] def newClientStart (address : String) (portHint : UInt32 := 0) :
    RuntimeM RuntimeRegisterPromiseRef := do
  Runtime.newClientStart (← runtime) address portHint

@[inline] def newServer (bootstrap : Client) : RuntimeM RuntimeServerRef := do
  Runtime.newServer (← runtime) bootstrap

@[inline] def newServerWithBootstrapFactory
    (bootstrapFactory : UInt16 -> IO Client) : RuntimeM RuntimeServerRef := do
  Runtime.newServerWithBootstrapFactory (← runtime) bootstrapFactory

@[inline] def clientRelease (client : RuntimeClientRef) : RuntimeM Unit := do
  client.release

@[inline] def clientBootstrap (client : RuntimeClientRef) : RuntimeM Client := do
  client.bootstrap

@[inline] def clientOnDisconnect (client : RuntimeClientRef) : RuntimeM Unit := do
  client.onDisconnect

@[inline] def clientOnDisconnectStart (client : RuntimeClientRef) :
    RuntimeM RuntimeUnitPromiseRef := do
  client.onDisconnectStart

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

@[inline] def serverAcceptStart (server : RuntimeServerRef) (listener : Listener) :
    RuntimeM RuntimeUnitPromiseRef := do
  server.acceptStart listener

@[inline] def serverAcceptFd (server : RuntimeServerRef) (fd : UInt32) : RuntimeM Unit := do
  server.acceptFd fd

@[inline] def serverDrain (server : RuntimeServerRef) : RuntimeM Unit := do
  server.drain

@[inline] def serverDrainStart (server : RuntimeServerRef) : RuntimeM RuntimeUnitPromiseRef := do
  server.drainStart

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

@[inline] def withServerWithBootstrapFactory
    (bootstrapFactory : UInt16 -> IO Client)
    (action : RuntimeServerRef -> RuntimeM α) : RuntimeM α := do
  let server ← newServerWithBootstrapFactory bootstrapFactory
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

@[inline] def registerPromiseAwait (promise : RuntimeRegisterPromiseRef) : RuntimeM UInt32 := do
  promise.await

@[inline] def registerPromiseCancel (promise : RuntimeRegisterPromiseRef) : RuntimeM Unit := do
  promise.cancel

@[inline] def registerPromiseRelease (promise : RuntimeRegisterPromiseRef) : RuntimeM Unit := do
  promise.release

@[inline] def unitPromiseAwait (promise : RuntimeUnitPromiseRef) : RuntimeM Unit := do
  promise.await

@[inline] def unitPromiseCancel (promise : RuntimeUnitPromiseRef) : RuntimeM Unit := do
  promise.cancel

@[inline] def unitPromiseRelease (promise : RuntimeUnitPromiseRef) : RuntimeM Unit := do
  promise.release

@[inline] def streamingCall (target : Client) (method : Method)
    (payload : Payload := Capnp.emptyRpcEnvelope) : RuntimeM Unit := do
  Runtime.streamingCall (← runtime) target method payload

@[inline] def targetGetFd? (target : Client) : RuntimeM (Option UInt32) := do
  Runtime.targetGetFd? (← runtime) target

@[inline] def targetWhenResolved (target : Client) : RuntimeM Unit := do
  Runtime.targetWhenResolved (← runtime) target

@[inline] def targetWhenResolvedStart (target : Client) : RuntimeM RuntimeUnitPromiseRef := do
  Runtime.targetWhenResolvedStart (← runtime) target

@[inline] def enableTraceEncoder : RuntimeM Unit := do
  Runtime.enableTraceEncoder (← runtime)

@[inline] def disableTraceEncoder : RuntimeM Unit := do
  Runtime.disableTraceEncoder (← runtime)

@[inline] def setTraceEncoder (encoder : RawTraceEncoder) : RuntimeM Unit := do
  Runtime.setTraceEncoder (← runtime) encoder

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
  for route in d.routes do
    if route.method == method then
      return some route.handler
  return none

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
  decodePayloadChecked responseBytes responseCaps

@[inline] def cppCallWithAccept (runtime : Runtime) (server : RuntimeServerRef) (listener : Listener)
    (address : String) (method : Method)
    (payload : Payload := Capnp.emptyRpcEnvelope) (portHint : UInt32 := 0) : IO Payload := do
  let requestBytes := payload.toBytes
  let requestCaps := CapTable.toBytes payload.capTable
  let (responseBytes, responseCaps) ←
    ffiRuntimeCppCallWithAcceptImpl runtime.handle server.handle.raw listener.raw
      address portHint method.interfaceId method.methodId requestBytes requestCaps
  decodePayloadChecked responseBytes responseCaps

@[inline] def cppCallPipelinedWithAccept (runtime : Runtime) (server : RuntimeServerRef)
    (listener : Listener) (address : String) (method : Method)
    (request : Payload := Capnp.emptyRpcEnvelope)
    (pipelinedRequest : Payload := Capnp.emptyRpcEnvelope) (portHint : UInt32 := 0) :
    IO Payload := do
  let requestBytes := request.toBytes
  let requestCaps := CapTable.toBytes request.capTable
  let pipelinedRequestBytes := pipelinedRequest.toBytes
  let pipelinedRequestCaps := CapTable.toBytes pipelinedRequest.capTable
  let (responseBytes, responseCaps) ← ffiRuntimeCppCallPipelinedWithAcceptImpl
    runtime.handle server.handle.raw listener.raw address portHint method.interfaceId method.methodId
    requestBytes requestCaps pipelinedRequestBytes pipelinedRequestCaps
  decodePayloadChecked responseBytes responseCaps

@[inline] def cppServeEchoOnce (address : String) (method : Method)
    (portHint : UInt32 := 0) : IO Payload := do
  let (requestBytes, requestCaps) ←
    ffiCppServeEchoOnceImpl address portHint method.interfaceId method.methodId
  decodePayloadChecked requestBytes requestCaps

@[inline] def cppServeThrowOnce (address : String) (method : Method)
    (withDetail : Bool := false) (portHint : UInt32 := 0) : IO Payload := do
  let detailFlag : UInt8 := if withDetail then 1 else 0
  let (requestBytes, requestCaps) ←
    ffiCppServeThrowOnceImpl address portHint method.interfaceId method.methodId detailFlag
  decodePayloadChecked requestBytes requestCaps

@[inline] def cppServeDelayedEchoOnce (address : String) (method : Method)
    (delayMillis : UInt32) (portHint : UInt32 := 0) : IO Payload := do
  let (requestBytes, requestCaps) ← ffiCppServeDelayedEchoOnceImpl
    address portHint method.interfaceId method.methodId delayMillis
  decodePayloadChecked requestBytes requestCaps

@[inline] def cppCallPipelinedCapOneShot (address : String) (method : Method)
    (request : Payload := Capnp.emptyRpcEnvelope)
    (pipelinedRequest : Payload := Capnp.emptyRpcEnvelope)
    (portHint : UInt32 := 0) : IO Payload := do
  let requestBytes := request.toBytes
  let requestCaps := CapTable.toBytes request.capTable
  let pipelinedRequestBytes := pipelinedRequest.toBytes
  let pipelinedRequestCaps := CapTable.toBytes pipelinedRequest.capTable
  let (responseBytes, responseCaps) ← ffiCppCallPipelinedCapOneShotImpl
    address portHint method.interfaceId method.methodId
    requestBytes requestCaps pipelinedRequestBytes pipelinedRequestCaps
  decodePayloadChecked responseBytes responseCaps

end Interop

def echoBackend : Backend where
  call := fun _ _ payload => pure payload

def emptyBackend : Backend where
  call := fun _ _ _ => pure Capnp.emptyRpcEnvelope

end Rpc
end Capnp
