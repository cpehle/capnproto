import Capnp.Runtime

namespace Capnp
namespace Rpc

abbrev Payload := Capnp.RpcEnvelope

structure Method where
  interfaceId : UInt64
  methodId : UInt16
  deriving Inhabited, BEq, Repr

abbrev Client := Capnp.Capability

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

structure Runtime where
  handle : UInt64
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
