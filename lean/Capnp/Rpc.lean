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

def echoBackend : Backend where
  call := fun _ _ payload => pure payload

def emptyBackend : Backend where
  call := fun _ _ _ => pure Capnp.emptyRpcEnvelope

end Rpc
end Capnp
