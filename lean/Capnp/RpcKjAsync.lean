import Capnp.Rpc
import Capnp.KjAsync

namespace Capnp

namespace Rpc
namespace Runtime

/-- Borrow an RPC runtime handle as a `Capnp.KjAsync.Runtime`. -/
@[inline] def asKjAsyncRuntime (runtime : Capnp.Rpc.Runtime) : Capnp.KjAsync.Runtime :=
  { handle := runtime.handle }

end Runtime

namespace RuntimeM

/--
Run a `Capnp.KjAsync.RuntimeM` action on the current RPC runtime handle.
The runtime ownership remains with the outer `Capnp.Rpc.Runtime`.
-/
@[inline] def runKjAsync (action : Capnp.KjAsync.RuntimeM α) : Capnp.Rpc.RuntimeM α :=
  fun rpcRuntime => Capnp.KjAsync.RuntimeM.run (Capnp.Rpc.Runtime.asKjAsyncRuntime rpcRuntime) action

end RuntimeM
end Rpc

end Capnp
