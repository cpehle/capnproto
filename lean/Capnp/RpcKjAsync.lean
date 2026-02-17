import Capnp.Rpc
import Capnp.KjAsync

namespace Capnp

namespace Rpc
namespace Runtime

/-- Borrow an RPC runtime handle as a `Capnp.KjAsync.Runtime`. -/
@[inline] def asKjAsyncRuntime (runtime : Capnp.Rpc.Runtime) : Capnp.KjAsync.Runtime :=
  { handle := runtime.handle }

/--
Run an `IO` action using a borrowed `Capnp.KjAsync.Runtime` view of this RPC runtime.
The runtime ownership remains with `Capnp.Rpc.Runtime`.
-/
@[inline] def withKjAsyncRuntime (runtime : Capnp.Rpc.Runtime)
    (action : Capnp.KjAsync.Runtime -> IO α) : IO α :=
  action runtime.asKjAsyncRuntime

/--
Run a `Capnp.KjAsync.RuntimeM` action on this RPC runtime handle.
The runtime ownership remains with `Capnp.Rpc.Runtime`.
-/
@[inline] def runKjAsync (runtime : Capnp.Rpc.Runtime)
    (action : Capnp.KjAsync.RuntimeM α) : IO α :=
  Capnp.KjAsync.RuntimeM.run runtime.asKjAsyncRuntime action

end Runtime

namespace RuntimeM

/-- Borrow the current RPC runtime handle as `Capnp.KjAsync.Runtime`. -/
@[inline] def kjAsyncRuntime : Capnp.Rpc.RuntimeM Capnp.KjAsync.Runtime := do
  return (← Capnp.Rpc.RuntimeM.runtime).asKjAsyncRuntime

/--
Run an `IO` action using a borrowed `Capnp.KjAsync.Runtime` view of the current RPC runtime.
The runtime ownership remains with the outer `Capnp.Rpc.Runtime`.
-/
@[inline] def withKjAsyncRuntime
    (action : Capnp.KjAsync.Runtime -> IO α) : Capnp.Rpc.RuntimeM α := do
  action (← kjAsyncRuntime)

/--
Run a `Capnp.KjAsync.RuntimeM` action on the current RPC runtime handle.
The runtime ownership remains with the outer `Capnp.Rpc.Runtime`.
-/
@[inline] def runKjAsync (action : Capnp.KjAsync.RuntimeM α) : Capnp.Rpc.RuntimeM α :=
  withKjAsyncRuntime fun runtime => Capnp.KjAsync.RuntimeM.run runtime action

end RuntimeM
end Rpc

end Capnp
