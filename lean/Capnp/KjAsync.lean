import Init
import Init.System.Promise

namespace Capnp
namespace KjAsync

structure Runtime where
  handle : UInt64
  deriving Inhabited, BEq, Repr

structure PromiseRef where
  runtime : Runtime
  handle : UInt32
  deriving Inhabited, BEq, Repr

structure Listener where
  runtime : Runtime
  handle : UInt32
  deriving Inhabited, BEq, Repr

structure Connection where
  runtime : Runtime
  handle : UInt32
  deriving Inhabited, BEq, Repr

@[extern "capnp_lean_kj_async_runtime_new"]
opaque ffiRuntimeNewImpl : IO UInt64

@[extern "capnp_lean_kj_async_runtime_release"]
opaque ffiRuntimeReleaseImpl (runtime : UInt64) : IO Unit

@[extern "capnp_lean_kj_async_runtime_is_alive"]
opaque ffiRuntimeIsAliveImpl (runtime : UInt64) : IO Bool

@[extern "capnp_lean_kj_async_runtime_sleep_nanos_start"]
opaque ffiRuntimeSleepNanosStartImpl (runtime : UInt64) (delayNanos : UInt64) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_promise_await"]
opaque ffiRuntimePromiseAwaitImpl (runtime : UInt64) (promise : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_promise_cancel"]
opaque ffiRuntimePromiseCancelImpl (runtime : UInt64) (promise : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_promise_release"]
opaque ffiRuntimePromiseReleaseImpl (runtime : UInt64) (promise : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_listen"]
opaque ffiRuntimeListenImpl (runtime : UInt64) (address : @& String) (portHint : UInt32) :
    IO UInt32

@[extern "capnp_lean_kj_async_runtime_release_listener"]
opaque ffiRuntimeReleaseListenerImpl (runtime : UInt64) (listener : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_listener_accept"]
opaque ffiRuntimeListenerAcceptImpl (runtime : UInt64) (listener : UInt32) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_connect"]
opaque ffiRuntimeConnectImpl (runtime : UInt64) (address : @& String) (portHint : UInt32) :
    IO UInt32

@[extern "capnp_lean_kj_async_runtime_release_connection"]
opaque ffiRuntimeReleaseConnectionImpl (runtime : UInt64) (connection : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_connection_write"]
opaque ffiRuntimeConnectionWriteImpl
    (runtime : UInt64) (connection : UInt32) (bytes : @& ByteArray) : IO Unit

@[extern "capnp_lean_kj_async_runtime_connection_read"]
opaque ffiRuntimeConnectionReadImpl
    (runtime : UInt64) (connection : UInt32) (minBytes : UInt32) (maxBytes : UInt32) :
    IO ByteArray

@[extern "capnp_lean_kj_async_runtime_connection_shutdown_write"]
opaque ffiRuntimeConnectionShutdownWriteImpl (runtime : UInt64) (connection : UInt32) : IO Unit

@[inline] private def millisToNanos (millis : UInt32) : UInt64 :=
  (UInt64.ofNat millis.toNat) * (UInt64.ofNat 1000000)

namespace Runtime

@[inline] def init : IO Runtime := do
  return { handle := (← ffiRuntimeNewImpl) }

@[inline] def shutdown (runtime : Runtime) : IO Unit :=
  ffiRuntimeReleaseImpl runtime.handle

@[inline] def isAlive (runtime : Runtime) : IO Bool :=
  ffiRuntimeIsAliveImpl runtime.handle

@[inline] def sleepNanosStart (runtime : Runtime) (delayNanos : UInt64) : IO PromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeSleepNanosStartImpl runtime.handle delayNanos)
  }

@[inline] def sleepMillisStart (runtime : Runtime) (delayMillis : UInt32) : IO PromiseRef :=
  runtime.sleepNanosStart (millisToNanos delayMillis)

@[inline] def sleepNanos (runtime : Runtime) (delayNanos : UInt64) : IO Unit := do
  let promise ← runtime.sleepNanosStart delayNanos
  ffiRuntimePromiseAwaitImpl runtime.handle promise.handle

@[inline] def sleepMillis (runtime : Runtime) (delayMillis : UInt32) : IO Unit :=
  runtime.sleepNanos (millisToNanos delayMillis)

@[inline] def listen (runtime : Runtime) (address : String) (portHint : UInt32 := 0) :
    IO Listener := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeListenImpl runtime.handle address portHint)
  }

@[inline] def connect (runtime : Runtime) (address : String) (portHint : UInt32 := 0) :
    IO Connection := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeConnectImpl runtime.handle address portHint)
  }

@[inline] def releaseListener (runtime : Runtime) (listener : Listener) : IO Unit :=
  ffiRuntimeReleaseListenerImpl runtime.handle listener.handle

@[inline] def releaseConnection (runtime : Runtime) (connection : Connection) : IO Unit :=
  ffiRuntimeReleaseConnectionImpl runtime.handle connection.handle

@[inline] def listenerAccept (runtime : Runtime) (listener : Listener) : IO Connection := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeListenerAcceptImpl runtime.handle listener.handle)
  }

@[inline] def connectionWrite (runtime : Runtime) (connection : Connection)
    (bytes : ByteArray) : IO Unit :=
  ffiRuntimeConnectionWriteImpl runtime.handle connection.handle bytes

@[inline] def connectionRead (runtime : Runtime) (connection : Connection)
    (minBytes maxBytes : UInt32) : IO ByteArray :=
  ffiRuntimeConnectionReadImpl runtime.handle connection.handle minBytes maxBytes

@[inline] def connectionShutdownWrite (runtime : Runtime) (connection : Connection) : IO Unit :=
  ffiRuntimeConnectionShutdownWriteImpl runtime.handle connection.handle

def withRuntime (action : Runtime -> IO α) : IO α := do
  let runtime ← init
  try
    action runtime
  finally
    runtime.shutdown

end Runtime

namespace PromiseRef

@[inline] def await (promise : PromiseRef) : IO Unit :=
  ffiRuntimePromiseAwaitImpl promise.runtime.handle promise.handle

@[inline] def cancel (promise : PromiseRef) : IO Unit :=
  ffiRuntimePromiseCancelImpl promise.runtime.handle promise.handle

@[inline] def release (promise : PromiseRef) : IO Unit :=
  ffiRuntimePromiseReleaseImpl promise.runtime.handle promise.handle

@[inline] def awaitAndRelease (promise : PromiseRef) : IO Unit := do
  promise.await

@[inline] def awaitAsTask (promise : PromiseRef) : IO (Task (Except IO.Error Unit)) :=
  IO.asTask promise.awaitAndRelease

def toIOPromise (promise : PromiseRef) : IO (IO.Promise (Except String Unit)) := do
  let out ← IO.Promise.new
  let _task ← IO.asTask do
    let result ←
      try
        promise.await
        pure (Except.ok ())
      catch e =>
        pure (Except.error e.toString)
    out.resolve result
  return out

end PromiseRef

namespace Listener

@[inline] def release (listener : Listener) : IO Unit :=
  ffiRuntimeReleaseListenerImpl listener.runtime.handle listener.handle

@[inline] def accept (listener : Listener) : IO Connection := do
  return {
    runtime := listener.runtime
    handle := (← ffiRuntimeListenerAcceptImpl listener.runtime.handle listener.handle)
  }

end Listener

namespace Connection

@[inline] def release (connection : Connection) : IO Unit :=
  ffiRuntimeReleaseConnectionImpl connection.runtime.handle connection.handle

@[inline] def write (connection : Connection) (bytes : ByteArray) : IO Unit :=
  ffiRuntimeConnectionWriteImpl connection.runtime.handle connection.handle bytes

@[inline] def read (connection : Connection) (minBytes maxBytes : UInt32) : IO ByteArray :=
  ffiRuntimeConnectionReadImpl connection.runtime.handle connection.handle minBytes maxBytes

@[inline] def shutdownWrite (connection : Connection) : IO Unit :=
  ffiRuntimeConnectionShutdownWriteImpl connection.runtime.handle connection.handle

end Connection

abbrev RuntimeM := ReaderT Runtime IO

namespace RuntimeM

@[inline] def run (runtime : Runtime) (action : RuntimeM α) : IO α :=
  action runtime

@[inline] def runWithNewRuntime (action : RuntimeM α) : IO α :=
  Runtime.withRuntime fun runtime => action runtime

@[inline] def runtime : RuntimeM Runtime := read

@[inline] def isAlive : RuntimeM Bool := do
  Runtime.isAlive (← runtime)

@[inline] def sleepNanosStart (delayNanos : UInt64) : RuntimeM PromiseRef := do
  Runtime.sleepNanosStart (← runtime) delayNanos

@[inline] def sleepMillisStart (delayMillis : UInt32) : RuntimeM PromiseRef := do
  Runtime.sleepMillisStart (← runtime) delayMillis

@[inline] def sleepNanos (delayNanos : UInt64) : RuntimeM Unit := do
  Runtime.sleepNanos (← runtime) delayNanos

@[inline] def sleepMillis (delayMillis : UInt32) : RuntimeM Unit := do
  Runtime.sleepMillis (← runtime) delayMillis

@[inline] def listen (address : String) (portHint : UInt32 := 0) : RuntimeM Listener := do
  Runtime.listen (← runtime) address portHint

@[inline] def connect (address : String) (portHint : UInt32 := 0) : RuntimeM Connection := do
  Runtime.connect (← runtime) address portHint

@[inline] def releaseListener (listener : Listener) : RuntimeM Unit := do
  Runtime.releaseListener (← runtime) listener

@[inline] def releaseConnection (connection : Connection) : RuntimeM Unit := do
  Runtime.releaseConnection (← runtime) connection

@[inline] def accept (listener : Listener) : RuntimeM Connection := do
  Runtime.listenerAccept (← runtime) listener

@[inline] def write (connection : Connection) (bytes : ByteArray) : RuntimeM Unit := do
  Runtime.connectionWrite (← runtime) connection bytes

@[inline] def read (connection : Connection) (minBytes maxBytes : UInt32) : RuntimeM ByteArray := do
  Runtime.connectionRead (← runtime) connection minBytes maxBytes

@[inline] def shutdownWrite (connection : Connection) : RuntimeM Unit := do
  Runtime.connectionShutdownWrite (← runtime) connection

@[inline] def await (promise : PromiseRef) : RuntimeM Unit := do
  promise.await

@[inline] def cancel (promise : PromiseRef) : RuntimeM Unit := do
  promise.cancel

@[inline] def release (promise : PromiseRef) : RuntimeM Unit := do
  promise.release

@[inline] def awaitAndRelease (promise : PromiseRef) : RuntimeM Unit := do
  promise.awaitAndRelease

end RuntimeM

end KjAsync
end Capnp
