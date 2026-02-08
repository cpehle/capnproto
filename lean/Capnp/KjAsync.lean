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

structure ConnectionPromiseRef where
  runtime : Runtime
  handle : UInt32
  deriving Inhabited, BEq, Repr

structure TaskSetRef where
  runtime : Runtime
  handle : UInt32
  deriving Inhabited, BEq, Repr

structure DatagramPort where
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

@[extern "capnp_lean_kj_async_runtime_listener_accept_start"]
opaque ffiRuntimeListenerAcceptStartImpl (runtime : UInt64) (listener : UInt32) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_connect"]
opaque ffiRuntimeConnectImpl (runtime : UInt64) (address : @& String) (portHint : UInt32) :
    IO UInt32

@[extern "capnp_lean_kj_async_runtime_connect_start"]
opaque ffiRuntimeConnectStartImpl (runtime : UInt64) (address : @& String) (portHint : UInt32) :
    IO UInt32

@[extern "capnp_lean_kj_async_runtime_connection_promise_await"]
opaque ffiRuntimeConnectionPromiseAwaitImpl (runtime : UInt64) (promise : UInt32) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_connection_promise_cancel"]
opaque ffiRuntimeConnectionPromiseCancelImpl (runtime : UInt64) (promise : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_connection_promise_release"]
opaque ffiRuntimeConnectionPromiseReleaseImpl (runtime : UInt64) (promise : UInt32) : IO Unit

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

@[extern "capnp_lean_kj_async_runtime_promise_all_start"]
opaque ffiRuntimePromiseAllStartImpl (runtime : UInt64) (promiseIds : @& ByteArray) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_promise_race_start"]
opaque ffiRuntimePromiseRaceStartImpl (runtime : UInt64) (promiseIds : @& ByteArray) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_task_set_new"]
opaque ffiRuntimeTaskSetNewImpl (runtime : UInt64) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_task_set_release"]
opaque ffiRuntimeTaskSetReleaseImpl (runtime : UInt64) (taskSet : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_task_set_add_promise"]
opaque ffiRuntimeTaskSetAddPromiseImpl
    (runtime : UInt64) (taskSet : UInt32) (promise : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_task_set_clear"]
opaque ffiRuntimeTaskSetClearImpl (runtime : UInt64) (taskSet : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_task_set_is_empty"]
opaque ffiRuntimeTaskSetIsEmptyImpl (runtime : UInt64) (taskSet : UInt32) : IO Bool

@[extern "capnp_lean_kj_async_runtime_task_set_on_empty_start"]
opaque ffiRuntimeTaskSetOnEmptyStartImpl (runtime : UInt64) (taskSet : UInt32) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_task_set_error_count"]
opaque ffiRuntimeTaskSetErrorCountImpl (runtime : UInt64) (taskSet : UInt32) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_task_set_take_last_error"]
opaque ffiRuntimeTaskSetTakeLastErrorImpl (runtime : UInt64) (taskSet : UInt32) : IO (Bool × String)

@[extern "capnp_lean_kj_async_runtime_connection_when_write_disconnected_start"]
opaque ffiRuntimeConnectionWhenWriteDisconnectedStartImpl
    (runtime : UInt64) (connection : UInt32) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_connection_abort_read"]
opaque ffiRuntimeConnectionAbortReadImpl (runtime : UInt64) (connection : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_connection_abort_write"]
opaque ffiRuntimeConnectionAbortWriteImpl
    (runtime : UInt64) (connection : UInt32) (reason : @& String) : IO Unit

@[extern "capnp_lean_kj_async_runtime_new_two_way_pipe"]
opaque ffiRuntimeNewTwoWayPipeImpl (runtime : UInt64) : IO (UInt32 × UInt32)

@[extern "capnp_lean_kj_async_runtime_datagram_bind"]
opaque ffiRuntimeDatagramBindImpl
    (runtime : UInt64) (address : @& String) (portHint : UInt32) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_datagram_release_port"]
opaque ffiRuntimeDatagramReleasePortImpl (runtime : UInt64) (port : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_datagram_get_port"]
opaque ffiRuntimeDatagramGetPortImpl (runtime : UInt64) (port : UInt32) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_datagram_send"]
opaque ffiRuntimeDatagramSendImpl
    (runtime : UInt64) (port : UInt32) (address : @& String) (portHint : UInt32)
    (bytes : @& ByteArray) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_datagram_receive"]
opaque ffiRuntimeDatagramReceiveImpl
    (runtime : UInt64) (port : UInt32) (maxBytes : UInt32) : IO (String × ByteArray)

@[inline] private def millisToNanos (millis : UInt32) : UInt64 :=
  (UInt64.ofNat millis.toNat) * (UInt64.ofNat 1000000)

@[inline] private def uint32ToLeBytes (value : UInt32) : ByteArray :=
  let b0 := UInt8.ofNat ((value >>> 0).toNat &&& 0xff)
  let b1 := UInt8.ofNat ((value >>> 8).toNat &&& 0xff)
  let b2 := UInt8.ofNat ((value >>> 16).toNat &&& 0xff)
  let b3 := UInt8.ofNat ((value >>> 24).toNat &&& 0xff)
  ByteArray.empty.push b0 |>.push b1 |>.push b2 |>.push b3

@[inline] private def encodeUInt32Array (values : Array UInt32) : ByteArray :=
  values.foldl (fun out value => out ++ uint32ToLeBytes value) ByteArray.empty

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

@[inline] def connectStart (runtime : Runtime) (address : String) (portHint : UInt32 := 0) :
    IO ConnectionPromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeConnectStartImpl runtime.handle address portHint)
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

@[inline] def listenerAcceptStart (runtime : Runtime) (listener : Listener) :
    IO ConnectionPromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeListenerAcceptStartImpl runtime.handle listener.handle)
  }

@[inline] def connectionWrite (runtime : Runtime) (connection : Connection)
    (bytes : ByteArray) : IO Unit :=
  ffiRuntimeConnectionWriteImpl runtime.handle connection.handle bytes

@[inline] def connectionRead (runtime : Runtime) (connection : Connection)
    (minBytes maxBytes : UInt32) : IO ByteArray :=
  ffiRuntimeConnectionReadImpl runtime.handle connection.handle minBytes maxBytes

@[inline] def connectionShutdownWrite (runtime : Runtime) (connection : Connection) : IO Unit :=
  ffiRuntimeConnectionShutdownWriteImpl runtime.handle connection.handle

@[inline] def promiseAllStart (runtime : Runtime) (promises : Array PromiseRef) :
    IO PromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimePromiseAllStartImpl runtime.handle
      (encodeUInt32Array (promises.map (·.handle))))
  }

@[inline] def promiseRaceStart (runtime : Runtime) (promises : Array PromiseRef) :
    IO PromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimePromiseRaceStartImpl runtime.handle
      (encodeUInt32Array (promises.map (·.handle))))
  }

@[inline] def taskSetNew (runtime : Runtime) : IO TaskSetRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeTaskSetNewImpl runtime.handle)
  }

@[inline] def taskSetRelease (runtime : Runtime) (taskSet : TaskSetRef) : IO Unit :=
  ffiRuntimeTaskSetReleaseImpl runtime.handle taskSet.handle

@[inline] def taskSetAddPromise (runtime : Runtime) (taskSet : TaskSetRef)
    (promise : PromiseRef) : IO Unit :=
  ffiRuntimeTaskSetAddPromiseImpl runtime.handle taskSet.handle promise.handle

@[inline] def taskSetClear (runtime : Runtime) (taskSet : TaskSetRef) : IO Unit :=
  ffiRuntimeTaskSetClearImpl runtime.handle taskSet.handle

@[inline] def taskSetIsEmpty (runtime : Runtime) (taskSet : TaskSetRef) : IO Bool :=
  ffiRuntimeTaskSetIsEmptyImpl runtime.handle taskSet.handle

@[inline] def taskSetOnEmptyStart (runtime : Runtime) (taskSet : TaskSetRef) : IO PromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeTaskSetOnEmptyStartImpl runtime.handle taskSet.handle)
  }

@[inline] def taskSetErrorCount (runtime : Runtime) (taskSet : TaskSetRef) : IO UInt32 :=
  ffiRuntimeTaskSetErrorCountImpl runtime.handle taskSet.handle

@[inline] def taskSetTakeLastError? (runtime : Runtime) (taskSet : TaskSetRef) :
    IO (Option String) := do
  let (hasError, message) ← ffiRuntimeTaskSetTakeLastErrorImpl runtime.handle taskSet.handle
  if hasError then
    return some message
  else
    return none

@[inline] def withTaskSet (runtime : Runtime) (action : TaskSetRef -> IO α) : IO α := do
  let taskSet ← runtime.taskSetNew
  try
    action taskSet
  finally
    runtime.taskSetRelease taskSet

@[inline] def connectionWhenWriteDisconnectedStart (runtime : Runtime) (connection : Connection) :
    IO PromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeConnectionWhenWriteDisconnectedStartImpl runtime.handle connection.handle)
  }

@[inline] def connectionAbortRead (runtime : Runtime) (connection : Connection) : IO Unit :=
  ffiRuntimeConnectionAbortReadImpl runtime.handle connection.handle

@[inline] def connectionAbortWrite (runtime : Runtime) (connection : Connection)
    (reason : String := "Capnp.KjAsync connection abortWrite") : IO Unit :=
  ffiRuntimeConnectionAbortWriteImpl runtime.handle connection.handle reason

@[inline] def newTwoWayPipe (runtime : Runtime) : IO (Connection × Connection) := do
  let (first, second) ← ffiRuntimeNewTwoWayPipeImpl runtime.handle
  return (
    { runtime := runtime, handle := first },
    { runtime := runtime, handle := second }
  )

@[inline] def datagramBind (runtime : Runtime) (address : String) (portHint : UInt32 := 0) :
    IO DatagramPort := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeDatagramBindImpl runtime.handle address portHint)
  }

@[inline] def datagramReleasePort (runtime : Runtime) (port : DatagramPort) : IO Unit :=
  ffiRuntimeDatagramReleasePortImpl runtime.handle port.handle

@[inline] def datagramGetPort (runtime : Runtime) (port : DatagramPort) : IO UInt32 :=
  ffiRuntimeDatagramGetPortImpl runtime.handle port.handle

@[inline] def datagramSend (runtime : Runtime) (port : DatagramPort)
    (address : String) (bytes : ByteArray) (portHint : UInt32 := 0) : IO UInt32 :=
  ffiRuntimeDatagramSendImpl runtime.handle port.handle address portHint bytes

@[inline] def datagramReceive (runtime : Runtime) (port : DatagramPort)
    (maxBytes : UInt32 := UInt32.ofNat 8192) : IO (String × ByteArray) :=
  ffiRuntimeDatagramReceiveImpl runtime.handle port.handle maxBytes

@[inline] def withDatagramPort (runtime : Runtime) (address : String)
    (action : DatagramPort -> IO α) (portHint : UInt32 := 0) : IO α := do
  let port ← runtime.datagramBind address portHint
  try
    action port
  finally
    runtime.datagramReleasePort port

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

@[inline] def acceptStart (listener : Listener) : IO ConnectionPromiseRef := do
  return {
    runtime := listener.runtime
    handle := (← ffiRuntimeListenerAcceptStartImpl listener.runtime.handle listener.handle)
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

@[inline] def whenWriteDisconnectedStart (connection : Connection) : IO PromiseRef := do
  return {
    runtime := connection.runtime
    handle := (← ffiRuntimeConnectionWhenWriteDisconnectedStartImpl
      connection.runtime.handle connection.handle)
  }

@[inline] def abortRead (connection : Connection) : IO Unit :=
  ffiRuntimeConnectionAbortReadImpl connection.runtime.handle connection.handle

@[inline] def abortWrite (connection : Connection)
    (reason : String := "Capnp.KjAsync connection abortWrite") : IO Unit :=
  ffiRuntimeConnectionAbortWriteImpl connection.runtime.handle connection.handle reason

end Connection

namespace ConnectionPromiseRef

@[inline] def await (promise : ConnectionPromiseRef) : IO Connection := do
  return {
    runtime := promise.runtime
    handle := (← ffiRuntimeConnectionPromiseAwaitImpl promise.runtime.handle promise.handle)
  }

@[inline] def cancel (promise : ConnectionPromiseRef) : IO Unit :=
  ffiRuntimeConnectionPromiseCancelImpl promise.runtime.handle promise.handle

@[inline] def release (promise : ConnectionPromiseRef) : IO Unit :=
  ffiRuntimeConnectionPromiseReleaseImpl promise.runtime.handle promise.handle

@[inline] def awaitAndRelease (promise : ConnectionPromiseRef) : IO Connection := do
  promise.await

end ConnectionPromiseRef

namespace TaskSetRef

@[inline] def release (taskSet : TaskSetRef) : IO Unit :=
  ffiRuntimeTaskSetReleaseImpl taskSet.runtime.handle taskSet.handle

@[inline] def addPromise (taskSet : TaskSetRef) (promise : PromiseRef) : IO Unit :=
  ffiRuntimeTaskSetAddPromiseImpl taskSet.runtime.handle taskSet.handle promise.handle

@[inline] def clear (taskSet : TaskSetRef) : IO Unit :=
  ffiRuntimeTaskSetClearImpl taskSet.runtime.handle taskSet.handle

@[inline] def isEmpty (taskSet : TaskSetRef) : IO Bool :=
  ffiRuntimeTaskSetIsEmptyImpl taskSet.runtime.handle taskSet.handle

@[inline] def onEmptyStart (taskSet : TaskSetRef) : IO PromiseRef := do
  return {
    runtime := taskSet.runtime
    handle := (← ffiRuntimeTaskSetOnEmptyStartImpl taskSet.runtime.handle taskSet.handle)
  }

@[inline] def errorCount (taskSet : TaskSetRef) : IO UInt32 :=
  ffiRuntimeTaskSetErrorCountImpl taskSet.runtime.handle taskSet.handle

@[inline] def takeLastError? (taskSet : TaskSetRef) : IO (Option String) := do
  let (hasError, message) ←
    ffiRuntimeTaskSetTakeLastErrorImpl taskSet.runtime.handle taskSet.handle
  if hasError then
    return some message
  else
    return none

@[inline] def withTaskSet (taskSet : TaskSetRef) (action : TaskSetRef -> IO α) : IO α := do
  try
    action taskSet
  finally
    taskSet.release

end TaskSetRef

namespace DatagramPort

@[inline] def release (port : DatagramPort) : IO Unit :=
  ffiRuntimeDatagramReleasePortImpl port.runtime.handle port.handle

@[inline] def getPort (port : DatagramPort) : IO UInt32 :=
  ffiRuntimeDatagramGetPortImpl port.runtime.handle port.handle

@[inline] def send (port : DatagramPort) (address : String)
    (bytes : ByteArray) (portHint : UInt32 := 0) : IO UInt32 :=
  ffiRuntimeDatagramSendImpl port.runtime.handle port.handle address portHint bytes

@[inline] def receive (port : DatagramPort)
    (maxBytes : UInt32 := UInt32.ofNat 8192) : IO (String × ByteArray) :=
  ffiRuntimeDatagramReceiveImpl port.runtime.handle port.handle maxBytes

@[inline] def withPort (port : DatagramPort) (action : DatagramPort -> IO α) : IO α := do
  try
    action port
  finally
    port.release

end DatagramPort

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

@[inline] def connectStart (address : String) (portHint : UInt32 := 0) :
    RuntimeM ConnectionPromiseRef := do
  Runtime.connectStart (← runtime) address portHint

@[inline] def releaseListener (listener : Listener) : RuntimeM Unit := do
  Runtime.releaseListener (← runtime) listener

@[inline] def releaseConnection (connection : Connection) : RuntimeM Unit := do
  Runtime.releaseConnection (← runtime) connection

@[inline] def accept (listener : Listener) : RuntimeM Connection := do
  Runtime.listenerAccept (← runtime) listener

@[inline] def acceptStart (listener : Listener) : RuntimeM ConnectionPromiseRef := do
  Runtime.listenerAcceptStart (← runtime) listener

@[inline] def write (connection : Connection) (bytes : ByteArray) : RuntimeM Unit := do
  Runtime.connectionWrite (← runtime) connection bytes

@[inline] def read (connection : Connection) (minBytes maxBytes : UInt32) : RuntimeM ByteArray := do
  Runtime.connectionRead (← runtime) connection minBytes maxBytes

@[inline] def shutdownWrite (connection : Connection) : RuntimeM Unit := do
  Runtime.connectionShutdownWrite (← runtime) connection

@[inline] def awaitConnection (promise : ConnectionPromiseRef) : RuntimeM Connection := do
  promise.await

@[inline] def cancelConnection (promise : ConnectionPromiseRef) : RuntimeM Unit := do
  promise.cancel

@[inline] def releaseConnectionPromise (promise : ConnectionPromiseRef) : RuntimeM Unit := do
  promise.release

@[inline] def await (promise : PromiseRef) : RuntimeM Unit := do
  promise.await

@[inline] def cancel (promise : PromiseRef) : RuntimeM Unit := do
  promise.cancel

@[inline] def release (promise : PromiseRef) : RuntimeM Unit := do
  promise.release

@[inline] def awaitAndRelease (promise : PromiseRef) : RuntimeM Unit := do
  promise.awaitAndRelease

@[inline] def promiseAllStart (promises : Array PromiseRef) : RuntimeM PromiseRef := do
  Runtime.promiseAllStart (← runtime) promises

@[inline] def promiseRaceStart (promises : Array PromiseRef) : RuntimeM PromiseRef := do
  Runtime.promiseRaceStart (← runtime) promises

@[inline] def taskSetNew : RuntimeM TaskSetRef := do
  Runtime.taskSetNew (← runtime)

@[inline] def taskSetRelease (taskSet : TaskSetRef) : RuntimeM Unit := do
  taskSet.release

@[inline] def taskSetAddPromise (taskSet : TaskSetRef) (promise : PromiseRef) : RuntimeM Unit := do
  taskSet.addPromise promise

@[inline] def taskSetClear (taskSet : TaskSetRef) : RuntimeM Unit := do
  taskSet.clear

@[inline] def taskSetIsEmpty (taskSet : TaskSetRef) : RuntimeM Bool := do
  taskSet.isEmpty

@[inline] def taskSetOnEmptyStart (taskSet : TaskSetRef) : RuntimeM PromiseRef := do
  taskSet.onEmptyStart

@[inline] def taskSetErrorCount (taskSet : TaskSetRef) : RuntimeM UInt32 := do
  taskSet.errorCount

@[inline] def taskSetTakeLastError? (taskSet : TaskSetRef) : RuntimeM (Option String) := do
  taskSet.takeLastError?

@[inline] def withTaskSet (action : TaskSetRef -> RuntimeM α) : RuntimeM α := do
  let taskSet ← taskSetNew
  try
    action taskSet
  finally
    taskSet.release

@[inline] def connectionWhenWriteDisconnectedStart (connection : Connection) :
    RuntimeM PromiseRef := do
  connection.whenWriteDisconnectedStart

@[inline] def connectionAbortRead (connection : Connection) : RuntimeM Unit := do
  connection.abortRead

@[inline] def connectionAbortWrite (connection : Connection)
    (reason : String := "Capnp.KjAsync connection abortWrite") : RuntimeM Unit := do
  connection.abortWrite reason

@[inline] def newTwoWayPipe : RuntimeM (Connection × Connection) := do
  Runtime.newTwoWayPipe (← runtime)

@[inline] def datagramBind (address : String) (portHint : UInt32 := 0) : RuntimeM DatagramPort := do
  Runtime.datagramBind (← runtime) address portHint

@[inline] def datagramReleasePort (port : DatagramPort) : RuntimeM Unit := do
  port.release

@[inline] def datagramGetPort (port : DatagramPort) : RuntimeM UInt32 := do
  port.getPort

@[inline] def datagramSend (port : DatagramPort) (address : String)
    (bytes : ByteArray) (portHint : UInt32 := 0) : RuntimeM UInt32 := do
  port.send address bytes portHint

@[inline] def datagramReceive (port : DatagramPort)
    (maxBytes : UInt32 := UInt32.ofNat 8192) : RuntimeM (String × ByteArray) := do
  port.receive maxBytes

@[inline] def withDatagramPort (address : String)
    (action : DatagramPort -> RuntimeM α) (portHint : UInt32 := 0) : RuntimeM α := do
  let port ← datagramBind address portHint
  try
    action port
  finally
    port.release

end RuntimeM

end KjAsync
end Capnp
