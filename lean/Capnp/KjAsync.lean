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

structure BytesPromiseRef where
  runtime : Runtime
  handle : UInt32
  deriving Inhabited, BEq, Repr

structure UInt32PromiseRef where
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

structure DatagramReceivePromiseRef where
  runtime : Runtime
  handle : UInt32
  deriving Inhabited, BEq, Repr

structure HttpResponse where
  status : UInt32
  body : ByteArray
  deriving Inhabited, BEq

structure HttpResponsePromiseRef where
  runtime : Runtime
  handle : UInt32
  deriving Inhabited, BEq, Repr

structure WebSocket where
  runtime : Runtime
  handle : UInt32
  deriving Inhabited, BEq, Repr

structure WebSocketPromiseRef where
  runtime : Runtime
  handle : UInt32
  deriving Inhabited, BEq, Repr

structure WebSocketMessagePromiseRef where
  runtime : Runtime
  handle : UInt32
  deriving Inhabited, BEq, Repr

inductive WebSocketMessage where
  | text (value : String)
  | binary (value : ByteArray)
  | close (code : UInt16) (reason : String)
  deriving Inhabited, BEq

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

@[extern "capnp_lean_kj_async_runtime_connection_write_start"]
opaque ffiRuntimeConnectionWriteStartImpl
    (runtime : UInt64) (connection : UInt32) (bytes : @& ByteArray) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_connection_read"]
opaque ffiRuntimeConnectionReadImpl
    (runtime : UInt64) (connection : UInt32) (minBytes : UInt32) (maxBytes : UInt32) :
    IO ByteArray

@[extern "capnp_lean_kj_async_runtime_connection_read_start"]
opaque ffiRuntimeConnectionReadStartImpl
    (runtime : UInt64) (connection : UInt32) (minBytes : UInt32) (maxBytes : UInt32) :
    IO UInt32

@[extern "capnp_lean_kj_async_runtime_bytes_promise_await"]
opaque ffiRuntimeBytesPromiseAwaitImpl (runtime : UInt64) (promise : UInt32) : IO ByteArray

@[extern "capnp_lean_kj_async_runtime_bytes_promise_cancel"]
opaque ffiRuntimeBytesPromiseCancelImpl (runtime : UInt64) (promise : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_bytes_promise_release"]
opaque ffiRuntimeBytesPromiseReleaseImpl (runtime : UInt64) (promise : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_connection_shutdown_write"]
opaque ffiRuntimeConnectionShutdownWriteImpl (runtime : UInt64) (connection : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_connection_shutdown_write_start"]
opaque ffiRuntimeConnectionShutdownWriteStartImpl
    (runtime : UInt64) (connection : UInt32) : IO UInt32

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

@[extern "capnp_lean_kj_async_runtime_datagram_send_start"]
opaque ffiRuntimeDatagramSendStartImpl
    (runtime : UInt64) (port : UInt32) (address : @& String) (portHint : UInt32)
    (bytes : @& ByteArray) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_uint32_promise_await"]
opaque ffiRuntimeUInt32PromiseAwaitImpl (runtime : UInt64) (promise : UInt32) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_uint32_promise_cancel"]
opaque ffiRuntimeUInt32PromiseCancelImpl (runtime : UInt64) (promise : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_uint32_promise_release"]
opaque ffiRuntimeUInt32PromiseReleaseImpl (runtime : UInt64) (promise : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_datagram_receive"]
opaque ffiRuntimeDatagramReceiveImpl
    (runtime : UInt64) (port : UInt32) (maxBytes : UInt32) : IO (String × ByteArray)

@[extern "capnp_lean_kj_async_runtime_datagram_receive_start"]
opaque ffiRuntimeDatagramReceiveStartImpl
    (runtime : UInt64) (port : UInt32) (maxBytes : UInt32) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_datagram_receive_promise_await"]
opaque ffiRuntimeDatagramReceivePromiseAwaitImpl
    (runtime : UInt64) (promise : UInt32) : IO (String × ByteArray)

@[extern "capnp_lean_kj_async_runtime_datagram_receive_promise_cancel"]
opaque ffiRuntimeDatagramReceivePromiseCancelImpl (runtime : UInt64) (promise : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_datagram_receive_promise_release"]
opaque ffiRuntimeDatagramReceivePromiseReleaseImpl
    (runtime : UInt64) (promise : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_http_request"]
opaque ffiRuntimeHttpRequestImpl
    (runtime : UInt64) (method : UInt32) (address : @& String) (portHint : UInt32)
    (path : @& String) (body : @& ByteArray) : IO (UInt32 × ByteArray)

@[extern "capnp_lean_kj_async_runtime_http_request_start"]
opaque ffiRuntimeHttpRequestStartImpl
    (runtime : UInt64) (method : UInt32) (address : @& String) (portHint : UInt32)
    (path : @& String) (body : @& ByteArray) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_http_response_promise_await"]
opaque ffiRuntimeHttpResponsePromiseAwaitImpl
    (runtime : UInt64) (promise : UInt32) : IO (UInt32 × ByteArray)

@[extern "capnp_lean_kj_async_runtime_http_response_promise_cancel"]
opaque ffiRuntimeHttpResponsePromiseCancelImpl (runtime : UInt64) (promise : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_http_response_promise_release"]
opaque ffiRuntimeHttpResponsePromiseReleaseImpl (runtime : UInt64) (promise : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_websocket_connect"]
opaque ffiRuntimeWebSocketConnectImpl
    (runtime : UInt64) (address : @& String) (portHint : UInt32) (path : @& String) :
    IO UInt32

@[extern "capnp_lean_kj_async_runtime_websocket_connect_start"]
opaque ffiRuntimeWebSocketConnectStartImpl
    (runtime : UInt64) (address : @& String) (portHint : UInt32) (path : @& String) :
    IO UInt32

@[extern "capnp_lean_kj_async_runtime_websocket_promise_await"]
opaque ffiRuntimeWebSocketPromiseAwaitImpl (runtime : UInt64) (promise : UInt32) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_websocket_promise_cancel"]
opaque ffiRuntimeWebSocketPromiseCancelImpl (runtime : UInt64) (promise : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_websocket_promise_release"]
opaque ffiRuntimeWebSocketPromiseReleaseImpl (runtime : UInt64) (promise : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_websocket_release"]
opaque ffiRuntimeWebSocketReleaseImpl (runtime : UInt64) (webSocket : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_websocket_send_text_start"]
opaque ffiRuntimeWebSocketSendTextStartImpl
    (runtime : UInt64) (webSocket : UInt32) (text : @& String) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_websocket_send_text"]
opaque ffiRuntimeWebSocketSendTextImpl
    (runtime : UInt64) (webSocket : UInt32) (text : @& String) : IO Unit

@[extern "capnp_lean_kj_async_runtime_websocket_send_binary_start"]
opaque ffiRuntimeWebSocketSendBinaryStartImpl
    (runtime : UInt64) (webSocket : UInt32) (bytes : @& ByteArray) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_websocket_send_binary"]
opaque ffiRuntimeWebSocketSendBinaryImpl
    (runtime : UInt64) (webSocket : UInt32) (bytes : @& ByteArray) : IO Unit

@[extern "capnp_lean_kj_async_runtime_websocket_receive_start"]
opaque ffiRuntimeWebSocketReceiveStartImpl
    (runtime : UInt64) (webSocket : UInt32) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_websocket_message_promise_await"]
opaque ffiRuntimeWebSocketMessagePromiseAwaitImpl
    (runtime : UInt64) (promise : UInt32) : IO (UInt32 × UInt32 × String × ByteArray)

@[extern "capnp_lean_kj_async_runtime_websocket_message_promise_cancel"]
opaque ffiRuntimeWebSocketMessagePromiseCancelImpl (runtime : UInt64) (promise : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_websocket_message_promise_release"]
opaque ffiRuntimeWebSocketMessagePromiseReleaseImpl (runtime : UInt64) (promise : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_websocket_receive"]
opaque ffiRuntimeWebSocketReceiveImpl
    (runtime : UInt64) (webSocket : UInt32) : IO (UInt32 × UInt32 × String × ByteArray)

@[extern "capnp_lean_kj_async_runtime_websocket_close_start"]
opaque ffiRuntimeWebSocketCloseStartImpl
    (runtime : UInt64) (webSocket : UInt32) (code : UInt32) (reason : @& String) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_websocket_close"]
opaque ffiRuntimeWebSocketCloseImpl
    (runtime : UInt64) (webSocket : UInt32) (code : UInt32) (reason : @& String) : IO Unit

@[extern "capnp_lean_kj_async_runtime_websocket_disconnect"]
opaque ffiRuntimeWebSocketDisconnectImpl (runtime : UInt64) (webSocket : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_websocket_abort"]
opaque ffiRuntimeWebSocketAbortImpl (runtime : UInt64) (webSocket : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_new_websocket_pipe"]
opaque ffiRuntimeNewWebSocketPipeImpl (runtime : UInt64) : IO (UInt32 × UInt32)

@[inline] private def millisToNanos (millis : UInt32) : UInt64 :=
  millis.toUInt64 * (1000000 : UInt64)

@[inline] private def uint32ToLeBytes (value : UInt32) : ByteArray :=
  let b0 := value.toUInt8
  let b1 := (value >>> 8).toUInt8
  let b2 := (value >>> 16).toUInt8
  let b3 := (value >>> 24).toUInt8
  ByteArray.empty.push b0 |>.push b1 |>.push b2 |>.push b3

@[inline] private def encodeUInt32Array (values : Array UInt32) : ByteArray :=
  values.foldl (fun out value => out ++ uint32ToLeBytes value) ByteArray.empty

inductive HttpMethod where
  | get
  | head
  | post
  | put
  | delete
  | patch
  | options
  | trace
  deriving Inhabited, BEq, Repr

@[inline] private def httpMethodToTag (method : HttpMethod) : UInt32 :=
  match method with
  | .get => UInt32.ofNat 0
  | .head => UInt32.ofNat 1
  | .post => UInt32.ofNat 2
  | .put => UInt32.ofNat 3
  | .delete => UInt32.ofNat 4
  | .patch => UInt32.ofNat 5
  | .options => UInt32.ofNat 6
  | .trace => UInt32.ofNat 7

@[inline] private def decodeWebSocketMessage
    (tag : UInt32) (closeCode : UInt32) (text : String) (bytes : ByteArray) :
    IO WebSocketMessage := do
  if tag == UInt32.ofNat 0 then
    return .text text
  else if tag == UInt32.ofNat 1 then
    return .binary bytes
  else if tag == UInt32.ofNat 2 then
    return .close closeCode.toUInt16 text
  else
    throw (IO.userError s!"unknown websocket message tag: {tag.toNat}")

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

@[inline] def connectionWriteStart (runtime : Runtime) (connection : Connection)
    (bytes : ByteArray) : IO PromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeConnectionWriteStartImpl runtime.handle connection.handle bytes)
  }

@[inline] def connectionRead (runtime : Runtime) (connection : Connection)
    (minBytes maxBytes : UInt32) : IO ByteArray :=
  ffiRuntimeConnectionReadImpl runtime.handle connection.handle minBytes maxBytes

@[inline] def connectionReadStart (runtime : Runtime) (connection : Connection)
    (minBytes maxBytes : UInt32) : IO BytesPromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeConnectionReadStartImpl
      runtime.handle connection.handle minBytes maxBytes)
  }

@[inline] def bytesPromiseAwait (runtime : Runtime) (promise : BytesPromiseRef) : IO ByteArray :=
  ffiRuntimeBytesPromiseAwaitImpl runtime.handle promise.handle

@[inline] def bytesPromiseCancel (runtime : Runtime) (promise : BytesPromiseRef) : IO Unit :=
  ffiRuntimeBytesPromiseCancelImpl runtime.handle promise.handle

@[inline] def bytesPromiseRelease (runtime : Runtime) (promise : BytesPromiseRef) : IO Unit :=
  ffiRuntimeBytesPromiseReleaseImpl runtime.handle promise.handle

@[inline] def connectionShutdownWrite (runtime : Runtime) (connection : Connection) : IO Unit :=
  ffiRuntimeConnectionShutdownWriteImpl runtime.handle connection.handle

@[inline] def connectionShutdownWriteStart (runtime : Runtime)
    (connection : Connection) : IO PromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeConnectionShutdownWriteStartImpl runtime.handle connection.handle)
  }

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

@[inline] def datagramSendStart (runtime : Runtime) (port : DatagramPort)
    (address : String) (bytes : ByteArray) (portHint : UInt32 := 0) : IO UInt32PromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeDatagramSendStartImpl runtime.handle port.handle address portHint bytes)
  }

@[inline] def uint32PromiseAwait (runtime : Runtime) (promise : UInt32PromiseRef) : IO UInt32 :=
  ffiRuntimeUInt32PromiseAwaitImpl runtime.handle promise.handle

@[inline] def uint32PromiseCancel (runtime : Runtime) (promise : UInt32PromiseRef) : IO Unit :=
  ffiRuntimeUInt32PromiseCancelImpl runtime.handle promise.handle

@[inline] def uint32PromiseRelease (runtime : Runtime) (promise : UInt32PromiseRef) : IO Unit :=
  ffiRuntimeUInt32PromiseReleaseImpl runtime.handle promise.handle

@[inline] def datagramReceive (runtime : Runtime) (port : DatagramPort)
    (maxBytes : UInt32 := UInt32.ofNat 8192) : IO (String × ByteArray) :=
  ffiRuntimeDatagramReceiveImpl runtime.handle port.handle maxBytes

@[inline] def datagramReceiveStart (runtime : Runtime) (port : DatagramPort)
    (maxBytes : UInt32 := UInt32.ofNat 8192) : IO DatagramReceivePromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeDatagramReceiveStartImpl runtime.handle port.handle maxBytes)
  }

@[inline] def datagramReceivePromiseAwait (runtime : Runtime)
    (promise : DatagramReceivePromiseRef) : IO (String × ByteArray) :=
  ffiRuntimeDatagramReceivePromiseAwaitImpl runtime.handle promise.handle

@[inline] def datagramReceivePromiseCancel (runtime : Runtime)
    (promise : DatagramReceivePromiseRef) : IO Unit :=
  ffiRuntimeDatagramReceivePromiseCancelImpl runtime.handle promise.handle

@[inline] def datagramReceivePromiseRelease (runtime : Runtime)
    (promise : DatagramReceivePromiseRef) : IO Unit :=
  ffiRuntimeDatagramReceivePromiseReleaseImpl runtime.handle promise.handle

@[inline] def withDatagramPort (runtime : Runtime) (address : String)
    (action : DatagramPort -> IO α) (portHint : UInt32 := 0) : IO α := do
  let port ← runtime.datagramBind address portHint
  try
    action port
  finally
    runtime.datagramReleasePort port

@[inline] def httpRequest (runtime : Runtime) (method : HttpMethod) (address : String)
    (path : String) (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) :
    IO HttpResponse := do
  let (status, responseBody) ←
    ffiRuntimeHttpRequestImpl runtime.handle (httpMethodToTag method) address portHint path body
  return { status := status, body := responseBody }

@[inline] def httpRequestStart (runtime : Runtime) (method : HttpMethod) (address : String)
    (path : String) (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) :
    IO HttpResponsePromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeHttpRequestStartImpl
      runtime.handle (httpMethodToTag method) address portHint path body)
  }

@[inline] def httpResponsePromiseAwait (runtime : Runtime)
    (promise : HttpResponsePromiseRef) : IO HttpResponse := do
  let (status, responseBody) ←
    ffiRuntimeHttpResponsePromiseAwaitImpl runtime.handle promise.handle
  return { status := status, body := responseBody }

@[inline] def httpResponsePromiseCancel (runtime : Runtime)
    (promise : HttpResponsePromiseRef) : IO Unit :=
  ffiRuntimeHttpResponsePromiseCancelImpl runtime.handle promise.handle

@[inline] def httpResponsePromiseRelease (runtime : Runtime)
    (promise : HttpResponsePromiseRef) : IO Unit :=
  ffiRuntimeHttpResponsePromiseReleaseImpl runtime.handle promise.handle

@[inline] def httpGet (runtime : Runtime) (address : String) (path : String)
    (portHint : UInt32 := 0) : IO HttpResponse :=
  runtime.httpRequest .get address path ByteArray.empty portHint

@[inline] def httpPost (runtime : Runtime) (address : String) (path : String)
    (body : ByteArray) (portHint : UInt32 := 0) : IO HttpResponse :=
  runtime.httpRequest .post address path body portHint

@[inline] def webSocketConnect (runtime : Runtime) (address : String)
    (path : String) (portHint : UInt32 := 0) : IO WebSocket := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeWebSocketConnectImpl runtime.handle address portHint path)
  }

@[inline] def webSocketConnectStart (runtime : Runtime) (address : String)
    (path : String) (portHint : UInt32 := 0) : IO WebSocketPromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeWebSocketConnectStartImpl runtime.handle address portHint path)
  }

@[inline] def webSocketPromiseAwait (runtime : Runtime)
    (promise : WebSocketPromiseRef) : IO WebSocket := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeWebSocketPromiseAwaitImpl runtime.handle promise.handle)
  }

@[inline] def webSocketPromiseCancel (runtime : Runtime)
    (promise : WebSocketPromiseRef) : IO Unit :=
  ffiRuntimeWebSocketPromiseCancelImpl runtime.handle promise.handle

@[inline] def webSocketPromiseRelease (runtime : Runtime)
    (promise : WebSocketPromiseRef) : IO Unit :=
  ffiRuntimeWebSocketPromiseReleaseImpl runtime.handle promise.handle

@[inline] def webSocketRelease (runtime : Runtime) (webSocket : WebSocket) : IO Unit :=
  ffiRuntimeWebSocketReleaseImpl runtime.handle webSocket.handle

@[inline] def webSocketSendTextStart (runtime : Runtime) (webSocket : WebSocket) (text : String) :
    IO PromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeWebSocketSendTextStartImpl runtime.handle webSocket.handle text)
  }

@[inline] def webSocketSendText (runtime : Runtime) (webSocket : WebSocket) (text : String) :
    IO Unit := do
  let promise ← runtime.webSocketSendTextStart webSocket text
  ffiRuntimePromiseAwaitImpl runtime.handle promise.handle

@[inline] def webSocketSendBinaryStart (runtime : Runtime) (webSocket : WebSocket)
    (bytes : ByteArray) : IO PromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeWebSocketSendBinaryStartImpl runtime.handle webSocket.handle bytes)
  }

@[inline] def webSocketSendBinary (runtime : Runtime) (webSocket : WebSocket) (bytes : ByteArray) :
    IO Unit := do
  let promise ← runtime.webSocketSendBinaryStart webSocket bytes
  ffiRuntimePromiseAwaitImpl runtime.handle promise.handle

@[inline] def webSocketReceiveStart (runtime : Runtime) (webSocket : WebSocket) :
    IO WebSocketMessagePromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeWebSocketReceiveStartImpl runtime.handle webSocket.handle)
  }

@[inline] def webSocketMessagePromiseAwait (runtime : Runtime)
    (promise : WebSocketMessagePromiseRef) : IO WebSocketMessage := do
  let (tag, closeCode, text, bytes) ←
    ffiRuntimeWebSocketMessagePromiseAwaitImpl runtime.handle promise.handle
  decodeWebSocketMessage tag closeCode text bytes

@[inline] def webSocketMessagePromiseCancel (runtime : Runtime)
    (promise : WebSocketMessagePromiseRef) : IO Unit :=
  ffiRuntimeWebSocketMessagePromiseCancelImpl runtime.handle promise.handle

@[inline] def webSocketMessagePromiseRelease (runtime : Runtime)
    (promise : WebSocketMessagePromiseRef) : IO Unit :=
  ffiRuntimeWebSocketMessagePromiseReleaseImpl runtime.handle promise.handle

@[inline] def webSocketReceive (runtime : Runtime) (webSocket : WebSocket) : IO WebSocketMessage := do
  let promise ← runtime.webSocketReceiveStart webSocket
  runtime.webSocketMessagePromiseAwait promise

@[inline] def webSocketCloseStart (runtime : Runtime) (webSocket : WebSocket)
    (code : UInt16) (reason : String := "") : IO PromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeWebSocketCloseStartImpl
      runtime.handle webSocket.handle code.toUInt32 reason)
  }

@[inline] def webSocketClose (runtime : Runtime) (webSocket : WebSocket)
    (code : UInt16) (reason : String := "") : IO Unit := do
  let promise ← runtime.webSocketCloseStart webSocket code reason
  ffiRuntimePromiseAwaitImpl runtime.handle promise.handle

@[inline] def webSocketDisconnect (runtime : Runtime) (webSocket : WebSocket) : IO Unit :=
  ffiRuntimeWebSocketDisconnectImpl runtime.handle webSocket.handle

@[inline] def webSocketAbort (runtime : Runtime) (webSocket : WebSocket) : IO Unit :=
  ffiRuntimeWebSocketAbortImpl runtime.handle webSocket.handle

@[inline] def newWebSocketPipe (runtime : Runtime) : IO (WebSocket × WebSocket) := do
  let (first, second) ← ffiRuntimeNewWebSocketPipeImpl runtime.handle
  return (
    { runtime := runtime, handle := first },
    { runtime := runtime, handle := second }
  )

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

namespace HttpResponsePromiseRef

@[inline] def await (promise : HttpResponsePromiseRef) : IO HttpResponse := do
  let (status, responseBody) ←
    ffiRuntimeHttpResponsePromiseAwaitImpl promise.runtime.handle promise.handle
  return { status := status, body := responseBody }

@[inline] def cancel (promise : HttpResponsePromiseRef) : IO Unit :=
  ffiRuntimeHttpResponsePromiseCancelImpl promise.runtime.handle promise.handle

@[inline] def release (promise : HttpResponsePromiseRef) : IO Unit :=
  ffiRuntimeHttpResponsePromiseReleaseImpl promise.runtime.handle promise.handle

@[inline] def awaitAndRelease (promise : HttpResponsePromiseRef) : IO HttpResponse := do
  promise.await

end HttpResponsePromiseRef

namespace WebSocketPromiseRef

@[inline] def await (promise : WebSocketPromiseRef) : IO WebSocket := do
  return {
    runtime := promise.runtime
    handle := (← ffiRuntimeWebSocketPromiseAwaitImpl promise.runtime.handle promise.handle)
  }

@[inline] def cancel (promise : WebSocketPromiseRef) : IO Unit :=
  ffiRuntimeWebSocketPromiseCancelImpl promise.runtime.handle promise.handle

@[inline] def release (promise : WebSocketPromiseRef) : IO Unit :=
  ffiRuntimeWebSocketPromiseReleaseImpl promise.runtime.handle promise.handle

@[inline] def awaitAndRelease (promise : WebSocketPromiseRef) : IO WebSocket := do
  promise.await

end WebSocketPromiseRef

namespace WebSocketMessagePromiseRef

@[inline] def await (promise : WebSocketMessagePromiseRef) : IO WebSocketMessage := do
  let (tag, closeCode, text, bytes) ←
    ffiRuntimeWebSocketMessagePromiseAwaitImpl promise.runtime.handle promise.handle
  decodeWebSocketMessage tag closeCode text bytes

@[inline] def cancel (promise : WebSocketMessagePromiseRef) : IO Unit :=
  ffiRuntimeWebSocketMessagePromiseCancelImpl promise.runtime.handle promise.handle

@[inline] def release (promise : WebSocketMessagePromiseRef) : IO Unit :=
  ffiRuntimeWebSocketMessagePromiseReleaseImpl promise.runtime.handle promise.handle

@[inline] def awaitAndRelease (promise : WebSocketMessagePromiseRef) : IO WebSocketMessage := do
  promise.await

end WebSocketMessagePromiseRef

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

@[inline] def writeStart (connection : Connection) (bytes : ByteArray) : IO PromiseRef := do
  return {
    runtime := connection.runtime
    handle := (← ffiRuntimeConnectionWriteStartImpl
      connection.runtime.handle connection.handle bytes)
  }

@[inline] def read (connection : Connection) (minBytes maxBytes : UInt32) : IO ByteArray :=
  ffiRuntimeConnectionReadImpl connection.runtime.handle connection.handle minBytes maxBytes

@[inline] def readStart (connection : Connection) (minBytes maxBytes : UInt32) :
    IO BytesPromiseRef := do
  return {
    runtime := connection.runtime
    handle := (← ffiRuntimeConnectionReadStartImpl
      connection.runtime.handle connection.handle minBytes maxBytes)
  }

@[inline] def shutdownWrite (connection : Connection) : IO Unit :=
  ffiRuntimeConnectionShutdownWriteImpl connection.runtime.handle connection.handle

@[inline] def shutdownWriteStart (connection : Connection) : IO PromiseRef := do
  return {
    runtime := connection.runtime
    handle := (← ffiRuntimeConnectionShutdownWriteStartImpl
      connection.runtime.handle connection.handle)
  }

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

namespace BytesPromiseRef

@[inline] def await (promise : BytesPromiseRef) : IO ByteArray :=
  ffiRuntimeBytesPromiseAwaitImpl promise.runtime.handle promise.handle

@[inline] def cancel (promise : BytesPromiseRef) : IO Unit :=
  ffiRuntimeBytesPromiseCancelImpl promise.runtime.handle promise.handle

@[inline] def release (promise : BytesPromiseRef) : IO Unit :=
  ffiRuntimeBytesPromiseReleaseImpl promise.runtime.handle promise.handle

@[inline] def awaitAndRelease (promise : BytesPromiseRef) : IO ByteArray := do
  promise.await

end BytesPromiseRef

namespace UInt32PromiseRef

@[inline] def await (promise : UInt32PromiseRef) : IO UInt32 :=
  ffiRuntimeUInt32PromiseAwaitImpl promise.runtime.handle promise.handle

@[inline] def cancel (promise : UInt32PromiseRef) : IO Unit :=
  ffiRuntimeUInt32PromiseCancelImpl promise.runtime.handle promise.handle

@[inline] def release (promise : UInt32PromiseRef) : IO Unit :=
  ffiRuntimeUInt32PromiseReleaseImpl promise.runtime.handle promise.handle

@[inline] def awaitAndRelease (promise : UInt32PromiseRef) : IO UInt32 := do
  promise.await

end UInt32PromiseRef

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

@[inline] def sendStart (port : DatagramPort) (address : String)
    (bytes : ByteArray) (portHint : UInt32 := 0) : IO UInt32PromiseRef := do
  return {
    runtime := port.runtime
    handle := (← ffiRuntimeDatagramSendStartImpl
      port.runtime.handle port.handle address portHint bytes)
  }

@[inline] def receive (port : DatagramPort)
    (maxBytes : UInt32 := UInt32.ofNat 8192) : IO (String × ByteArray) :=
  ffiRuntimeDatagramReceiveImpl port.runtime.handle port.handle maxBytes

@[inline] def receiveStart (port : DatagramPort)
    (maxBytes : UInt32 := UInt32.ofNat 8192) : IO DatagramReceivePromiseRef := do
  return {
    runtime := port.runtime
    handle := (← ffiRuntimeDatagramReceiveStartImpl port.runtime.handle port.handle maxBytes)
  }

@[inline] def withPort (port : DatagramPort) (action : DatagramPort -> IO α) : IO α := do
  try
    action port
  finally
    port.release

end DatagramPort

namespace DatagramReceivePromiseRef

@[inline] def await (promise : DatagramReceivePromiseRef) : IO (String × ByteArray) :=
  ffiRuntimeDatagramReceivePromiseAwaitImpl promise.runtime.handle promise.handle

@[inline] def cancel (promise : DatagramReceivePromiseRef) : IO Unit :=
  ffiRuntimeDatagramReceivePromiseCancelImpl promise.runtime.handle promise.handle

@[inline] def release (promise : DatagramReceivePromiseRef) : IO Unit :=
  ffiRuntimeDatagramReceivePromiseReleaseImpl promise.runtime.handle promise.handle

@[inline] def awaitAndRelease (promise : DatagramReceivePromiseRef) :
    IO (String × ByteArray) := do
  promise.await

end DatagramReceivePromiseRef

namespace WebSocket

@[inline] def release (webSocket : WebSocket) : IO Unit :=
  ffiRuntimeWebSocketReleaseImpl webSocket.runtime.handle webSocket.handle

@[inline] def sendTextStart (webSocket : WebSocket) (text : String) : IO PromiseRef := do
  return {
    runtime := webSocket.runtime
    handle := (← ffiRuntimeWebSocketSendTextStartImpl
      webSocket.runtime.handle webSocket.handle text)
  }

@[inline] def sendText (webSocket : WebSocket) (text : String) : IO Unit := do
  let promise ← webSocket.sendTextStart text
  promise.await

@[inline] def sendBinaryStart (webSocket : WebSocket) (bytes : ByteArray) : IO PromiseRef := do
  return {
    runtime := webSocket.runtime
    handle := (← ffiRuntimeWebSocketSendBinaryStartImpl
      webSocket.runtime.handle webSocket.handle bytes)
  }

@[inline] def sendBinary (webSocket : WebSocket) (bytes : ByteArray) : IO Unit := do
  let promise ← webSocket.sendBinaryStart bytes
  promise.await

@[inline] def receiveStart (webSocket : WebSocket) : IO WebSocketMessagePromiseRef := do
  return {
    runtime := webSocket.runtime
    handle := (← ffiRuntimeWebSocketReceiveStartImpl webSocket.runtime.handle webSocket.handle)
  }

@[inline] def receive (webSocket : WebSocket) : IO WebSocketMessage := do
  let promise ← webSocket.receiveStart
  let (tag, closeCode, text, bytes) ←
    ffiRuntimeWebSocketMessagePromiseAwaitImpl webSocket.runtime.handle promise.handle
  decodeWebSocketMessage tag closeCode text bytes

@[inline] def closeStart (webSocket : WebSocket) (code : UInt16)
    (reason : String := "") : IO PromiseRef := do
  return {
    runtime := webSocket.runtime
    handle := (← ffiRuntimeWebSocketCloseStartImpl webSocket.runtime.handle
      webSocket.handle code.toUInt32 reason)
  }

@[inline] def close (webSocket : WebSocket) (code : UInt16) (reason : String := "") : IO Unit := do
  let promise ← webSocket.closeStart code reason
  promise.await

@[inline] def disconnect (webSocket : WebSocket) : IO Unit :=
  ffiRuntimeWebSocketDisconnectImpl webSocket.runtime.handle webSocket.handle

@[inline] def abort (webSocket : WebSocket) : IO Unit :=
  ffiRuntimeWebSocketAbortImpl webSocket.runtime.handle webSocket.handle

end WebSocket

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

@[inline] def writeStart (connection : Connection) (bytes : ByteArray) : RuntimeM PromiseRef := do
  Runtime.connectionWriteStart (← runtime) connection bytes

@[inline] def read (connection : Connection) (minBytes maxBytes : UInt32) : RuntimeM ByteArray := do
  Runtime.connectionRead (← runtime) connection minBytes maxBytes

@[inline] def readStart (connection : Connection) (minBytes maxBytes : UInt32) :
    RuntimeM BytesPromiseRef := do
  Runtime.connectionReadStart (← runtime) connection minBytes maxBytes

@[inline] def shutdownWrite (connection : Connection) : RuntimeM Unit := do
  Runtime.connectionShutdownWrite (← runtime) connection

@[inline] def shutdownWriteStart (connection : Connection) : RuntimeM PromiseRef := do
  Runtime.connectionShutdownWriteStart (← runtime) connection

@[inline] def awaitConnection (promise : ConnectionPromiseRef) : RuntimeM Connection := do
  promise.await

@[inline] def cancelConnection (promise : ConnectionPromiseRef) : RuntimeM Unit := do
  promise.cancel

@[inline] def releaseConnectionPromise (promise : ConnectionPromiseRef) : RuntimeM Unit := do
  promise.release

@[inline] def awaitBytes (promise : BytesPromiseRef) : RuntimeM ByteArray := do
  promise.await

@[inline] def cancelBytes (promise : BytesPromiseRef) : RuntimeM Unit := do
  promise.cancel

@[inline] def releaseBytesPromise (promise : BytesPromiseRef) : RuntimeM Unit := do
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

@[inline] def datagramSendStart (port : DatagramPort) (address : String)
    (bytes : ByteArray) (portHint : UInt32 := 0) : RuntimeM UInt32PromiseRef := do
  Runtime.datagramSendStart (← runtime) port address bytes portHint

@[inline] def awaitUInt32 (promise : UInt32PromiseRef) : RuntimeM UInt32 := do
  promise.await

@[inline] def cancelUInt32 (promise : UInt32PromiseRef) : RuntimeM Unit := do
  promise.cancel

@[inline] def releaseUInt32Promise (promise : UInt32PromiseRef) : RuntimeM Unit := do
  promise.release

@[inline] def datagramReceive (port : DatagramPort)
    (maxBytes : UInt32 := UInt32.ofNat 8192) : RuntimeM (String × ByteArray) := do
  port.receive maxBytes

@[inline] def datagramReceiveStart (port : DatagramPort)
    (maxBytes : UInt32 := UInt32.ofNat 8192) : RuntimeM DatagramReceivePromiseRef := do
  Runtime.datagramReceiveStart (← runtime) port maxBytes

@[inline] def awaitDatagramReceive (promise : DatagramReceivePromiseRef) :
    RuntimeM (String × ByteArray) := do
  promise.await

@[inline] def cancelDatagramReceive (promise : DatagramReceivePromiseRef) : RuntimeM Unit := do
  promise.cancel

@[inline] def releaseDatagramReceivePromise (promise : DatagramReceivePromiseRef) :
    RuntimeM Unit := do
  promise.release

@[inline] def withDatagramPort (address : String)
    (action : DatagramPort -> RuntimeM α) (portHint : UInt32 := 0) : RuntimeM α := do
  let port ← datagramBind address portHint
  try
    action port
  finally
    port.release

@[inline] def httpRequest (method : HttpMethod) (address : String) (path : String)
    (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) : RuntimeM HttpResponse := do
  Runtime.httpRequest (← runtime) method address path body portHint

@[inline] def httpRequestStart (method : HttpMethod) (address : String) (path : String)
    (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) :
    RuntimeM HttpResponsePromiseRef := do
  Runtime.httpRequestStart (← runtime) method address path body portHint

@[inline] def awaitHttpResponse (promise : HttpResponsePromiseRef) : RuntimeM HttpResponse := do
  promise.await

@[inline] def cancelHttpResponse (promise : HttpResponsePromiseRef) : RuntimeM Unit := do
  promise.cancel

@[inline] def releaseHttpResponsePromise (promise : HttpResponsePromiseRef) : RuntimeM Unit := do
  promise.release

@[inline] def httpGet (address : String) (path : String) (portHint : UInt32 := 0) :
    RuntimeM HttpResponse := do
  Runtime.httpGet (← runtime) address path portHint

@[inline] def httpPost (address : String) (path : String) (body : ByteArray)
    (portHint : UInt32 := 0) : RuntimeM HttpResponse := do
  Runtime.httpPost (← runtime) address path body portHint

@[inline] def webSocketConnect (address : String) (path : String) (portHint : UInt32 := 0) :
    RuntimeM WebSocket := do
  Runtime.webSocketConnect (← runtime) address path portHint

@[inline] def webSocketConnectStart (address : String) (path : String)
    (portHint : UInt32 := 0) : RuntimeM WebSocketPromiseRef := do
  Runtime.webSocketConnectStart (← runtime) address path portHint

@[inline] def awaitWebSocket (promise : WebSocketPromiseRef) : RuntimeM WebSocket := do
  promise.await

@[inline] def cancelWebSocket (promise : WebSocketPromiseRef) : RuntimeM Unit := do
  promise.cancel

@[inline] def releaseWebSocketPromise (promise : WebSocketPromiseRef) : RuntimeM Unit := do
  promise.release

@[inline] def webSocketRelease (webSocket : WebSocket) : RuntimeM Unit := do
  webSocket.release

@[inline] def webSocketSendTextStart (webSocket : WebSocket) (text : String) :
    RuntimeM PromiseRef := do
  Runtime.webSocketSendTextStart (← runtime) webSocket text

@[inline] def webSocketSendText (webSocket : WebSocket) (text : String) : RuntimeM Unit := do
  webSocket.sendText text

@[inline] def webSocketSendBinaryStart (webSocket : WebSocket) (bytes : ByteArray) :
    RuntimeM PromiseRef := do
  Runtime.webSocketSendBinaryStart (← runtime) webSocket bytes

@[inline] def webSocketSendBinary (webSocket : WebSocket) (bytes : ByteArray) : RuntimeM Unit := do
  webSocket.sendBinary bytes

@[inline] def webSocketReceiveStart (webSocket : WebSocket) :
    RuntimeM WebSocketMessagePromiseRef := do
  Runtime.webSocketReceiveStart (← runtime) webSocket

@[inline] def awaitWebSocketMessage (promise : WebSocketMessagePromiseRef) :
    RuntimeM WebSocketMessage := do
  promise.await

@[inline] def cancelWebSocketMessage (promise : WebSocketMessagePromiseRef) :
    RuntimeM Unit := do
  promise.cancel

@[inline] def releaseWebSocketMessagePromise (promise : WebSocketMessagePromiseRef) :
    RuntimeM Unit := do
  promise.release

@[inline] def webSocketReceive (webSocket : WebSocket) : RuntimeM WebSocketMessage := do
  webSocket.receive

@[inline] def webSocketCloseStart (webSocket : WebSocket) (code : UInt16)
    (reason : String := "") : RuntimeM PromiseRef := do
  Runtime.webSocketCloseStart (← runtime) webSocket code reason

@[inline] def webSocketClose (webSocket : WebSocket) (code : UInt16)
    (reason : String := "") : RuntimeM Unit := do
  webSocket.close code reason

@[inline] def webSocketDisconnect (webSocket : WebSocket) : RuntimeM Unit := do
  webSocket.disconnect

@[inline] def webSocketAbort (webSocket : WebSocket) : RuntimeM Unit := do
  webSocket.abort

@[inline] def newWebSocketPipe : RuntimeM (WebSocket × WebSocket) := do
  Runtime.newWebSocketPipe (← runtime)

end RuntimeM

end KjAsync
end Capnp
