import Capnp.Async
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

structure DatagramPeer where
  port : DatagramPort
  remoteAddress : String
  remotePort : UInt32
  deriving Inhabited, BEq, Repr

structure DatagramReceivePromiseRef where
  runtime : Runtime
  handle : UInt32
  deriving Inhabited, BEq, Repr

structure HttpHeader where
  name : String
  value : String
  deriving Inhabited, BEq, Repr

inductive TlsVersion where
  | ssl3
  | tls10
  | tls11
  | tls12
  | tls13
  deriving Inhabited, BEq, Repr

structure TlsConfig where
  useSystemTrustStore : Bool := true
  verifyClients : Bool := false
  minVersion : Option TlsVersion := none
  trustedCertificatesPem : String := ""
  certificateChainPem : String := ""
  privateKeyPem : String := ""
  cipherList : String := ""
  curveList : String := ""
  acceptTimeoutNanos : UInt64 := 0
  deriving Inhabited, BEq, Repr

structure HttpResponse where
  status : UInt32
  body : ByteArray
  deriving Inhabited, BEq

structure HttpResponseEx where
  status : UInt32
  statusText : String
  headers : Array HttpHeader
  body : ByteArray
  deriving Inhabited, BEq

structure HttpResponsePromiseRef where
  runtime : Runtime
  handle : UInt32
  deriving Inhabited, BEq, Repr

structure HttpRequestBody where
  runtime : Runtime
  handle : UInt32
  deriving Inhabited, BEq, Repr

structure HttpResponseBody where
  runtime : Runtime
  handle : UInt32
  deriving Inhabited, BEq, Repr

structure HttpServerRequestBody where
  runtime : Runtime
  handle : UInt32
  deriving Inhabited, BEq, Repr

structure HttpServerResponseBody where
  runtime : Runtime
  handle : UInt32
  deriving Inhabited, BEq, Repr

structure HttpResponseStreaming where
  status : UInt32
  statusText : String
  headers : Array HttpHeader
  body : HttpResponseBody
  deriving Inhabited, BEq, Repr

structure HttpServer where
  runtime : Runtime
  handle : UInt32
  boundPort : UInt32
  deriving Inhabited, BEq, Repr

inductive HttpWebSocketCompressionMode where
  | none
  | manual
  | automatic
  deriving Inhabited, BEq, Repr

structure HttpServerConfig where
  headerTimeoutNanos : UInt64 := 15000000000
  pipelineTimeoutNanos : UInt64 := 5000000000
  canceledUploadGracePeriodNanos : UInt64 := 1000000000
  canceledUploadGraceBytes : UInt64 := 65536
  webSocketCompressionMode : HttpWebSocketCompressionMode := .none
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

@[extern "capnp_lean_kj_async_runtime_enable_tls"]
opaque ffiRuntimeEnableTlsImpl (runtime : UInt64) : IO Unit

@[extern "capnp_lean_kj_async_runtime_configure_tls"]
opaque ffiRuntimeConfigureTlsImpl
    (runtime : UInt64) (useSystemTrustStore : UInt32) (verifyClients : UInt32)
    (minVersionTag : UInt32)
    (trustedCertificatesPem : @& String) (certificateChainPem : @& String)
    (privateKeyPem : @& String) (cipherList : @& String) (curveList : @& String)
    (acceptTimeoutNanos : UInt64) : IO Unit

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

@[extern "capnp_lean_kj_async_runtime_connection_promise_await_with_timeout"]
opaque ffiRuntimeConnectionPromiseAwaitWithTimeoutImpl
    (runtime : UInt64) (promise : UInt32) (timeoutNanos : UInt64) : IO (Bool × UInt32)

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

@[extern "capnp_lean_kj_async_runtime_promise_then_start"]
opaque ffiRuntimePromiseThenStartImpl
    (runtime : UInt64) (firstPromise : UInt32) (secondPromise : UInt32) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_promise_catch_start"]
opaque ffiRuntimePromiseCatchStartImpl
    (runtime : UInt64) (promise : UInt32) (fallbackPromise : UInt32) : IO UInt32

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

@[extern "capnp_lean_kj_async_runtime_connection_dup_fd"]
opaque ffiRuntimeConnectionDupFdImpl (runtime : UInt64) (connection : UInt32) : IO (Bool × UInt32)

@[extern "capnp_lean_kj_async_runtime_new_two_way_pipe"]
opaque ffiRuntimeNewTwoWayPipeImpl (runtime : UInt64) : IO (UInt32 × UInt32)

@[extern "capnp_lean_kj_async_runtime_new_capability_pipe"]
opaque ffiRuntimeNewCapabilityPipeImpl (runtime : UInt64) : IO (UInt32 × UInt32)

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

@[extern "capnp_lean_kj_async_runtime_http_request_with_response_limit"]
opaque ffiRuntimeHttpRequestWithResponseLimitImpl
    (runtime : UInt64) (method : UInt32) (address : @& String) (portHint : UInt32)
    (path : @& String) (body : @& ByteArray) (responseBodyLimit : UInt64) :
    IO (UInt32 × ByteArray)

@[extern "capnp_lean_kj_async_runtime_http_request_start"]
opaque ffiRuntimeHttpRequestStartImpl
    (runtime : UInt64) (method : UInt32) (address : @& String) (portHint : UInt32)
    (path : @& String) (body : @& ByteArray) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_http_request_start_with_response_limit"]
opaque ffiRuntimeHttpRequestStartWithResponseLimitImpl
    (runtime : UInt64) (method : UInt32) (address : @& String) (portHint : UInt32)
    (path : @& String) (body : @& ByteArray) (responseBodyLimit : UInt64) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_http_response_promise_await"]
opaque ffiRuntimeHttpResponsePromiseAwaitImpl
    (runtime : UInt64) (promise : UInt32) : IO (UInt32 × ByteArray)

@[extern "capnp_lean_kj_async_runtime_http_response_promise_cancel"]
opaque ffiRuntimeHttpResponsePromiseCancelImpl (runtime : UInt64) (promise : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_http_response_promise_release"]
opaque ffiRuntimeHttpResponsePromiseReleaseImpl (runtime : UInt64) (promise : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_http_request_with_headers"]
opaque ffiRuntimeHttpRequestWithHeadersImpl
    (runtime : UInt64) (method : UInt32) (address : @& String) (portHint : UInt32)
    (path : @& String) (requestHeaders : @& ByteArray) (body : @& ByteArray) :
    IO (UInt32 × String × ByteArray × ByteArray)

@[extern "capnp_lean_kj_async_runtime_http_request_with_headers_secure"]
opaque ffiRuntimeHttpRequestWithHeadersSecureImpl
    (runtime : UInt64) (method : UInt32) (address : @& String) (portHint : UInt32)
    (path : @& String) (requestHeaders : @& ByteArray) (body : @& ByteArray) :
    IO (UInt32 × String × ByteArray × ByteArray)

@[extern "capnp_lean_kj_async_runtime_http_request_with_headers_with_response_limit"]
opaque ffiRuntimeHttpRequestWithHeadersWithResponseLimitImpl
    (runtime : UInt64) (method : UInt32) (address : @& String) (portHint : UInt32)
    (path : @& String) (requestHeaders : @& ByteArray) (body : @& ByteArray)
    (responseBodyLimit : UInt64) : IO (UInt32 × String × ByteArray × ByteArray)

@[extern "capnp_lean_kj_async_runtime_http_request_with_headers_with_response_limit_secure"]
opaque ffiRuntimeHttpRequestWithHeadersWithResponseLimitSecureImpl
    (runtime : UInt64) (method : UInt32) (address : @& String) (portHint : UInt32)
    (path : @& String) (requestHeaders : @& ByteArray) (body : @& ByteArray)
    (responseBodyLimit : UInt64) : IO (UInt32 × String × ByteArray × ByteArray)

@[extern "capnp_lean_kj_async_runtime_http_request_start_with_headers"]
opaque ffiRuntimeHttpRequestStartWithHeadersImpl
    (runtime : UInt64) (method : UInt32) (address : @& String) (portHint : UInt32)
    (path : @& String) (requestHeaders : @& ByteArray) (body : @& ByteArray) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_http_request_start_with_headers_secure"]
opaque ffiRuntimeHttpRequestStartWithHeadersSecureImpl
    (runtime : UInt64) (method : UInt32) (address : @& String) (portHint : UInt32)
    (path : @& String) (requestHeaders : @& ByteArray) (body : @& ByteArray) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_http_request_start_streaming_with_headers"]
opaque ffiRuntimeHttpRequestStartStreamingWithHeadersImpl
    (runtime : UInt64) (method : UInt32) (address : @& String) (portHint : UInt32)
    (path : @& String) (requestHeaders : @& ByteArray) : IO (UInt32 × UInt32)

@[extern "capnp_lean_kj_async_runtime_http_request_start_streaming_with_headers_secure"]
opaque ffiRuntimeHttpRequestStartStreamingWithHeadersSecureImpl
    (runtime : UInt64) (method : UInt32) (address : @& String) (portHint : UInt32)
    (path : @& String) (requestHeaders : @& ByteArray) : IO (UInt32 × UInt32)

@[extern "capnp_lean_kj_async_runtime_http_request_start_with_headers_with_response_limit"]
opaque ffiRuntimeHttpRequestStartWithHeadersWithResponseLimitImpl
    (runtime : UInt64) (method : UInt32) (address : @& String) (portHint : UInt32)
    (path : @& String) (requestHeaders : @& ByteArray) (body : @& ByteArray)
    (responseBodyLimit : UInt64) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_http_request_start_with_headers_with_response_limit_secure"]
opaque ffiRuntimeHttpRequestStartWithHeadersWithResponseLimitSecureImpl
    (runtime : UInt64) (method : UInt32) (address : @& String) (portHint : UInt32)
    (path : @& String) (requestHeaders : @& ByteArray) (body : @& ByteArray)
    (responseBodyLimit : UInt64) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_http_response_promise_await_with_headers"]
opaque ffiRuntimeHttpResponsePromiseAwaitWithHeadersImpl
    (runtime : UInt64) (promise : UInt32) : IO (UInt32 × String × ByteArray × ByteArray)

@[extern "capnp_lean_kj_async_runtime_http_response_promise_await_streaming_with_headers"]
opaque ffiRuntimeHttpResponsePromiseAwaitStreamingWithHeadersImpl
    (runtime : UInt64) (promise : UInt32) : IO (UInt32 × String × ByteArray × UInt32)

@[extern "capnp_lean_kj_async_runtime_http_request_body_write_start"]
opaque ffiRuntimeHttpRequestBodyWriteStartImpl
    (runtime : UInt64) (requestBody : UInt32) (bytes : @& ByteArray) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_http_request_body_write"]
opaque ffiRuntimeHttpRequestBodyWriteImpl
    (runtime : UInt64) (requestBody : UInt32) (bytes : @& ByteArray) : IO Unit

@[extern "capnp_lean_kj_async_runtime_http_request_body_finish_start"]
opaque ffiRuntimeHttpRequestBodyFinishStartImpl
    (runtime : UInt64) (requestBody : UInt32) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_http_request_body_finish"]
opaque ffiRuntimeHttpRequestBodyFinishImpl
    (runtime : UInt64) (requestBody : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_http_request_body_release"]
opaque ffiRuntimeHttpRequestBodyReleaseImpl
    (runtime : UInt64) (requestBody : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_http_response_body_read_start"]
opaque ffiRuntimeHttpResponseBodyReadStartImpl
    (runtime : UInt64) (responseBody : UInt32) (minBytes : UInt32) (maxBytes : UInt32) :
    IO UInt32

@[extern "capnp_lean_kj_async_runtime_http_response_body_read"]
opaque ffiRuntimeHttpResponseBodyReadImpl
    (runtime : UInt64) (responseBody : UInt32) (minBytes : UInt32) (maxBytes : UInt32) :
    IO ByteArray

@[extern "capnp_lean_kj_async_runtime_http_response_body_release"]
opaque ffiRuntimeHttpResponseBodyReleaseImpl
    (runtime : UInt64) (responseBody : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_http_server_request_body_read_start"]
opaque ffiRuntimeHttpServerRequestBodyReadStartImpl
    (runtime : UInt64) (requestBody : UInt32) (minBytes : UInt32) (maxBytes : UInt32) :
    IO UInt32

@[extern "capnp_lean_kj_async_runtime_http_server_request_body_read"]
opaque ffiRuntimeHttpServerRequestBodyReadImpl
    (runtime : UInt64) (requestBody : UInt32) (minBytes : UInt32) (maxBytes : UInt32) :
    IO ByteArray

@[extern "capnp_lean_kj_async_runtime_http_server_request_body_release"]
opaque ffiRuntimeHttpServerRequestBodyReleaseImpl
    (runtime : UInt64) (requestBody : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_http_server_listen"]
opaque ffiRuntimeHttpServerListenImpl
    (runtime : UInt64) (address : @& String) (portHint : UInt32) : IO (UInt32 × UInt32)

@[extern "capnp_lean_kj_async_runtime_http_server_listen_secure"]
opaque ffiRuntimeHttpServerListenSecureImpl
    (runtime : UInt64) (address : @& String) (portHint : UInt32) : IO (UInt32 × UInt32)

@[extern "capnp_lean_kj_async_runtime_http_server_listen_with_config"]
opaque ffiRuntimeHttpServerListenWithConfigImpl
    (runtime : UInt64) (address : @& String) (portHint : UInt32)
    (headerTimeoutNanos : UInt64) (pipelineTimeoutNanos : UInt64)
    (canceledUploadGracePeriodNanos : UInt64) (canceledUploadGraceBytes : UInt64)
    (webSocketCompressionMode : UInt32) : IO (UInt32 × UInt32)

@[extern "capnp_lean_kj_async_runtime_http_server_listen_secure_with_config"]
opaque ffiRuntimeHttpServerListenSecureWithConfigImpl
    (runtime : UInt64) (address : @& String) (portHint : UInt32)
    (headerTimeoutNanos : UInt64) (pipelineTimeoutNanos : UInt64)
    (canceledUploadGracePeriodNanos : UInt64) (canceledUploadGraceBytes : UInt64)
    (webSocketCompressionMode : UInt32) : IO (UInt32 × UInt32)

@[extern "capnp_lean_kj_async_runtime_http_server_release"]
opaque ffiRuntimeHttpServerReleaseImpl (runtime : UInt64) (server : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_http_server_drain_start"]
opaque ffiRuntimeHttpServerDrainStartImpl (runtime : UInt64) (server : UInt32) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_http_server_drain"]
opaque ffiRuntimeHttpServerDrainImpl (runtime : UInt64) (server : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_http_server_poll_request"]
opaque ffiRuntimeHttpServerPollRequestImpl
    (runtime : UInt64) (server : UInt32) : IO (Bool × ByteArray)

@[extern "capnp_lean_kj_async_runtime_http_server_respond"]
opaque ffiRuntimeHttpServerRespondImpl
    (runtime : UInt64) (server : UInt32) (requestId : UInt32) (status : UInt32)
    (statusText : @& String) (responseHeaders : @& ByteArray) (body : @& ByteArray) : IO Unit

@[extern "capnp_lean_kj_async_runtime_http_server_respond_websocket"]
opaque ffiRuntimeHttpServerRespondWebSocketImpl
    (runtime : UInt64) (server : UInt32) (requestId : UInt32) (responseHeaders : @& ByteArray) :
    IO UInt32

@[extern "capnp_lean_kj_async_runtime_http_server_respond_start_streaming"]
opaque ffiRuntimeHttpServerRespondStartStreamingImpl
    (runtime : UInt64) (server : UInt32) (requestId : UInt32) (status : UInt32)
    (statusText : @& String) (responseHeaders : @& ByteArray) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_http_server_response_body_write_start"]
opaque ffiRuntimeHttpServerResponseBodyWriteStartImpl
    (runtime : UInt64) (responseBody : UInt32) (bytes : @& ByteArray) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_http_server_response_body_write"]
opaque ffiRuntimeHttpServerResponseBodyWriteImpl
    (runtime : UInt64) (responseBody : UInt32) (bytes : @& ByteArray) : IO Unit

@[extern "capnp_lean_kj_async_runtime_http_server_response_body_finish_start"]
opaque ffiRuntimeHttpServerResponseBodyFinishStartImpl
    (runtime : UInt64) (responseBody : UInt32) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_http_server_response_body_finish"]
opaque ffiRuntimeHttpServerResponseBodyFinishImpl
    (runtime : UInt64) (responseBody : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_http_server_response_body_release"]
opaque ffiRuntimeHttpServerResponseBodyReleaseImpl
    (runtime : UInt64) (responseBody : UInt32) : IO Unit

@[extern "capnp_lean_kj_async_runtime_websocket_connect"]
opaque ffiRuntimeWebSocketConnectImpl
    (runtime : UInt64) (address : @& String) (portHint : UInt32) (path : @& String) :
    IO UInt32

@[extern "capnp_lean_kj_async_runtime_websocket_connect_start"]
opaque ffiRuntimeWebSocketConnectStartImpl
    (runtime : UInt64) (address : @& String) (portHint : UInt32) (path : @& String) :
    IO UInt32

@[extern "capnp_lean_kj_async_runtime_websocket_connect_with_headers"]
opaque ffiRuntimeWebSocketConnectWithHeadersImpl
    (runtime : UInt64) (address : @& String) (portHint : UInt32) (path : @& String)
    (requestHeaders : @& ByteArray) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_websocket_connect_with_headers_secure"]
opaque ffiRuntimeWebSocketConnectWithHeadersSecureImpl
    (runtime : UInt64) (address : @& String) (portHint : UInt32) (path : @& String)
    (requestHeaders : @& ByteArray) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_websocket_connect_start_with_headers"]
opaque ffiRuntimeWebSocketConnectStartWithHeadersImpl
    (runtime : UInt64) (address : @& String) (portHint : UInt32) (path : @& String)
    (requestHeaders : @& ByteArray) : IO UInt32

@[extern "capnp_lean_kj_async_runtime_websocket_connect_start_with_headers_secure"]
opaque ffiRuntimeWebSocketConnectStartWithHeadersSecureImpl
    (runtime : UInt64) (address : @& String) (portHint : UInt32) (path : @& String)
    (requestHeaders : @& ByteArray) : IO UInt32

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

@[extern "capnp_lean_kj_async_runtime_websocket_receive_start_with_max"]
opaque ffiRuntimeWebSocketReceiveStartWithMaxImpl
    (runtime : UInt64) (webSocket : UInt32) (maxBytes : UInt32) : IO UInt32

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

@[extern "capnp_lean_kj_async_runtime_websocket_receive_with_max"]
opaque ffiRuntimeWebSocketReceiveWithMaxImpl
    (runtime : UInt64) (webSocket : UInt32) (maxBytes : UInt32) :
    IO (UInt32 × UInt32 × String × ByteArray)

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

@[inline] private def boolToUInt32 (value : Bool) : UInt32 :=
  if value then 1 else 0

@[inline] private def tlsVersionToTag (version : Option TlsVersion) : UInt32 :=
  match version with
  | none => 0
  | some .ssl3 => 1
  | some .tls10 => 2
  | some .tls11 => 3
  | some .tls12 => 4
  | some .tls13 => 5

@[inline] private def webSocketCompressionModeToTag (mode : HttpWebSocketCompressionMode) :
    UInt32 :=
  match mode with
  | .none => 0
  | .manual => 1
  | .automatic => 2

@[inline] private def ensureSameRuntime (runtime : Runtime) (owner : Runtime)
    (resource : String) : IO Unit := do
  if runtime.handle != owner.handle then
    throw (IO.userError
      s!"{resource} belongs to a different Capnp.KjAsync runtime")

@[inline] private def appendUInt32Le (bytes : ByteArray) (value : UInt32) : ByteArray :=
  bytes.push value.toUInt8
    |>.push ((value >>> 8).toUInt8)
    |>.push ((value >>> 16).toUInt8)
    |>.push ((value >>> 24).toUInt8)

@[inline] private def encodePromiseHandlesForRuntime (runtime : Runtime)
    (promises : Array PromiseRef) : IO ByteArray := do
  let mut out := ByteArray.emptyWithCapacity (promises.size * 4)
  for promise in promises do
    ensureSameRuntime runtime promise.runtime "PromiseRef"
    out := appendUInt32Le out promise.handle
  pure out

@[inline] private def appendBytes (dst src : ByteArray) : ByteArray :=
  Id.run do
    let mut out := dst
    for b in src do
      out := out.push b
    pure out

@[inline] private def decodeUInt32Le? (bytes : ByteArray) (offset : Nat) :
    Option (UInt32 × Nat) :=
  if offset + 3 < bytes.size then
    let b0 := (bytes.get! offset).toUInt32
    let b1 := ((bytes.get! (offset + 1)).toUInt32) <<< 8
    let b2 := ((bytes.get! (offset + 2)).toUInt32) <<< 16
    let b3 := ((bytes.get! (offset + 3)).toUInt32) <<< 24
    some (b0 ||| b1 ||| b2 ||| b3, offset + 4)
  else
    none

@[inline] private def decodeUtf8At (bytes : ByteArray) (offset length : Nat) : IO String := do
  if offset + length ≤ bytes.size then
    let slice := bytes.extract offset (offset + length)
    match String.fromUTF8? slice with
    | some s => pure s
    | none => throw (IO.userError "invalid UTF-8 in KJ async payload")
  else
    throw (IO.userError "truncated KJ async payload")

@[inline] private def encodeHeaders (headers : Array HttpHeader) : ByteArray :=
  Id.run do
    let mut out := ByteArray.emptyWithCapacity 16
    out := appendUInt32Le out headers.size.toUInt32
    for header in headers do
      let nameBytes := header.name.toUTF8
      let valueBytes := header.value.toUTF8
      out := appendUInt32Le out nameBytes.size.toUInt32
      out := appendBytes out nameBytes
      out := appendUInt32Le out valueBytes.size.toUInt32
      out := appendBytes out valueBytes
    pure out

@[inline] private def decodeHeaders (bytes : ByteArray) : IO (Array HttpHeader) := do
  let some (count, offset0) := decodeUInt32Le? bytes 0
    | throw (IO.userError "invalid header list payload")
  let mut offset := offset0
  let mut headers : Array HttpHeader := #[]
  let mut remaining := count
  while remaining != 0 do
    let some (nameLen, nextOffset) := decodeUInt32Le? bytes offset
      | throw (IO.userError "invalid header list payload")
    let nameLenNat := nameLen.toNat
    let name ← decodeUtf8At bytes nextOffset nameLenNat
    offset := nextOffset + nameLenNat
    let some (valueLen, nextOffset2) := decodeUInt32Le? bytes offset
      | throw (IO.userError "invalid header list payload")
    let valueLenNat := valueLen.toNat
    let value ← decodeUtf8At bytes nextOffset2 valueLenNat
    offset := nextOffset2 + valueLenNat
    headers := headers.push { name := name, value := value }
    remaining := remaining - 1
  if offset == bytes.size then
    pure headers
  else
    throw (IO.userError "invalid header list payload: trailing bytes")

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
  | .get => 0
  | .head => 1
  | .post => 2
  | .put => 3
  | .delete => 4
  | .patch => 5
  | .options => 6
  | .trace => 7

@[inline] private def httpMethodFromTag (tag : UInt32) : IO HttpMethod := do
  if tag == 0 then
    return .get
  else if tag == 1 then
    return .head
  else if tag == 2 then
    return .post
  else if tag == 3 then
    return .put
  else if tag == 4 then
    return .delete
  else if tag == 5 then
    return .patch
  else if tag == 6 then
    return .options
  else if tag == 7 then
    return .trace
  else
    throw (IO.userError s!"unknown HTTP method tag: {tag}")

@[inline] private def decodeWebSocketMessage
    (tag : UInt32) (closeCode : UInt32) (text : String) (bytes : ByteArray) :
    IO WebSocketMessage := do
  if tag == 0 then
    return .text text
  else if tag == 1 then
    return .binary bytes
  else if tag == 2 then
    return .close closeCode.toUInt16 text
  else
    throw (IO.userError s!"unknown websocket message tag: {tag}")

structure HttpServerRequest where
  requestId : UInt32
  method : HttpMethod
  webSocketRequested : Bool
  path : String
  headers : Array HttpHeader
  body : ByteArray
  bodyStream? : Option HttpServerRequestBody := none
  deriving Inhabited, BEq

structure Endpoint where
  address : String
  portHint : UInt32 := 0
  deriving Inhabited, BEq, Repr

namespace Endpoint

@[inline] def tcp (address : String) (port : UInt32) : Endpoint :=
  { address := address, portHint := port }

@[inline] def unix (path : String) : Endpoint :=
  { address := s!"unix:{path}", portHint := 0 }

end Endpoint

structure HttpEndpoint where
  host : String
  port : UInt32 := 0
  deriving Inhabited, BEq, Repr

@[inline] private def decodeHttpServerRequest (runtime : Runtime) (bytes : ByteArray) :
    IO HttpServerRequest := do
  let some (requestId, offset1) := decodeUInt32Le? bytes 0
    | throw (IO.userError "invalid HTTP server request payload")
  let some (methodTag, offset2) := decodeUInt32Le? bytes offset1
    | throw (IO.userError "invalid HTTP server request payload")
  let some (webSocketTag, offset3) := decodeUInt32Le? bytes offset2
    | throw (IO.userError "invalid HTTP server request payload")
  let some (pathLen, offset4) := decodeUInt32Le? bytes offset3
    | throw (IO.userError "invalid HTTP server request payload")
  let pathLenNat := pathLen.toNat
  let path ← decodeUtf8At bytes offset4 pathLenNat
  let offset5 := offset4 + pathLenNat
  let some (headersLen, offset6) := decodeUInt32Le? bytes offset5
    | throw (IO.userError "invalid HTTP server request payload")
  let headersLenNat := headersLen.toNat
  if offset6 + headersLenNat ≤ bytes.size then
    let headerBytes := bytes.extract offset6 (offset6 + headersLenNat)
    let headers ← decodeHeaders headerBytes
    let offset7 := offset6 + headersLenNat
    let some (bodyLen, offset8) := decodeUInt32Le? bytes offset7
      | throw (IO.userError "invalid HTTP server request payload")
    let bodyLenNat := bodyLen.toNat
    if offset8 + bodyLenNat ≤ bytes.size then
      let body := bytes.extract offset8 (offset8 + bodyLenNat)
      let offset9 := offset8 + bodyLenNat
      let bodyStream? ←
        if offset9 == bytes.size then
          pure none
        else
          let some (bodyHandle, offset10) := decodeUInt32Le? bytes offset9
            | throw (IO.userError "invalid HTTP server request payload")
          if offset10 == bytes.size then
            if bodyHandle == 0 then
              pure none
            else
              pure (some { runtime := runtime, handle := bodyHandle })
          else
            throw (IO.userError "invalid HTTP server request payload: trailing bytes")
      return {
        requestId := requestId
        method := (← httpMethodFromTag methodTag)
        webSocketRequested := (webSocketTag != 0)
        path := path
        headers := headers
        body := body
        bodyStream? := bodyStream?
      }
    else
      throw (IO.userError "invalid HTTP server request payload")
  else
    throw (IO.userError "invalid HTTP server request payload")

@[inline] private def drainHttpServerRequestBody (request : HttpServerRequest) :
    IO HttpServerRequest := do
  match request.bodyStream? with
  | none =>
    pure request
  | some requestBody =>
    try
      let mut body := request.body
      let mut done := false
      while !done do
        let chunk ← ffiRuntimeHttpServerRequestBodyReadImpl requestBody.runtime.handle
          requestBody.handle 1 0x1000
        if chunk.size == 0 then
          done := true
        else
          body := appendBytes body chunk
      pure { request with body := body, bodyStream? := none }
    finally
      ffiRuntimeHttpServerRequestBodyReleaseImpl requestBody.runtime.handle requestBody.handle

namespace Runtime

@[inline] def init : IO Runtime := do
  return { handle := (← ffiRuntimeNewImpl) }

@[inline] def shutdown (runtime : Runtime) : IO Unit :=
  ffiRuntimeReleaseImpl runtime.handle

@[inline] def isAlive (runtime : Runtime) : IO Bool :=
  ffiRuntimeIsAliveImpl runtime.handle

@[inline] def enableTls (runtime : Runtime) : IO Unit :=
  ffiRuntimeEnableTlsImpl runtime.handle

@[inline] def configureTls (runtime : Runtime) (config : TlsConfig) : IO Unit := do
  if config.certificateChainPem.isEmpty != config.privateKeyPem.isEmpty then
    throw (IO.userError
      "TLS configuration requires both certificateChainPem and privateKeyPem")
  ffiRuntimeConfigureTlsImpl runtime.handle (boolToUInt32 config.useSystemTrustStore)
    (boolToUInt32 config.verifyClients) (tlsVersionToTag config.minVersion)
    config.trustedCertificatesPem config.certificateChainPem config.privateKeyPem
    config.cipherList config.curveList config.acceptTimeoutNanos

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

private def connectionPromiseCancelAndReleaseBestEffort (runtime : Runtime)
    (promiseHandle : UInt32) : IO Unit := do
  try
    ffiRuntimeConnectionPromiseCancelImpl runtime.handle promiseHandle
  catch _ =>
    pure ()
  try
    ffiRuntimeConnectionPromiseReleaseImpl runtime.handle promiseHandle
  catch _ =>
    pure ()

private def connectionPromiseAwait (pending : ConnectionPromiseRef) : IO Connection := do
  try
    return {
      runtime := pending.runtime
      handle := (← ffiRuntimeConnectionPromiseAwaitImpl pending.runtime.handle pending.handle)
    }
  catch err =>
    connectionPromiseCancelAndReleaseBestEffort pending.runtime pending.handle
    throw err

private def connectionPromiseAwaitWithTimeoutNanos? (pending : ConnectionPromiseRef)
    (timeoutNanos : UInt64) : IO (Option Connection) := do
  try
    let (hasValue, handle) ←
      ffiRuntimeConnectionPromiseAwaitWithTimeoutImpl
        pending.runtime.handle pending.handle timeoutNanos
    if hasValue then
      return some { runtime := pending.runtime, handle := handle }
    else
      return none
  catch err =>
    connectionPromiseCancelAndReleaseBestEffort pending.runtime pending.handle
    throw err

private partial def connectWithRetryLoop (runtime : Runtime) (address : String)
    (remaining : UInt32) (retryDelayMs : UInt32) (portHint : UInt32)
    (lastErr? : Option IO.Error := none) : IO Connection := do
  if remaining == 0 then
    match lastErr? with
    | some err =>
      throw err
    | none =>
      throw (IO.userError "Runtime.connectWithRetry exhausted attempts")
  try
    connectionPromiseAwait (← runtime.connectStart address portHint)
  catch err =>
    let nextRemaining := remaining - 1
    if nextRemaining > 0 && retryDelayMs > 0 then
      runtime.sleepMillis retryDelayMs
    connectWithRetryLoop runtime address nextRemaining retryDelayMs portHint (some err)

@[inline] def connectAsTask (runtime : Runtime) (address : String) (portHint : UInt32 := 0) :
    IO (Task (Except IO.Error Connection)) := do
  let pending ← runtime.connectStart address portHint
  IO.asTask (connectionPromiseAwait pending)

@[inline] def connectAsPromise (runtime : Runtime) (address : String) (portHint : UInt32 := 0) :
    IO (Capnp.Async.Promise Connection) := do
  let task ← runtime.connectAsTask address portHint
  pure (Capnp.Async.Promise.ofTask task)

@[inline] def connectWithTimeoutNanos? (runtime : Runtime) (address : String)
    (timeoutNanos : UInt64) (portHint : UInt32 := 0) : IO (Option Connection) := do
  let pending ← runtime.connectStart address portHint
  connectionPromiseAwaitWithTimeoutNanos? pending timeoutNanos

@[inline] def connectWithTimeoutMillis? (runtime : Runtime) (address : String)
    (timeoutMillis : UInt32) (portHint : UInt32 := 0) : IO (Option Connection) := do
  runtime.connectWithTimeoutNanos? address (millisToNanos timeoutMillis) portHint

@[inline] def connectWithRetry (runtime : Runtime) (address : String)
    (attempts : UInt32) (retryDelayMs : UInt32) (portHint : UInt32 := 0) : IO Connection := do
  if attempts == 0 then
    throw (IO.userError "Runtime.connectWithRetry requires attempts > 0")
  connectWithRetryLoop runtime address attempts retryDelayMs portHint

@[inline] def connectWithRetryAsTask (runtime : Runtime) (address : String) (attempts : UInt32)
    (retryDelayMs : UInt32) (portHint : UInt32 := 0) :
    IO (Task (Except IO.Error Connection)) :=
  IO.asTask (runtime.connectWithRetry address attempts retryDelayMs portHint)

@[inline] def connectWithRetryAsPromise (runtime : Runtime) (address : String) (attempts : UInt32)
    (retryDelayMs : UInt32) (portHint : UInt32 := 0) :
    IO (Capnp.Async.Promise Connection) := do
  pure (Capnp.Async.Promise.ofTask
    (← runtime.connectWithRetryAsTask address attempts retryDelayMs portHint))

@[inline] def withListener (runtime : Runtime) (address : String)
    (action : Listener -> IO α) (portHint : UInt32 := 0) : IO α := do
  let listener ← runtime.listen address portHint
  try
    action listener
  finally
    ffiRuntimeReleaseListenerImpl runtime.handle listener.handle

@[inline] def withConnection (runtime : Runtime) (address : String)
    (action : Connection -> IO α) (portHint : UInt32 := 0) : IO α := do
  let connection ← runtime.connect address portHint
  try
    action connection
  finally
    ffiRuntimeReleaseConnectionImpl runtime.handle connection.handle

@[inline] def releaseListener (runtime : Runtime) (listener : Listener) : IO Unit := do
  ensureSameRuntime runtime listener.runtime "Listener"
  ffiRuntimeReleaseListenerImpl runtime.handle listener.handle

@[inline] def releaseConnection (runtime : Runtime) (connection : Connection) : IO Unit := do
  ensureSameRuntime runtime connection.runtime "Connection"
  ffiRuntimeReleaseConnectionImpl runtime.handle connection.handle

@[inline] def listenerAccept (runtime : Runtime) (listener : Listener) : IO Connection := do
  ensureSameRuntime runtime listener.runtime "Listener"
  return {
    runtime := runtime
    handle := (← ffiRuntimeListenerAcceptImpl runtime.handle listener.handle)
  }

@[inline] def listenerAcceptStart (runtime : Runtime) (listener : Listener) :
    IO ConnectionPromiseRef := do
  ensureSameRuntime runtime listener.runtime "Listener"
  return {
    runtime := runtime
    handle := (← ffiRuntimeListenerAcceptStartImpl runtime.handle listener.handle)
  }

@[inline] def listenerAcceptWithTimeoutNanos? (runtime : Runtime) (listener : Listener)
    (timeoutNanos : UInt64) : IO (Option Connection) := do
  ensureSameRuntime runtime listener.runtime "Listener"
  let pending ← runtime.listenerAcceptStart listener
  connectionPromiseAwaitWithTimeoutNanos? pending timeoutNanos

@[inline] def listenerAcceptWithTimeoutMillis? (runtime : Runtime) (listener : Listener)
    (timeoutMillis : UInt32) : IO (Option Connection) := do
  runtime.listenerAcceptWithTimeoutNanos? listener (millisToNanos timeoutMillis)

@[inline] def connectionWrite (runtime : Runtime) (connection : Connection)
    (bytes : ByteArray) : IO Unit := do
  ensureSameRuntime runtime connection.runtime "Connection"
  ffiRuntimeConnectionWriteImpl runtime.handle connection.handle bytes

@[inline] def connectionWriteStart (runtime : Runtime) (connection : Connection)
    (bytes : ByteArray) : IO PromiseRef := do
  ensureSameRuntime runtime connection.runtime "Connection"
  return {
    runtime := runtime
    handle := (← ffiRuntimeConnectionWriteStartImpl runtime.handle connection.handle bytes)
  }

@[inline] def connectionRead (runtime : Runtime) (connection : Connection)
    (minBytes maxBytes : UInt32) : IO ByteArray := do
  ensureSameRuntime runtime connection.runtime "Connection"
  ffiRuntimeConnectionReadImpl runtime.handle connection.handle minBytes maxBytes

@[inline] def connectionReadStart (runtime : Runtime) (connection : Connection)
    (minBytes maxBytes : UInt32) : IO BytesPromiseRef := do
  ensureSameRuntime runtime connection.runtime "Connection"
  return {
    runtime := runtime
    handle := (← ffiRuntimeConnectionReadStartImpl
      runtime.handle connection.handle minBytes maxBytes)
  }

@[inline] def bytesPromiseAwait (runtime : Runtime) (promise : BytesPromiseRef) : IO ByteArray := do
  ensureSameRuntime runtime promise.runtime "BytesPromiseRef"
  ffiRuntimeBytesPromiseAwaitImpl runtime.handle promise.handle

@[inline] def bytesPromiseCancel (runtime : Runtime) (promise : BytesPromiseRef) : IO Unit := do
  ensureSameRuntime runtime promise.runtime "BytesPromiseRef"
  ffiRuntimeBytesPromiseCancelImpl runtime.handle promise.handle

@[inline] def bytesPromiseRelease (runtime : Runtime) (promise : BytesPromiseRef) : IO Unit := do
  ensureSameRuntime runtime promise.runtime "BytesPromiseRef"
  ffiRuntimeBytesPromiseReleaseImpl runtime.handle promise.handle

@[inline] def connectionShutdownWrite (runtime : Runtime) (connection : Connection) : IO Unit := do
  ensureSameRuntime runtime connection.runtime "Connection"
  ffiRuntimeConnectionShutdownWriteImpl runtime.handle connection.handle

@[inline] def connectionShutdownWriteStart (runtime : Runtime)
    (connection : Connection) : IO PromiseRef := do
  ensureSameRuntime runtime connection.runtime "Connection"
  return {
    runtime := runtime
    handle := (← ffiRuntimeConnectionShutdownWriteStartImpl runtime.handle connection.handle)
  }

@[inline] def promiseThenStart (runtime : Runtime) (first second : PromiseRef) :
    IO PromiseRef := do
  ensureSameRuntime runtime first.runtime "PromiseRef"
  ensureSameRuntime runtime second.runtime "PromiseRef"
  return {
    runtime := runtime
    handle := (← ffiRuntimePromiseThenStartImpl runtime.handle first.handle second.handle)
  }

@[inline] def promiseCatchStart (runtime : Runtime) (promise fallback : PromiseRef) :
    IO PromiseRef := do
  ensureSameRuntime runtime promise.runtime "PromiseRef"
  ensureSameRuntime runtime fallback.runtime "PromiseRef"
  return {
    runtime := runtime
    handle := (← ffiRuntimePromiseCatchStartImpl runtime.handle promise.handle fallback.handle)
  }

@[inline] def promiseAllStart (runtime : Runtime) (promises : Array PromiseRef) :
    IO PromiseRef := do
  let encoded ← encodePromiseHandlesForRuntime runtime promises
  return {
    runtime := runtime
    handle := (← ffiRuntimePromiseAllStartImpl runtime.handle encoded)
  }

@[inline] def promiseRaceStart (runtime : Runtime) (promises : Array PromiseRef) :
    IO PromiseRef := do
  let encoded ← encodePromiseHandlesForRuntime runtime promises
  return {
    runtime := runtime
    handle := (← ffiRuntimePromiseRaceStartImpl runtime.handle encoded)
  }

@[inline] def taskSetNew (runtime : Runtime) : IO TaskSetRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeTaskSetNewImpl runtime.handle)
  }

@[inline] def taskSetRelease (runtime : Runtime) (taskSet : TaskSetRef) : IO Unit := do
  ensureSameRuntime runtime taskSet.runtime "TaskSetRef"
  ffiRuntimeTaskSetReleaseImpl runtime.handle taskSet.handle

@[inline] def taskSetAddPromise (runtime : Runtime) (taskSet : TaskSetRef)
    (promise : PromiseRef) : IO Unit := do
  ensureSameRuntime runtime taskSet.runtime "TaskSetRef"
  ensureSameRuntime runtime promise.runtime "PromiseRef"
  ffiRuntimeTaskSetAddPromiseImpl runtime.handle taskSet.handle promise.handle

@[inline] def taskSetClear (runtime : Runtime) (taskSet : TaskSetRef) : IO Unit := do
  ensureSameRuntime runtime taskSet.runtime "TaskSetRef"
  ffiRuntimeTaskSetClearImpl runtime.handle taskSet.handle

@[inline] def taskSetIsEmpty (runtime : Runtime) (taskSet : TaskSetRef) : IO Bool := do
  ensureSameRuntime runtime taskSet.runtime "TaskSetRef"
  ffiRuntimeTaskSetIsEmptyImpl runtime.handle taskSet.handle

@[inline] def taskSetOnEmptyStart (runtime : Runtime) (taskSet : TaskSetRef) : IO PromiseRef := do
  ensureSameRuntime runtime taskSet.runtime "TaskSetRef"
  return {
    runtime := runtime
    handle := (← ffiRuntimeTaskSetOnEmptyStartImpl runtime.handle taskSet.handle)
  }

@[inline] def taskSetErrorCount (runtime : Runtime) (taskSet : TaskSetRef) : IO UInt32 := do
  ensureSameRuntime runtime taskSet.runtime "TaskSetRef"
  ffiRuntimeTaskSetErrorCountImpl runtime.handle taskSet.handle

@[inline] def taskSetTakeLastError? (runtime : Runtime) (taskSet : TaskSetRef) :
    IO (Option String) := do
  ensureSameRuntime runtime taskSet.runtime "TaskSetRef"
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
  ensureSameRuntime runtime connection.runtime "Connection"
  return {
    runtime := runtime
    handle := (← ffiRuntimeConnectionWhenWriteDisconnectedStartImpl runtime.handle connection.handle)
  }

@[inline] def connectionAbortRead (runtime : Runtime) (connection : Connection) : IO Unit := do
  ensureSameRuntime runtime connection.runtime "Connection"
  ffiRuntimeConnectionAbortReadImpl runtime.handle connection.handle

@[inline] def connectionAbortWrite (runtime : Runtime) (connection : Connection)
    (reason : String := "Capnp.KjAsync connection abortWrite") : IO Unit := do
  ensureSameRuntime runtime connection.runtime "Connection"
  ffiRuntimeConnectionAbortWriteImpl runtime.handle connection.handle reason

@[inline] def connectionDupFd? (runtime : Runtime) (connection : Connection) :
    IO (Option UInt32) := do
  ensureSameRuntime runtime connection.runtime "Connection"
  let (hasFd, fd) ← ffiRuntimeConnectionDupFdImpl runtime.handle connection.handle
  if hasFd then
    return some fd
  else
    return none

@[inline] def newTwoWayPipe (runtime : Runtime) : IO (Connection × Connection) := do
  let (first, second) ← ffiRuntimeNewTwoWayPipeImpl runtime.handle
  return (
    { runtime := runtime, handle := first },
    { runtime := runtime, handle := second }
  )

@[inline] def newCapabilityPipe (runtime : Runtime) : IO (Connection × Connection) := do
  let (first, second) ← ffiRuntimeNewCapabilityPipeImpl runtime.handle
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

@[inline] def datagramBindEndpoint (runtime : Runtime) (endpoint : Endpoint) : IO DatagramPort :=
  runtime.datagramBind endpoint.address endpoint.portHint

@[inline] def datagramReleasePort (runtime : Runtime) (port : DatagramPort) : IO Unit := do
  ensureSameRuntime runtime port.runtime "DatagramPort"
  ffiRuntimeDatagramReleasePortImpl runtime.handle port.handle

@[inline] def datagramGetPort (runtime : Runtime) (port : DatagramPort) : IO UInt32 := do
  ensureSameRuntime runtime port.runtime "DatagramPort"
  ffiRuntimeDatagramGetPortImpl runtime.handle port.handle

@[inline] def datagramSend (runtime : Runtime) (port : DatagramPort)
    (address : String) (bytes : ByteArray) (portHint : UInt32 := 0) : IO UInt32 := do
  ensureSameRuntime runtime port.runtime "DatagramPort"
  ffiRuntimeDatagramSendImpl runtime.handle port.handle address portHint bytes

@[inline] def datagramSendStart (runtime : Runtime) (port : DatagramPort)
    (address : String) (bytes : ByteArray) (portHint : UInt32 := 0) : IO UInt32PromiseRef := do
  ensureSameRuntime runtime port.runtime "DatagramPort"
  return {
    runtime := runtime
    handle := (← ffiRuntimeDatagramSendStartImpl runtime.handle port.handle address portHint bytes)
  }

@[inline] def datagramPeerBind (runtime : Runtime) (localAddress remoteAddress : String)
    (remotePort : UInt32) (localPortHint : UInt32 := 0) : IO DatagramPeer := do
  pure {
    port := (← runtime.datagramBind localAddress localPortHint)
    remoteAddress := remoteAddress
    remotePort := remotePort
  }

@[inline] def uint32PromiseAwait (runtime : Runtime) (promise : UInt32PromiseRef) : IO UInt32 := do
  ensureSameRuntime runtime promise.runtime "UInt32PromiseRef"
  ffiRuntimeUInt32PromiseAwaitImpl runtime.handle promise.handle

@[inline] def uint32PromiseCancel (runtime : Runtime) (promise : UInt32PromiseRef) : IO Unit := do
  ensureSameRuntime runtime promise.runtime "UInt32PromiseRef"
  ffiRuntimeUInt32PromiseCancelImpl runtime.handle promise.handle

@[inline] def uint32PromiseRelease (runtime : Runtime) (promise : UInt32PromiseRef) : IO Unit := do
  ensureSameRuntime runtime promise.runtime "UInt32PromiseRef"
  ffiRuntimeUInt32PromiseReleaseImpl runtime.handle promise.handle

@[inline] def datagramSendAsTask (runtime : Runtime) (port : DatagramPort) (address : String)
    (bytes : ByteArray) (portHint : UInt32 := 0) :
    IO (Task (Except IO.Error UInt32)) := do
  let pending ← runtime.datagramSendStart port address bytes portHint
  IO.asTask do
    runtime.uint32PromiseAwait pending

@[inline] def datagramSendAsPromise (runtime : Runtime) (port : DatagramPort) (address : String)
    (bytes : ByteArray) (portHint : UInt32 := 0) :
    IO (Capnp.Async.Promise UInt32) := do
  pure (Capnp.Async.Promise.ofTask (← runtime.datagramSendAsTask port address bytes portHint))

@[inline] def datagramReceive (runtime : Runtime) (port : DatagramPort)
    (maxBytes : UInt32 := 0x2000) : IO (String × ByteArray) := do
  ensureSameRuntime runtime port.runtime "DatagramPort"
  ffiRuntimeDatagramReceiveImpl runtime.handle port.handle maxBytes

@[inline] def datagramReceiveStart (runtime : Runtime) (port : DatagramPort)
    (maxBytes : UInt32 := 0x2000) : IO DatagramReceivePromiseRef := do
  ensureSameRuntime runtime port.runtime "DatagramPort"
  return {
    runtime := runtime
    handle := (← ffiRuntimeDatagramReceiveStartImpl runtime.handle port.handle maxBytes)
  }

@[inline] def datagramReceivePromiseAwait (runtime : Runtime)
    (promise : DatagramReceivePromiseRef) : IO (String × ByteArray) := do
  ensureSameRuntime runtime promise.runtime "DatagramReceivePromiseRef"
  ffiRuntimeDatagramReceivePromiseAwaitImpl runtime.handle promise.handle

@[inline] def datagramReceivePromiseCancel (runtime : Runtime)
    (promise : DatagramReceivePromiseRef) : IO Unit := do
  ensureSameRuntime runtime promise.runtime "DatagramReceivePromiseRef"
  ffiRuntimeDatagramReceivePromiseCancelImpl runtime.handle promise.handle

@[inline] def datagramReceivePromiseRelease (runtime : Runtime)
    (promise : DatagramReceivePromiseRef) : IO Unit := do
  ensureSameRuntime runtime promise.runtime "DatagramReceivePromiseRef"
  ffiRuntimeDatagramReceivePromiseReleaseImpl runtime.handle promise.handle

@[inline] def datagramReceiveAsTask (runtime : Runtime) (port : DatagramPort)
    (maxBytes : UInt32 := 0x2000) :
    IO (Task (Except IO.Error (String × ByteArray))) := do
  let pending ← runtime.datagramReceiveStart port maxBytes
  IO.asTask do
    runtime.datagramReceivePromiseAwait pending

@[inline] def datagramReceiveAsPromise (runtime : Runtime) (port : DatagramPort)
    (maxBytes : UInt32 := 0x2000) :
    IO (Capnp.Async.Promise (String × ByteArray)) := do
  pure (Capnp.Async.Promise.ofTask (← runtime.datagramReceiveAsTask port maxBytes))

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

@[inline] def httpRequestWithResponseLimit (runtime : Runtime) (method : HttpMethod)
    (address : String) (path : String) (responseBodyLimit : UInt64)
    (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) : IO HttpResponse := do
  let (status, responseBody) ←
    ffiRuntimeHttpRequestWithResponseLimitImpl runtime.handle (httpMethodToTag method) address
      portHint path body responseBodyLimit
  return { status := status, body := responseBody }

@[inline] def httpRequestWithHeaders (runtime : Runtime) (method : HttpMethod) (address : String)
    (path : String) (requestHeaders : Array HttpHeader) (body : ByteArray := ByteArray.empty)
    (portHint : UInt32 := 0) : IO HttpResponseEx := do
  let (status, statusText, responseHeaderBytes, responseBody) ←
    ffiRuntimeHttpRequestWithHeadersImpl runtime.handle (httpMethodToTag method) address
      portHint path (encodeHeaders requestHeaders) body
  return {
    status := status
    statusText := statusText
    headers := (← decodeHeaders responseHeaderBytes)
    body := responseBody
  }

@[inline] def httpRequestWithHeadersSecure (runtime : Runtime) (method : HttpMethod)
    (address : String) (path : String) (requestHeaders : Array HttpHeader)
    (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) : IO HttpResponseEx := do
  let (status, statusText, responseHeaderBytes, responseBody) ←
    ffiRuntimeHttpRequestWithHeadersSecureImpl runtime.handle (httpMethodToTag method) address
      portHint path (encodeHeaders requestHeaders) body
  return {
    status := status
    statusText := statusText
    headers := (← decodeHeaders responseHeaderBytes)
    body := responseBody
  }

@[inline] def httpRequestWithHeadersWithResponseLimit (runtime : Runtime) (method : HttpMethod)
    (address : String) (path : String) (requestHeaders : Array HttpHeader)
    (responseBodyLimit : UInt64) (body : ByteArray := ByteArray.empty)
    (portHint : UInt32 := 0) : IO HttpResponseEx := do
  let (status, statusText, responseHeaderBytes, responseBody) ←
    ffiRuntimeHttpRequestWithHeadersWithResponseLimitImpl runtime.handle (httpMethodToTag method)
      address portHint path (encodeHeaders requestHeaders) body responseBodyLimit
  return {
    status := status
    statusText := statusText
    headers := (← decodeHeaders responseHeaderBytes)
    body := responseBody
  }

@[inline] def httpRequestWithHeadersWithResponseLimitSecure (runtime : Runtime)
    (method : HttpMethod) (address : String) (path : String)
    (requestHeaders : Array HttpHeader) (responseBodyLimit : UInt64)
    (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) : IO HttpResponseEx := do
  let (status, statusText, responseHeaderBytes, responseBody) ←
    ffiRuntimeHttpRequestWithHeadersWithResponseLimitSecureImpl runtime.handle
      (httpMethodToTag method) address portHint path (encodeHeaders requestHeaders) body
      responseBodyLimit
  return {
    status := status
    statusText := statusText
    headers := (← decodeHeaders responseHeaderBytes)
    body := responseBody
  }

@[inline] def httpRequestStart (runtime : Runtime) (method : HttpMethod) (address : String)
    (path : String) (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) :
    IO HttpResponsePromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeHttpRequestStartImpl
      runtime.handle (httpMethodToTag method) address portHint path body)
  }

@[inline] def httpRequestStartWithResponseLimit (runtime : Runtime) (method : HttpMethod)
    (address : String) (path : String) (responseBodyLimit : UInt64)
    (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) :
    IO HttpResponsePromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeHttpRequestStartWithResponseLimitImpl
      runtime.handle (httpMethodToTag method) address portHint path body responseBodyLimit)
  }

@[inline] def httpRequestStartWithHeaders (runtime : Runtime) (method : HttpMethod)
    (address : String) (path : String) (requestHeaders : Array HttpHeader)
    (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) :
    IO HttpResponsePromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeHttpRequestStartWithHeadersImpl
      runtime.handle (httpMethodToTag method) address portHint path
      (encodeHeaders requestHeaders) body)
  }

@[inline] def httpRequestStartWithHeadersSecure (runtime : Runtime) (method : HttpMethod)
    (address : String) (path : String) (requestHeaders : Array HttpHeader)
    (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) :
    IO HttpResponsePromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeHttpRequestStartWithHeadersSecureImpl
      runtime.handle (httpMethodToTag method) address portHint path
      (encodeHeaders requestHeaders) body)
  }

@[inline] def httpRequestStartStreamingWithHeaders (runtime : Runtime) (method : HttpMethod)
    (address : String) (path : String) (requestHeaders : Array HttpHeader)
    (portHint : UInt32 := 0) : IO (Option HttpRequestBody × HttpResponsePromiseRef) := do
  let (requestBodyHandle, promiseHandle) ←
    ffiRuntimeHttpRequestStartStreamingWithHeadersImpl runtime.handle (httpMethodToTag method)
      address portHint path (encodeHeaders requestHeaders)
  let requestBody? :=
    if requestBodyHandle == 0 then
      none
    else
      some { runtime := runtime, handle := requestBodyHandle }
  let responsePromise : HttpResponsePromiseRef := { runtime := runtime, handle := promiseHandle }
  return (requestBody?, responsePromise)

@[inline] def httpRequestStartStreamingWithHeadersSecure (runtime : Runtime)
    (method : HttpMethod) (address : String) (path : String)
    (requestHeaders : Array HttpHeader) (portHint : UInt32 := 0) :
    IO (Option HttpRequestBody × HttpResponsePromiseRef) := do
  let (requestBodyHandle, promiseHandle) ←
    ffiRuntimeHttpRequestStartStreamingWithHeadersSecureImpl runtime.handle (httpMethodToTag method)
      address portHint path (encodeHeaders requestHeaders)
  let requestBody? :=
    if requestBodyHandle == 0 then
      none
    else
      some { runtime := runtime, handle := requestBodyHandle }
  let responsePromise : HttpResponsePromiseRef := { runtime := runtime, handle := promiseHandle }
  return (requestBody?, responsePromise)

@[inline] def httpRequestStartWithHeadersWithResponseLimit (runtime : Runtime)
    (method : HttpMethod) (address : String) (path : String)
    (requestHeaders : Array HttpHeader) (responseBodyLimit : UInt64)
    (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) :
    IO HttpResponsePromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeHttpRequestStartWithHeadersWithResponseLimitImpl
      runtime.handle (httpMethodToTag method) address portHint path
      (encodeHeaders requestHeaders) body responseBodyLimit)
  }

@[inline] def httpRequestStartWithHeadersWithResponseLimitSecure (runtime : Runtime)
    (method : HttpMethod) (address : String) (path : String)
    (requestHeaders : Array HttpHeader) (responseBodyLimit : UInt64)
    (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) :
    IO HttpResponsePromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeHttpRequestStartWithHeadersWithResponseLimitSecureImpl
      runtime.handle (httpMethodToTag method) address portHint path
      (encodeHeaders requestHeaders) body responseBodyLimit)
  }

@[inline] def httpResponsePromiseAwait (runtime : Runtime)
    (promise : HttpResponsePromiseRef) : IO HttpResponse := do
  ensureSameRuntime runtime promise.runtime "HttpResponsePromiseRef"
  let (status, responseBody) ←
    ffiRuntimeHttpResponsePromiseAwaitImpl runtime.handle promise.handle
  return { status := status, body := responseBody }

@[inline] def httpResponsePromiseAwaitWithHeaders (runtime : Runtime)
    (promise : HttpResponsePromiseRef) : IO HttpResponseEx := do
  ensureSameRuntime runtime promise.runtime "HttpResponsePromiseRef"
  let (status, statusText, responseHeaderBytes, responseBody) ←
    ffiRuntimeHttpResponsePromiseAwaitWithHeadersImpl runtime.handle promise.handle
  return {
    status := status
    statusText := statusText
    headers := (← decodeHeaders responseHeaderBytes)
    body := responseBody
  }

@[inline] def httpResponsePromiseAwaitStreamingWithHeaders (runtime : Runtime)
    (promise : HttpResponsePromiseRef) : IO HttpResponseStreaming := do
  ensureSameRuntime runtime promise.runtime "HttpResponsePromiseRef"
  let (status, statusText, responseHeaderBytes, responseBodyHandle) ←
    ffiRuntimeHttpResponsePromiseAwaitStreamingWithHeadersImpl runtime.handle promise.handle
  return {
    status := status
    statusText := statusText
    headers := (← decodeHeaders responseHeaderBytes)
    body := { runtime := runtime, handle := responseBodyHandle }
  }

@[inline] def httpResponsePromiseCancel (runtime : Runtime)
    (promise : HttpResponsePromiseRef) : IO Unit := do
  ensureSameRuntime runtime promise.runtime "HttpResponsePromiseRef"
  ffiRuntimeHttpResponsePromiseCancelImpl runtime.handle promise.handle

@[inline] def httpResponsePromiseRelease (runtime : Runtime)
    (promise : HttpResponsePromiseRef) : IO Unit := do
  ensureSameRuntime runtime promise.runtime "HttpResponsePromiseRef"
  ffiRuntimeHttpResponsePromiseReleaseImpl runtime.handle promise.handle

@[inline] def httpRequestAsTask (runtime : Runtime) (method : HttpMethod) (address : String)
    (path : String) (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) :
    IO (Task (Except IO.Error HttpResponse)) := do
  IO.asTask do
    runtime.httpRequest method address path body portHint

@[inline] def httpRequestAsPromise (runtime : Runtime) (method : HttpMethod) (address : String)
    (path : String) (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) :
    IO (Capnp.Async.Promise HttpResponse) := do
  pure (Capnp.Async.Promise.ofTask
    (← runtime.httpRequestAsTask method address path body portHint))

@[inline] def httpRequestWithHeadersAsTask (runtime : Runtime) (method : HttpMethod)
    (address : String) (path : String) (requestHeaders : Array HttpHeader)
    (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) :
    IO (Task (Except IO.Error HttpResponseEx)) := do
  IO.asTask do
    runtime.httpRequestWithHeaders method address path requestHeaders body portHint

@[inline] def httpRequestWithHeadersAsPromise (runtime : Runtime) (method : HttpMethod)
    (address : String) (path : String) (requestHeaders : Array HttpHeader)
    (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) :
    IO (Capnp.Async.Promise HttpResponseEx) := do
  pure (Capnp.Async.Promise.ofTask
    (← runtime.httpRequestWithHeadersAsTask method address path requestHeaders body portHint))

@[inline] def httpRequestWithHeadersSecureAsTask (runtime : Runtime) (method : HttpMethod)
    (address : String) (path : String) (requestHeaders : Array HttpHeader)
    (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) :
    IO (Task (Except IO.Error HttpResponseEx)) := do
  IO.asTask do
    runtime.httpRequestWithHeadersSecure method address path requestHeaders body portHint

@[inline] def httpRequestWithHeadersSecureAsPromise (runtime : Runtime) (method : HttpMethod)
    (address : String) (path : String) (requestHeaders : Array HttpHeader)
    (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) :
    IO (Capnp.Async.Promise HttpResponseEx) := do
  pure (Capnp.Async.Promise.ofTask
    (← runtime.httpRequestWithHeadersSecureAsTask method address path requestHeaders body
      portHint))

@[inline] def httpRequestBodyWriteStart (runtime : Runtime) (requestBody : HttpRequestBody)
    (bytes : ByteArray) : IO PromiseRef := do
  ensureSameRuntime runtime requestBody.runtime "HttpRequestBody"
  return {
    runtime := runtime
    handle := (← ffiRuntimeHttpRequestBodyWriteStartImpl runtime.handle requestBody.handle bytes)
  }

@[inline] def httpRequestBodyWrite (runtime : Runtime) (requestBody : HttpRequestBody)
    (bytes : ByteArray) : IO Unit := do
  let promise ← runtime.httpRequestBodyWriteStart requestBody bytes
  ffiRuntimePromiseAwaitImpl runtime.handle promise.handle

@[inline] def httpRequestBodyFinishStart (runtime : Runtime) (requestBody : HttpRequestBody) :
    IO PromiseRef := do
  ensureSameRuntime runtime requestBody.runtime "HttpRequestBody"
  return {
    runtime := runtime
    handle := (← ffiRuntimeHttpRequestBodyFinishStartImpl runtime.handle requestBody.handle)
  }

@[inline] def httpRequestBodyFinish (runtime : Runtime) (requestBody : HttpRequestBody) :
    IO Unit := do
  let promise ← runtime.httpRequestBodyFinishStart requestBody
  ffiRuntimePromiseAwaitImpl runtime.handle promise.handle

@[inline] def httpRequestBodyRelease (runtime : Runtime) (requestBody : HttpRequestBody) :
    IO Unit := do
  ensureSameRuntime runtime requestBody.runtime "HttpRequestBody"
  ffiRuntimeHttpRequestBodyReleaseImpl runtime.handle requestBody.handle

@[inline] def httpResponseBodyReadStart (runtime : Runtime) (responseBody : HttpResponseBody)
    (minBytes maxBytes : UInt32) : IO BytesPromiseRef := do
  ensureSameRuntime runtime responseBody.runtime "HttpResponseBody"
  return {
    runtime := runtime
    handle := (← ffiRuntimeHttpResponseBodyReadStartImpl runtime.handle responseBody.handle
      minBytes maxBytes)
  }

@[inline] def httpResponseBodyRead (runtime : Runtime) (responseBody : HttpResponseBody)
    (minBytes maxBytes : UInt32) : IO ByteArray := do
  ensureSameRuntime runtime responseBody.runtime "HttpResponseBody"
  ffiRuntimeHttpResponseBodyReadImpl runtime.handle responseBody.handle minBytes maxBytes

@[inline] def httpResponseBodyRelease (runtime : Runtime) (responseBody : HttpResponseBody) :
    IO Unit := do
  ensureSameRuntime runtime responseBody.runtime "HttpResponseBody"
  ffiRuntimeHttpResponseBodyReleaseImpl runtime.handle responseBody.handle

@[inline] def httpServerRequestBodyReadStart (runtime : Runtime)
    (requestBody : HttpServerRequestBody) (minBytes maxBytes : UInt32) :
    IO BytesPromiseRef := do
  ensureSameRuntime runtime requestBody.runtime "HttpServerRequestBody"
  return {
    runtime := runtime
    handle := (← ffiRuntimeHttpServerRequestBodyReadStartImpl runtime.handle requestBody.handle
      minBytes maxBytes)
  }

@[inline] def httpServerRequestBodyRead (runtime : Runtime)
    (requestBody : HttpServerRequestBody) (minBytes maxBytes : UInt32) :
    IO ByteArray := do
  ensureSameRuntime runtime requestBody.runtime "HttpServerRequestBody"
  ffiRuntimeHttpServerRequestBodyReadImpl runtime.handle requestBody.handle minBytes maxBytes

@[inline] def httpServerRequestBodyRelease (runtime : Runtime)
    (requestBody : HttpServerRequestBody) : IO Unit := do
  ensureSameRuntime runtime requestBody.runtime "HttpServerRequestBody"
  ffiRuntimeHttpServerRequestBodyReleaseImpl runtime.handle requestBody.handle

@[inline] def httpGet (runtime : Runtime) (address : String) (path : String)
    (portHint : UInt32 := 0) : IO HttpResponse :=
  runtime.httpRequest .get address path ByteArray.empty portHint

@[inline] def httpPost (runtime : Runtime) (address : String) (path : String)
    (body : ByteArray) (portHint : UInt32 := 0) : IO HttpResponse :=
  runtime.httpRequest .post address path body portHint

@[inline] def httpServerListenWithConfig (runtime : Runtime) (address : String)
    (config : HttpServerConfig) (portHint : UInt32 := 0) : IO HttpServer := do
  let (serverId, boundPort) ← ffiRuntimeHttpServerListenWithConfigImpl runtime.handle
    address portHint config.headerTimeoutNanos config.pipelineTimeoutNanos
    config.canceledUploadGracePeriodNanos config.canceledUploadGraceBytes
    (webSocketCompressionModeToTag config.webSocketCompressionMode)
  return { runtime := runtime, handle := serverId, boundPort := boundPort }

@[inline] def httpServerListenSecureWithConfig (runtime : Runtime) (address : String)
    (config : HttpServerConfig) (portHint : UInt32 := 0) : IO HttpServer := do
  let (serverId, boundPort) ← ffiRuntimeHttpServerListenSecureWithConfigImpl runtime.handle
    address portHint config.headerTimeoutNanos config.pipelineTimeoutNanos
    config.canceledUploadGracePeriodNanos config.canceledUploadGraceBytes
    (webSocketCompressionModeToTag config.webSocketCompressionMode)
  return { runtime := runtime, handle := serverId, boundPort := boundPort }

@[inline] def httpServerListen (runtime : Runtime) (address : String)
    (portHint : UInt32 := 0) : IO HttpServer :=
  runtime.httpServerListenWithConfig address {} portHint

@[inline] def httpServerListenSecure (runtime : Runtime) (address : String)
    (portHint : UInt32 := 0) : IO HttpServer :=
  runtime.httpServerListenSecureWithConfig address {} portHint

@[inline] def httpServerRelease (runtime : Runtime) (server : HttpServer) : IO Unit := do
  ensureSameRuntime runtime server.runtime "HttpServer"
  ffiRuntimeHttpServerReleaseImpl runtime.handle server.handle

@[inline] def httpServerPollRequestStreaming? (runtime : Runtime) (server : HttpServer) :
    IO (Option HttpServerRequest) := do
  ensureSameRuntime runtime server.runtime "HttpServer"
  let (hasRequest, payload) ← ffiRuntimeHttpServerPollRequestImpl runtime.handle server.handle
  if hasRequest then
    return some (← decodeHttpServerRequest runtime payload)
  else
    return none

@[inline] def httpServerDrainStart (runtime : Runtime) (server : HttpServer) :
    IO PromiseRef := do
  ensureSameRuntime runtime server.runtime "HttpServer"
  return {
    runtime := runtime
    handle := (← ffiRuntimeHttpServerDrainStartImpl runtime.handle server.handle)
  }

@[inline] def httpServerDrain (runtime : Runtime) (server : HttpServer) :
    IO Unit := do
  ensureSameRuntime runtime server.runtime "HttpServer"
  ffiRuntimeHttpServerDrainImpl runtime.handle server.handle

@[inline] def httpServerPollRequest? (runtime : Runtime) (server : HttpServer) :
    IO (Option HttpServerRequest) := do
  match (← runtime.httpServerPollRequestStreaming? server) with
  | some request => return some (← drainHttpServerRequestBody request)
  | none => return none

@[inline] def httpServerRespond (runtime : Runtime) (server : HttpServer) (requestId : UInt32)
    (status : UInt32) (statusText : String) (responseHeaders : Array HttpHeader := #[])
    (body : ByteArray := ByteArray.empty) : IO Unit := do
  ensureSameRuntime runtime server.runtime "HttpServer"
  ffiRuntimeHttpServerRespondImpl runtime.handle server.handle requestId status statusText
    (encodeHeaders responseHeaders) body

@[inline] def httpServerRespondWebSocket (runtime : Runtime) (server : HttpServer)
    (requestId : UInt32) (responseHeaders : Array HttpHeader := #[]) : IO WebSocket := do
  ensureSameRuntime runtime server.runtime "HttpServer"
  return {
    runtime := runtime
    handle := (← ffiRuntimeHttpServerRespondWebSocketImpl runtime.handle server.handle requestId
      (encodeHeaders responseHeaders))
  }

@[inline] def httpServerRespondStartStreaming (runtime : Runtime) (server : HttpServer)
    (requestId : UInt32) (status : UInt32) (statusText : String)
    (responseHeaders : Array HttpHeader := #[]) : IO HttpServerResponseBody := do
  ensureSameRuntime runtime server.runtime "HttpServer"
  return {
    runtime := runtime
    handle := (← ffiRuntimeHttpServerRespondStartStreamingImpl
      runtime.handle server.handle requestId status statusText (encodeHeaders responseHeaders))
  }

@[inline] def httpServerResponseBodyWriteStart (runtime : Runtime)
    (responseBody : HttpServerResponseBody) (bytes : ByteArray) : IO PromiseRef := do
  ensureSameRuntime runtime responseBody.runtime "HttpServerResponseBody"
  return {
    runtime := runtime
    handle := (← ffiRuntimeHttpServerResponseBodyWriteStartImpl runtime.handle
      responseBody.handle bytes)
  }

@[inline] def httpServerResponseBodyWrite (runtime : Runtime)
    (responseBody : HttpServerResponseBody) (bytes : ByteArray) : IO Unit := do
  let promise ← runtime.httpServerResponseBodyWriteStart responseBody bytes
  ffiRuntimePromiseAwaitImpl runtime.handle promise.handle

@[inline] def httpServerResponseBodyFinishStart (runtime : Runtime)
    (responseBody : HttpServerResponseBody) : IO PromiseRef := do
  ensureSameRuntime runtime responseBody.runtime "HttpServerResponseBody"
  return {
    runtime := runtime
    handle := (← ffiRuntimeHttpServerResponseBodyFinishStartImpl runtime.handle
      responseBody.handle)
  }

@[inline] def httpServerResponseBodyFinish (runtime : Runtime)
    (responseBody : HttpServerResponseBody) : IO Unit := do
  let promise ← runtime.httpServerResponseBodyFinishStart responseBody
  ffiRuntimePromiseAwaitImpl runtime.handle promise.handle

@[inline] def httpServerResponseBodyRelease (runtime : Runtime)
    (responseBody : HttpServerResponseBody) : IO Unit := do
  ensureSameRuntime runtime responseBody.runtime "HttpServerResponseBody"
  ffiRuntimeHttpServerResponseBodyReleaseImpl runtime.handle responseBody.handle

@[inline] def webSocketConnect (runtime : Runtime) (address : String)
    (path : String) (portHint : UInt32 := 0) : IO WebSocket := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeWebSocketConnectWithHeadersImpl runtime.handle address portHint path
      (encodeHeaders #[]))
  }

@[inline] def webSocketConnectSecure (runtime : Runtime) (address : String)
    (path : String) (portHint : UInt32 := 0) : IO WebSocket := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeWebSocketConnectWithHeadersSecureImpl runtime.handle address portHint path
      (encodeHeaders #[]))
  }

@[inline] def webSocketConnectStart (runtime : Runtime) (address : String)
    (path : String) (portHint : UInt32 := 0) : IO WebSocketPromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeWebSocketConnectStartWithHeadersImpl
      runtime.handle address portHint path (encodeHeaders #[]))
  }

@[inline] def webSocketConnectStartSecure (runtime : Runtime) (address : String)
    (path : String) (portHint : UInt32 := 0) : IO WebSocketPromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeWebSocketConnectStartWithHeadersSecureImpl
      runtime.handle address portHint path (encodeHeaders #[]))
  }

@[inline] def webSocketConnectWithHeaders (runtime : Runtime) (address : String)
    (path : String) (requestHeaders : Array HttpHeader) (portHint : UInt32 := 0) :
    IO WebSocket := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeWebSocketConnectWithHeadersImpl runtime.handle address portHint path
      (encodeHeaders requestHeaders))
  }

@[inline] def webSocketConnectWithHeadersSecure (runtime : Runtime) (address : String)
    (path : String) (requestHeaders : Array HttpHeader) (portHint : UInt32 := 0) :
    IO WebSocket := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeWebSocketConnectWithHeadersSecureImpl runtime.handle address portHint path
      (encodeHeaders requestHeaders))
  }

@[inline] def webSocketConnectStartWithHeaders (runtime : Runtime) (address : String)
    (path : String) (requestHeaders : Array HttpHeader) (portHint : UInt32 := 0) :
    IO WebSocketPromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeWebSocketConnectStartWithHeadersImpl
      runtime.handle address portHint path (encodeHeaders requestHeaders))
  }

@[inline] def webSocketConnectStartWithHeadersSecure (runtime : Runtime) (address : String)
    (path : String) (requestHeaders : Array HttpHeader) (portHint : UInt32 := 0) :
    IO WebSocketPromiseRef := do
  return {
    runtime := runtime
    handle := (← ffiRuntimeWebSocketConnectStartWithHeadersSecureImpl
      runtime.handle address portHint path (encodeHeaders requestHeaders))
  }

@[inline] def webSocketPromiseAwait (runtime : Runtime)
    (promise : WebSocketPromiseRef) : IO WebSocket := do
  ensureSameRuntime runtime promise.runtime "WebSocketPromiseRef"
  return {
    runtime := runtime
    handle := (← ffiRuntimeWebSocketPromiseAwaitImpl runtime.handle promise.handle)
  }

@[inline] def webSocketPromiseCancel (runtime : Runtime)
    (promise : WebSocketPromiseRef) : IO Unit := do
  ensureSameRuntime runtime promise.runtime "WebSocketPromiseRef"
  ffiRuntimeWebSocketPromiseCancelImpl runtime.handle promise.handle

@[inline] def webSocketPromiseRelease (runtime : Runtime)
    (promise : WebSocketPromiseRef) : IO Unit := do
  ensureSameRuntime runtime promise.runtime "WebSocketPromiseRef"
  ffiRuntimeWebSocketPromiseReleaseImpl runtime.handle promise.handle

@[inline] def webSocketConnectAsTask (runtime : Runtime) (address : String) (path : String)
    (portHint : UInt32 := 0) : IO (Task (Except IO.Error WebSocket)) := do
  let pending ← runtime.webSocketConnectStart address path portHint
  IO.asTask do
    runtime.webSocketPromiseAwait pending

@[inline] def webSocketConnectAsPromise (runtime : Runtime) (address : String)
    (path : String) (portHint : UInt32 := 0) : IO (Capnp.Async.Promise WebSocket) := do
  pure (Capnp.Async.Promise.ofTask (← runtime.webSocketConnectAsTask address path portHint))

@[inline] def webSocketConnectSecureAsTask (runtime : Runtime) (address : String)
    (path : String) (portHint : UInt32 := 0) : IO (Task (Except IO.Error WebSocket)) := do
  let pending ← runtime.webSocketConnectStartSecure address path portHint
  IO.asTask do
    runtime.webSocketPromiseAwait pending

@[inline] def webSocketConnectSecureAsPromise (runtime : Runtime) (address : String)
    (path : String) (portHint : UInt32 := 0) : IO (Capnp.Async.Promise WebSocket) := do
  pure (Capnp.Async.Promise.ofTask
    (← runtime.webSocketConnectSecureAsTask address path portHint))

@[inline] def webSocketConnectWithHeadersAsTask (runtime : Runtime) (address : String)
    (path : String) (requestHeaders : Array HttpHeader) (portHint : UInt32 := 0) :
    IO (Task (Except IO.Error WebSocket)) := do
  let pending ←
    runtime.webSocketConnectStartWithHeaders address path requestHeaders portHint
  IO.asTask do
    runtime.webSocketPromiseAwait pending

@[inline] def webSocketConnectWithHeadersAsPromise (runtime : Runtime) (address : String)
    (path : String) (requestHeaders : Array HttpHeader) (portHint : UInt32 := 0) :
    IO (Capnp.Async.Promise WebSocket) := do
  pure (Capnp.Async.Promise.ofTask
    (← runtime.webSocketConnectWithHeadersAsTask address path requestHeaders portHint))

@[inline] def webSocketConnectWithHeadersSecureAsTask (runtime : Runtime) (address : String)
    (path : String) (requestHeaders : Array HttpHeader) (portHint : UInt32 := 0) :
    IO (Task (Except IO.Error WebSocket)) := do
  let pending ←
    runtime.webSocketConnectStartWithHeadersSecure address path requestHeaders portHint
  IO.asTask do
    runtime.webSocketPromiseAwait pending

@[inline] def webSocketConnectWithHeadersSecureAsPromise (runtime : Runtime) (address : String)
    (path : String) (requestHeaders : Array HttpHeader) (portHint : UInt32 := 0) :
    IO (Capnp.Async.Promise WebSocket) := do
  pure (Capnp.Async.Promise.ofTask
    (← runtime.webSocketConnectWithHeadersSecureAsTask address path requestHeaders portHint))

@[inline] def webSocketRelease (runtime : Runtime) (webSocket : WebSocket) : IO Unit := do
  ensureSameRuntime runtime webSocket.runtime "WebSocket"
  ffiRuntimeWebSocketReleaseImpl runtime.handle webSocket.handle

@[inline] def webSocketSendTextStart (runtime : Runtime) (webSocket : WebSocket) (text : String) :
    IO PromiseRef := do
  ensureSameRuntime runtime webSocket.runtime "WebSocket"
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
  ensureSameRuntime runtime webSocket.runtime "WebSocket"
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
  ensureSameRuntime runtime webSocket.runtime "WebSocket"
  return {
    runtime := runtime
    handle := (← ffiRuntimeWebSocketReceiveStartImpl runtime.handle webSocket.handle)
  }

@[inline] def webSocketReceiveStartWithMax (runtime : Runtime) (webSocket : WebSocket)
    (maxBytes : UInt32) : IO WebSocketMessagePromiseRef := do
  ensureSameRuntime runtime webSocket.runtime "WebSocket"
  return {
    runtime := runtime
    handle := (← ffiRuntimeWebSocketReceiveStartWithMaxImpl runtime.handle
      webSocket.handle maxBytes)
  }

@[inline] def webSocketMessagePromiseAwait (runtime : Runtime)
    (promise : WebSocketMessagePromiseRef) : IO WebSocketMessage := do
  ensureSameRuntime runtime promise.runtime "WebSocketMessagePromiseRef"
  let (tag, closeCode, text, bytes) ←
    ffiRuntimeWebSocketMessagePromiseAwaitImpl runtime.handle promise.handle
  decodeWebSocketMessage tag closeCode text bytes

@[inline] def webSocketMessagePromiseCancel (runtime : Runtime)
    (promise : WebSocketMessagePromiseRef) : IO Unit := do
  ensureSameRuntime runtime promise.runtime "WebSocketMessagePromiseRef"
  ffiRuntimeWebSocketMessagePromiseCancelImpl runtime.handle promise.handle

@[inline] def webSocketMessagePromiseRelease (runtime : Runtime)
    (promise : WebSocketMessagePromiseRef) : IO Unit := do
  ensureSameRuntime runtime promise.runtime "WebSocketMessagePromiseRef"
  ffiRuntimeWebSocketMessagePromiseReleaseImpl runtime.handle promise.handle

@[inline] def webSocketReceive (runtime : Runtime) (webSocket : WebSocket) : IO WebSocketMessage := do
  let promise ← runtime.webSocketReceiveStart webSocket
  runtime.webSocketMessagePromiseAwait promise

@[inline] def webSocketReceiveWithMax (runtime : Runtime) (webSocket : WebSocket)
    (maxBytes : UInt32) : IO WebSocketMessage := do
  ensureSameRuntime runtime webSocket.runtime "WebSocket"
  let (tag, closeCode, text, bytes) ←
    ffiRuntimeWebSocketReceiveWithMaxImpl runtime.handle webSocket.handle maxBytes
  decodeWebSocketMessage tag closeCode text bytes

@[inline] def webSocketCloseStart (runtime : Runtime) (webSocket : WebSocket)
    (code : UInt16) (reason : String := "") : IO PromiseRef := do
  ensureSameRuntime runtime webSocket.runtime "WebSocket"
  return {
    runtime := runtime
    handle := (← ffiRuntimeWebSocketCloseStartImpl
      runtime.handle webSocket.handle code.toUInt32 reason)
  }

@[inline] def webSocketClose (runtime : Runtime) (webSocket : WebSocket)
    (code : UInt16) (reason : String := "") : IO Unit := do
  let promise ← runtime.webSocketCloseStart webSocket code reason
  ffiRuntimePromiseAwaitImpl runtime.handle promise.handle

@[inline] def webSocketDisconnect (runtime : Runtime) (webSocket : WebSocket) : IO Unit := do
  ensureSameRuntime runtime webSocket.runtime "WebSocket"
  ffiRuntimeWebSocketDisconnectImpl runtime.handle webSocket.handle

@[inline] def webSocketAbort (runtime : Runtime) (webSocket : WebSocket) : IO Unit := do
  ensureSameRuntime runtime webSocket.runtime "WebSocket"
  ffiRuntimeWebSocketAbortImpl runtime.handle webSocket.handle

@[inline] def newWebSocketPipe (runtime : Runtime) : IO (WebSocket × WebSocket) := do
  let (first, second) ← ffiRuntimeNewWebSocketPipeImpl runtime.handle
  return (
    { runtime := runtime, handle := first },
    { runtime := runtime, handle := second }
  )

@[inline] def listenEndpoint (runtime : Runtime) (endpoint : Endpoint) : IO Listener :=
  runtime.listen endpoint.address endpoint.portHint

@[inline] def connectEndpoint (runtime : Runtime) (endpoint : Endpoint) : IO Connection :=
  runtime.connect endpoint.address endpoint.portHint

@[inline] def connectStartEndpoint (runtime : Runtime) (endpoint : Endpoint) :
    IO ConnectionPromiseRef :=
  runtime.connectStart endpoint.address endpoint.portHint

@[inline] def connectAsTaskEndpoint (runtime : Runtime) (endpoint : Endpoint) :
    IO (Task (Except IO.Error Connection)) :=
  runtime.connectAsTask endpoint.address endpoint.portHint

@[inline] def connectAsPromiseEndpoint (runtime : Runtime) (endpoint : Endpoint) :
    IO (Capnp.Async.Promise Connection) :=
  runtime.connectAsPromise endpoint.address endpoint.portHint

@[inline] def httpRequestEndpoint (runtime : Runtime) (method : HttpMethod)
    (endpoint : HttpEndpoint) (path : String) (body : ByteArray := ByteArray.empty) :
    IO HttpResponse :=
  runtime.httpRequest method endpoint.host path body endpoint.port

@[inline] def httpRequestWithHeadersEndpoint (runtime : Runtime) (method : HttpMethod)
    (endpoint : HttpEndpoint) (path : String) (requestHeaders : Array HttpHeader)
    (body : ByteArray := ByteArray.empty) : IO HttpResponseEx :=
  runtime.httpRequestWithHeaders method endpoint.host path requestHeaders body endpoint.port

@[inline] def webSocketConnectEndpoint (runtime : Runtime) (endpoint : HttpEndpoint)
    (path : String) : IO WebSocket :=
  runtime.webSocketConnect endpoint.host path endpoint.port

@[inline] def webSocketConnectWithHeadersEndpoint (runtime : Runtime) (endpoint : HttpEndpoint)
    (path : String) (requestHeaders : Array HttpHeader) : IO WebSocket :=
  runtime.webSocketConnectWithHeaders endpoint.host path requestHeaders endpoint.port

@[inline] def httpServerListenEndpoint (runtime : Runtime) (endpoint : Endpoint) :
    IO HttpServer :=
  runtime.httpServerListen endpoint.address endpoint.portHint

@[inline] def httpServerListenSecureEndpoint (runtime : Runtime) (endpoint : Endpoint) :
    IO HttpServer :=
  runtime.httpServerListenSecure endpoint.address endpoint.portHint

@[inline] def httpServerListenWithConfigEndpoint (runtime : Runtime) (endpoint : Endpoint)
    (config : HttpServerConfig) : IO HttpServer :=
  runtime.httpServerListenWithConfig endpoint.address config endpoint.portHint

@[inline] def httpServerListenSecureWithConfigEndpoint (runtime : Runtime) (endpoint : Endpoint)
    (config : HttpServerConfig) : IO HttpServer :=
  runtime.httpServerListenSecureWithConfig endpoint.address config endpoint.portHint

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

@[inline] def thenStart (first second : PromiseRef) : IO PromiseRef := do
  first.runtime.promiseThenStart first second

@[inline] def catchStart (promise fallback : PromiseRef) : IO PromiseRef := do
  promise.runtime.promiseCatchStart promise fallback

@[inline] def allStart (first : PromiseRef) (rest : Array PromiseRef := #[]) :
    IO PromiseRef := do
  first.runtime.promiseAllStart (#[first] ++ rest)

@[inline] def raceStart (first : PromiseRef) (rest : Array PromiseRef := #[]) :
    IO PromiseRef := do
  first.runtime.promiseRaceStart (#[first] ++ rest)

@[inline] def «then» (first second : PromiseRef) : IO PromiseRef := do
  first.thenStart second

@[inline] def «catch» (promise fallback : PromiseRef) : IO PromiseRef := do
  promise.catchStart fallback

@[inline] def all (first : PromiseRef) (rest : Array PromiseRef := #[]) : IO PromiseRef := do
  first.allStart rest

@[inline] def race (first : PromiseRef) (rest : Array PromiseRef := #[]) : IO PromiseRef := do
  first.raceStart rest

@[inline] def thenAwait (first second : PromiseRef) : IO Unit := do
  let chained ← first.thenStart second
  chained.await

@[inline] def catchAwait (promise fallback : PromiseRef) : IO Unit := do
  let recovered ← promise.catchStart fallback
  recovered.await

@[inline] def allAwait (first : PromiseRef) (rest : Array PromiseRef := #[]) : IO Unit := do
  let allPromise ← first.allStart rest
  allPromise.await

@[inline] def raceAwait (first : PromiseRef) (rest : Array PromiseRef := #[]) : IO Unit := do
  let raced ← first.raceStart rest
  raced.await

@[inline] def cancelAndRelease (promise : PromiseRef) : IO Unit := do
  promise.cancel
  promise.release

instance : Capnp.Async.Awaitable PromiseRef Unit where
  await := PromiseRef.await

instance : Capnp.Async.Cancelable PromiseRef where
  cancel := PromiseRef.cancel

instance : Capnp.Async.Releasable PromiseRef where
  release := PromiseRef.release

@[inline] def awaitAsTask (promise : PromiseRef) : IO (Task (Except IO.Error Unit)) :=
  Capnp.Async.awaitAsTask promise

def toIOPromise (promise : PromiseRef) : IO (IO.Promise (Except String Unit)) := do
  Capnp.Async.toIOPromise promise

end PromiseRef

namespace HttpResponsePromiseRef

@[inline] def await (promise : HttpResponsePromiseRef) : IO HttpResponse := do
  let (status, responseBody) ←
    ffiRuntimeHttpResponsePromiseAwaitImpl promise.runtime.handle promise.handle
  return { status := status, body := responseBody }

@[inline] def awaitWithHeaders (promise : HttpResponsePromiseRef) : IO HttpResponseEx := do
  let (status, statusText, responseHeaderBytes, responseBody) ←
    ffiRuntimeHttpResponsePromiseAwaitWithHeadersImpl promise.runtime.handle promise.handle
  return {
    status := status
    statusText := statusText
    headers := (← decodeHeaders responseHeaderBytes)
    body := responseBody
  }

@[inline] def awaitStreamingWithHeaders (promise : HttpResponsePromiseRef) :
    IO HttpResponseStreaming := do
  let (status, statusText, responseHeaderBytes, responseBodyHandle) ←
    ffiRuntimeHttpResponsePromiseAwaitStreamingWithHeadersImpl promise.runtime.handle
      promise.handle
  return {
    status := status
    statusText := statusText
    headers := (← decodeHeaders responseHeaderBytes)
    body := { runtime := promise.runtime, handle := responseBodyHandle }
  }

@[inline] def cancel (promise : HttpResponsePromiseRef) : IO Unit :=
  ffiRuntimeHttpResponsePromiseCancelImpl promise.runtime.handle promise.handle

@[inline] def release (promise : HttpResponsePromiseRef) : IO Unit :=
  ffiRuntimeHttpResponsePromiseReleaseImpl promise.runtime.handle promise.handle

@[inline] def awaitAndRelease (promise : HttpResponsePromiseRef) : IO HttpResponse := do
  promise.await

instance : Capnp.Async.Awaitable HttpResponsePromiseRef HttpResponse where
  await := HttpResponsePromiseRef.await

instance : Capnp.Async.Cancelable HttpResponsePromiseRef where
  cancel := HttpResponsePromiseRef.cancel

instance : Capnp.Async.Releasable HttpResponsePromiseRef where
  release := HttpResponsePromiseRef.release

@[inline] def awaitAsTask (promise : HttpResponsePromiseRef) :
    IO (Task (Except IO.Error HttpResponse)) :=
  Capnp.Async.awaitAsTask promise

def toIOPromise (promise : HttpResponsePromiseRef) :
    IO (IO.Promise (Except String HttpResponse)) := do
  Capnp.Async.toIOPromise promise

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

instance : Capnp.Async.Awaitable WebSocketPromiseRef WebSocket where
  await := WebSocketPromiseRef.await

instance : Capnp.Async.Cancelable WebSocketPromiseRef where
  cancel := WebSocketPromiseRef.cancel

instance : Capnp.Async.Releasable WebSocketPromiseRef where
  release := WebSocketPromiseRef.release

@[inline] def awaitAsTask (promise : WebSocketPromiseRef) :
    IO (Task (Except IO.Error WebSocket)) :=
  Capnp.Async.awaitAsTask promise

def toIOPromise (promise : WebSocketPromiseRef) :
    IO (IO.Promise (Except String WebSocket)) := do
  Capnp.Async.toIOPromise promise

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

instance : Capnp.Async.Awaitable WebSocketMessagePromiseRef WebSocketMessage where
  await := WebSocketMessagePromiseRef.await

instance : Capnp.Async.Cancelable WebSocketMessagePromiseRef where
  cancel := WebSocketMessagePromiseRef.cancel

instance : Capnp.Async.Releasable WebSocketMessagePromiseRef where
  release := WebSocketMessagePromiseRef.release

@[inline] def awaitAsTask (promise : WebSocketMessagePromiseRef) :
    IO (Task (Except IO.Error WebSocketMessage)) :=
  Capnp.Async.awaitAsTask promise

def toIOPromise (promise : WebSocketMessagePromiseRef) :
    IO (IO.Promise (Except String WebSocketMessage)) := do
  Capnp.Async.toIOPromise promise

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

@[inline] def acceptAsTask (listener : Listener) :
    IO (Task (Except IO.Error Connection)) := do
  let pending ← listener.acceptStart
  IO.asTask do
    return {
      runtime := listener.runtime
      handle := (← ffiRuntimeConnectionPromiseAwaitImpl listener.runtime.handle pending.handle)
    }

@[inline] def acceptAsPromise (listener : Listener) :
    IO (Capnp.Async.Promise Connection) := do
  pure (Capnp.Async.Promise.ofTask (← listener.acceptAsTask))

@[inline] def acceptWithTimeoutNanos? (listener : Listener)
    (timeoutNanos : UInt64) : IO (Option Connection) := do
  let pending ← listener.acceptStart
  let (hasValue, handle) ←
    ffiRuntimeConnectionPromiseAwaitWithTimeoutImpl
      listener.runtime.handle pending.handle timeoutNanos
  if hasValue then
    return some { runtime := listener.runtime, handle := handle }
  else
    return none

@[inline] def acceptWithTimeoutMillis? (listener : Listener)
    (timeoutMillis : UInt32) : IO (Option Connection) :=
  listener.acceptWithTimeoutNanos? (millisToNanos timeoutMillis)

@[inline] def withAccept (listener : Listener) (action : Connection -> IO α) : IO α := do
  let connection ← listener.accept
  try
    action connection
  finally
    ffiRuntimeReleaseConnectionImpl listener.runtime.handle connection.handle

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

@[inline] def writeAsTask (connection : Connection) (bytes : ByteArray) :
    IO (Task (Except IO.Error Unit)) := do
  let pending ← connection.writeStart bytes
  pending.awaitAsTask

@[inline] def writeAsPromise (connection : Connection) (bytes : ByteArray) :
    IO (Capnp.Async.Promise Unit) := do
  pure (Capnp.Async.Promise.ofTask (← connection.writeAsTask bytes))

@[inline] def read (connection : Connection) (minBytes maxBytes : UInt32) : IO ByteArray :=
  ffiRuntimeConnectionReadImpl connection.runtime.handle connection.handle minBytes maxBytes

@[inline] def readStart (connection : Connection) (minBytes maxBytes : UInt32) :
    IO BytesPromiseRef := do
  return {
    runtime := connection.runtime
    handle := (← ffiRuntimeConnectionReadStartImpl
      connection.runtime.handle connection.handle minBytes maxBytes)
  }

@[inline] def readAsTask (connection : Connection) (minBytes maxBytes : UInt32) :
    IO (Task (Except IO.Error ByteArray)) := do
  let pending ← connection.readStart minBytes maxBytes
  IO.asTask do
    ffiRuntimeBytesPromiseAwaitImpl connection.runtime.handle pending.handle

@[inline] def readAsPromise (connection : Connection) (minBytes maxBytes : UInt32) :
    IO (Capnp.Async.Promise ByteArray) := do
  pure (Capnp.Async.Promise.ofTask (← connection.readAsTask minBytes maxBytes))

@[inline] def shutdownWrite (connection : Connection) : IO Unit :=
  ffiRuntimeConnectionShutdownWriteImpl connection.runtime.handle connection.handle

@[inline] def shutdownWriteStart (connection : Connection) : IO PromiseRef := do
  return {
    runtime := connection.runtime
    handle := (← ffiRuntimeConnectionShutdownWriteStartImpl
      connection.runtime.handle connection.handle)
  }

@[inline] def shutdownWriteAsTask (connection : Connection) :
    IO (Task (Except IO.Error Unit)) := do
  let pending ← connection.shutdownWriteStart
  pending.awaitAsTask

@[inline] def shutdownWriteAsPromise (connection : Connection) :
    IO (Capnp.Async.Promise Unit) := do
  pure (Capnp.Async.Promise.ofTask (← connection.shutdownWriteAsTask))

@[inline] def writeAndShutdownWrite (connection : Connection) (bytes : ByteArray) : IO Unit := do
  connection.write bytes
  connection.shutdownWrite

@[inline] def readAll (connection : Connection) (chunkSize : UInt32 := 0x1000) : IO ByteArray := do
  if chunkSize == 0 then
    throw (IO.userError "Connection.readAll requires chunkSize > 0")
  let mut out := ByteArray.empty
  let mut done := false
  while !done do
    let chunk ← connection.read (1 : UInt32) chunkSize
    if chunk.size == 0 then
      done := true
    else
      out := appendBytes out chunk
  pure out

@[inline] def pipeTo (source target : Connection) (chunkSize : UInt32 := 0x1000) : IO UInt64 := do
  if source.runtime.handle != target.runtime.handle then
    throw (IO.userError "Connection.pipeTo requires both connections to share the same runtime")
  if chunkSize == 0 then
    throw (IO.userError "Connection.pipeTo requires chunkSize > 0")
  let mut copied : UInt64 := 0
  let mut done := false
  while !done do
    let chunk ← source.read (1 : UInt32) chunkSize
    if chunk.size == 0 then
      done := true
    else
      target.write chunk
      copied := copied + chunk.size.toUInt64
  pure copied

@[inline] def pipeToAndShutdownWrite (source target : Connection)
    (chunkSize : UInt32 := 0x1000) : IO UInt64 := do
  let copied ← source.pipeTo target chunkSize
  target.shutdownWrite
  pure copied

@[inline] def whenWriteDisconnectedStart (connection : Connection) : IO PromiseRef := do
  return {
    runtime := connection.runtime
    handle := (← ffiRuntimeConnectionWhenWriteDisconnectedStartImpl
      connection.runtime.handle connection.handle)
  }

@[inline] def whenWriteDisconnectedAsTask (connection : Connection) :
    IO (Task (Except IO.Error Unit)) := do
  let pending ← connection.whenWriteDisconnectedStart
  pending.awaitAsTask

@[inline] def whenWriteDisconnectedAsPromise (connection : Connection) :
    IO (Capnp.Async.Promise Unit) := do
  pure (Capnp.Async.Promise.ofTask (← connection.whenWriteDisconnectedAsTask))

@[inline] def abortRead (connection : Connection) : IO Unit :=
  ffiRuntimeConnectionAbortReadImpl connection.runtime.handle connection.handle

@[inline] def abortWrite (connection : Connection)
    (reason : String := "Capnp.KjAsync connection abortWrite") : IO Unit :=
  ffiRuntimeConnectionAbortWriteImpl connection.runtime.handle connection.handle reason

@[inline] def dupFd? (connection : Connection) : IO (Option UInt32) := do
  let (hasFd, fd) ← ffiRuntimeConnectionDupFdImpl connection.runtime.handle connection.handle
  if hasFd then
    return some fd
  else
    return none

end Connection

namespace ConnectionPromiseRef

private def cancelAndReleaseBestEffort (promise : ConnectionPromiseRef) : IO Unit := do
  try
    ffiRuntimeConnectionPromiseCancelImpl promise.runtime.handle promise.handle
  catch _ =>
    pure ()
  try
    ffiRuntimeConnectionPromiseReleaseImpl promise.runtime.handle promise.handle
  catch _ =>
    pure ()

@[inline] def await (promise : ConnectionPromiseRef) : IO Connection := do
  try
    return {
      runtime := promise.runtime
      handle := (← ffiRuntimeConnectionPromiseAwaitImpl promise.runtime.handle promise.handle)
    }
  catch err =>
    cancelAndReleaseBestEffort promise
    throw err

@[inline] def awaitWithTimeoutNanos? (promise : ConnectionPromiseRef)
    (timeoutNanos : UInt64) : IO (Option Connection) := do
  try
    let (hasValue, handle) ←
      ffiRuntimeConnectionPromiseAwaitWithTimeoutImpl
        promise.runtime.handle promise.handle timeoutNanos
    if hasValue then
      return some {
        runtime := promise.runtime
        handle := handle
      }
    else
      return none
  catch err =>
    cancelAndReleaseBestEffort promise
    throw err

@[inline] def awaitWithTimeoutMillis? (promise : ConnectionPromiseRef)
    (timeoutMillis : UInt32) : IO (Option Connection) :=
  promise.awaitWithTimeoutNanos? (millisToNanos timeoutMillis)

@[inline] def cancel (promise : ConnectionPromiseRef) : IO Unit :=
  ffiRuntimeConnectionPromiseCancelImpl promise.runtime.handle promise.handle

@[inline] def release (promise : ConnectionPromiseRef) : IO Unit :=
  ffiRuntimeConnectionPromiseReleaseImpl promise.runtime.handle promise.handle

@[inline] def awaitAndRelease (promise : ConnectionPromiseRef) : IO Connection := do
  promise.await

instance : Capnp.Async.Awaitable ConnectionPromiseRef Connection where
  await := ConnectionPromiseRef.await

instance : Capnp.Async.Cancelable ConnectionPromiseRef where
  cancel := ConnectionPromiseRef.cancel

instance : Capnp.Async.Releasable ConnectionPromiseRef where
  release := ConnectionPromiseRef.release

@[inline] def awaitAsTask (promise : ConnectionPromiseRef) :
    IO (Task (Except IO.Error Connection)) :=
  Capnp.Async.awaitAsTask promise

@[inline] def toPromise (promise : ConnectionPromiseRef) :
    IO (Capnp.Async.Promise Connection) := do
  pure (Capnp.Async.Promise.ofTask (← promise.awaitAsTask))

def toIOPromise (promise : ConnectionPromiseRef) :
    IO (IO.Promise (Except String Connection)) := do
  Capnp.Async.toIOPromise promise

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

instance : Capnp.Async.Awaitable BytesPromiseRef ByteArray where
  await := BytesPromiseRef.await

instance : Capnp.Async.Cancelable BytesPromiseRef where
  cancel := BytesPromiseRef.cancel

instance : Capnp.Async.Releasable BytesPromiseRef where
  release := BytesPromiseRef.release

@[inline] def awaitAsTask (promise : BytesPromiseRef) :
    IO (Task (Except IO.Error ByteArray)) :=
  Capnp.Async.awaitAsTask promise

def toIOPromise (promise : BytesPromiseRef) :
    IO (IO.Promise (Except String ByteArray)) := do
  Capnp.Async.toIOPromise promise

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

instance : Capnp.Async.Awaitable UInt32PromiseRef UInt32 where
  await := UInt32PromiseRef.await

instance : Capnp.Async.Cancelable UInt32PromiseRef where
  cancel := UInt32PromiseRef.cancel

instance : Capnp.Async.Releasable UInt32PromiseRef where
  release := UInt32PromiseRef.release

@[inline] def awaitAsTask (promise : UInt32PromiseRef) :
    IO (Task (Except IO.Error UInt32)) :=
  Capnp.Async.awaitAsTask promise

def toIOPromise (promise : UInt32PromiseRef) :
    IO (IO.Promise (Except String UInt32)) := do
  Capnp.Async.toIOPromise promise

end UInt32PromiseRef

namespace TaskSetRef

@[inline] def release (taskSet : TaskSetRef) : IO Unit :=
  ffiRuntimeTaskSetReleaseImpl taskSet.runtime.handle taskSet.handle

@[inline] def addPromise (taskSet : TaskSetRef) (promise : PromiseRef) : IO Unit :=
  if taskSet.runtime.handle != promise.runtime.handle then
    throw (IO.userError "PromiseRef belongs to a different Capnp.KjAsync runtime")
  else
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
    (maxBytes : UInt32 := 0x2000) : IO (String × ByteArray) :=
  ffiRuntimeDatagramReceiveImpl port.runtime.handle port.handle maxBytes

@[inline] def receiveStart (port : DatagramPort)
    (maxBytes : UInt32 := 0x2000) : IO DatagramReceivePromiseRef := do
  return {
    runtime := port.runtime
    handle := (← ffiRuntimeDatagramReceiveStartImpl port.runtime.handle port.handle maxBytes)
  }

@[inline] def sendAsTask (port : DatagramPort) (address : String)
    (bytes : ByteArray) (portHint : UInt32 := 0) :
    IO (Task (Except IO.Error UInt32)) := do
  let promise ← port.sendStart address bytes portHint
  promise.awaitAsTask

@[inline] def sendAsPromise (port : DatagramPort) (address : String)
    (bytes : ByteArray) (portHint : UInt32 := 0) :
    IO (Capnp.Async.Promise UInt32) := do
  pure (Capnp.Async.Promise.ofTask (← port.sendAsTask address bytes portHint))

@[inline] def receiveAsTask (port : DatagramPort) (maxBytes : UInt32 := 0x2000) :
    IO (Task (Except IO.Error (String × ByteArray))) := do
  let promise ← port.receiveStart maxBytes
  IO.asTask do
    ffiRuntimeDatagramReceivePromiseAwaitImpl port.runtime.handle promise.handle

@[inline] def receiveAsPromise (port : DatagramPort) (maxBytes : UInt32 := 0x2000) :
    IO (Capnp.Async.Promise (String × ByteArray)) := do
  pure (Capnp.Async.Promise.ofTask (← port.receiveAsTask maxBytes))

@[inline] def sendAwait (port : DatagramPort) (address : String)
    (bytes : ByteArray) (portHint : UInt32 := 0) : IO UInt32 := do
  let promise ← port.sendStart address bytes portHint
  promise.await

@[inline] def receiveMany (port : DatagramPort) (count : UInt32)
    (maxBytes : UInt32 := 0x2000) : IO (Array (String × ByteArray)) := do
  let mut remaining := count
  let mut out : Array (String × ByteArray) := #[]
  while remaining != 0 do
    out := out.push (← port.receive maxBytes)
    remaining := remaining - 1
  pure out

@[inline] def withPort (port : DatagramPort) (action : DatagramPort -> IO α) : IO α := do
  try
    action port
  finally
    port.release

end DatagramPort

namespace DatagramPeer

@[inline] def send (peer : DatagramPeer) (bytes : ByteArray) : IO UInt32 :=
  peer.port.send peer.remoteAddress bytes peer.remotePort

@[inline] def sendStart (peer : DatagramPeer) (bytes : ByteArray) : IO UInt32PromiseRef :=
  peer.port.sendStart peer.remoteAddress bytes peer.remotePort

@[inline] def sendAsTask (peer : DatagramPeer) (bytes : ByteArray) :
    IO (Task (Except IO.Error UInt32)) := do
  let promise ← peer.sendStart bytes
  promise.awaitAsTask

@[inline] def sendAsPromise (peer : DatagramPeer) (bytes : ByteArray) :
    IO (Capnp.Async.Promise UInt32) := do
  pure (Capnp.Async.Promise.ofTask (← peer.sendAsTask bytes))

@[inline] def sendAwait (peer : DatagramPeer) (bytes : ByteArray) : IO UInt32 := do
  peer.port.sendAwait peer.remoteAddress bytes peer.remotePort

@[inline] def receive (peer : DatagramPeer)
    (maxBytes : UInt32 := 0x2000) : IO (String × ByteArray) :=
  peer.port.receive maxBytes

@[inline] def receiveStart (peer : DatagramPeer)
    (maxBytes : UInt32 := 0x2000) : IO DatagramReceivePromiseRef :=
  peer.port.receiveStart maxBytes

@[inline] def receiveAsTask (peer : DatagramPeer) (maxBytes : UInt32 := 0x2000) :
    IO (Task (Except IO.Error (String × ByteArray))) :=
  peer.port.receiveAsTask maxBytes

@[inline] def receiveAsPromise (peer : DatagramPeer) (maxBytes : UInt32 := 0x2000) :
    IO (Capnp.Async.Promise (String × ByteArray)) :=
  peer.port.receiveAsPromise maxBytes

@[inline] def receiveMany (peer : DatagramPeer) (count : UInt32)
    (maxBytes : UInt32 := 0x2000) : IO (Array (String × ByteArray)) :=
  peer.port.receiveMany count maxBytes

@[inline] def release (peer : DatagramPeer) : IO Unit :=
  peer.port.release

end DatagramPeer

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

instance : Capnp.Async.Awaitable DatagramReceivePromiseRef (String × ByteArray) where
  await := DatagramReceivePromiseRef.await

instance : Capnp.Async.Cancelable DatagramReceivePromiseRef where
  cancel := DatagramReceivePromiseRef.cancel

instance : Capnp.Async.Releasable DatagramReceivePromiseRef where
  release := DatagramReceivePromiseRef.release

@[inline] def awaitAsTask (promise : DatagramReceivePromiseRef) :
    IO (Task (Except IO.Error (String × ByteArray))) :=
  Capnp.Async.awaitAsTask promise

def toIOPromise (promise : DatagramReceivePromiseRef) :
    IO (IO.Promise (Except String (String × ByteArray))) := do
  Capnp.Async.toIOPromise promise

end DatagramReceivePromiseRef

namespace HttpServer

@[inline] def release (server : HttpServer) : IO Unit :=
  ffiRuntimeHttpServerReleaseImpl server.runtime.handle server.handle

@[inline] def drainStart (server : HttpServer) : IO PromiseRef := do
  return {
    runtime := server.runtime
    handle := (← ffiRuntimeHttpServerDrainStartImpl server.runtime.handle server.handle)
  }

@[inline] def drainAsTask (server : HttpServer) : IO (Task (Except IO.Error Unit)) := do
  let promise ← server.drainStart
  promise.awaitAsTask

@[inline] def drainAsPromise (server : HttpServer) : IO (Capnp.Async.Promise Unit) := do
  pure (Capnp.Async.Promise.ofTask (← server.drainAsTask))

@[inline] def drain (server : HttpServer) : IO Unit :=
  ffiRuntimeHttpServerDrainImpl server.runtime.handle server.handle

@[inline] def pollRequestStreaming? (server : HttpServer) : IO (Option HttpServerRequest) := do
  let (hasRequest, payload) ← ffiRuntimeHttpServerPollRequestImpl server.runtime.handle server.handle
  if hasRequest then
    return some (← decodeHttpServerRequest server.runtime payload)
  else
    return none

@[inline] def pollRequest? (server : HttpServer) : IO (Option HttpServerRequest) := do
  match (← server.pollRequestStreaming?) with
  | some request => return some (← drainHttpServerRequestBody request)
  | none => return none

@[inline] def respond (server : HttpServer) (requestId : UInt32) (status : UInt32)
    (statusText : String) (responseHeaders : Array HttpHeader := #[])
    (body : ByteArray := ByteArray.empty) : IO Unit :=
  ffiRuntimeHttpServerRespondImpl server.runtime.handle server.handle requestId status statusText
    (encodeHeaders responseHeaders) body

@[inline] def respondWebSocket (server : HttpServer) (requestId : UInt32)
    (responseHeaders : Array HttpHeader := #[]) : IO WebSocket := do
  return {
    runtime := server.runtime
    handle := (← ffiRuntimeHttpServerRespondWebSocketImpl
      server.runtime.handle server.handle requestId (encodeHeaders responseHeaders))
  }

@[inline] def respondStartStreaming (server : HttpServer) (requestId : UInt32)
    (status : UInt32) (statusText : String) (responseHeaders : Array HttpHeader := #[]) :
    IO HttpServerResponseBody := do
  return {
    runtime := server.runtime
    handle := (← ffiRuntimeHttpServerRespondStartStreamingImpl server.runtime.handle server.handle
      requestId status statusText (encodeHeaders responseHeaders))
  }

end HttpServer

namespace HttpRequestBody

@[inline] def writeStart (requestBody : HttpRequestBody) (bytes : ByteArray) : IO PromiseRef := do
  return {
    runtime := requestBody.runtime
    handle := (← ffiRuntimeHttpRequestBodyWriteStartImpl
      requestBody.runtime.handle requestBody.handle bytes)
  }

@[inline] def writeAsTask (requestBody : HttpRequestBody) (bytes : ByteArray) :
    IO (Task (Except IO.Error Unit)) := do
  let promise ← requestBody.writeStart bytes
  promise.awaitAsTask

@[inline] def writeAsPromise (requestBody : HttpRequestBody) (bytes : ByteArray) :
    IO (Capnp.Async.Promise Unit) := do
  pure (Capnp.Async.Promise.ofTask (← requestBody.writeAsTask bytes))

@[inline] def write (requestBody : HttpRequestBody) (bytes : ByteArray) : IO Unit := do
  let promise ← requestBody.writeStart bytes
  promise.await

@[inline] def finishStart (requestBody : HttpRequestBody) : IO PromiseRef := do
  return {
    runtime := requestBody.runtime
    handle := (← ffiRuntimeHttpRequestBodyFinishStartImpl
      requestBody.runtime.handle requestBody.handle)
  }

@[inline] def finishAsTask (requestBody : HttpRequestBody) :
    IO (Task (Except IO.Error Unit)) := do
  let promise ← requestBody.finishStart
  promise.awaitAsTask

@[inline] def finishAsPromise (requestBody : HttpRequestBody) :
    IO (Capnp.Async.Promise Unit) := do
  pure (Capnp.Async.Promise.ofTask (← requestBody.finishAsTask))

@[inline] def finish (requestBody : HttpRequestBody) : IO Unit := do
  let promise ← requestBody.finishStart
  promise.await

@[inline] def release (requestBody : HttpRequestBody) : IO Unit :=
  ffiRuntimeHttpRequestBodyReleaseImpl requestBody.runtime.handle requestBody.handle

end HttpRequestBody

namespace HttpResponseBody

@[inline] def readStart (responseBody : HttpResponseBody) (minBytes maxBytes : UInt32) :
    IO BytesPromiseRef := do
  return {
    runtime := responseBody.runtime
    handle := (← ffiRuntimeHttpResponseBodyReadStartImpl
      responseBody.runtime.handle responseBody.handle minBytes maxBytes)
  }

@[inline] def readAsTask (responseBody : HttpResponseBody) (minBytes maxBytes : UInt32) :
    IO (Task (Except IO.Error ByteArray)) := do
  let promise ← responseBody.readStart minBytes maxBytes
  promise.awaitAsTask

@[inline] def readAsPromise (responseBody : HttpResponseBody) (minBytes maxBytes : UInt32) :
    IO (Capnp.Async.Promise ByteArray) := do
  pure (Capnp.Async.Promise.ofTask (← responseBody.readAsTask minBytes maxBytes))

@[inline] def read (responseBody : HttpResponseBody) (minBytes maxBytes : UInt32) :
    IO ByteArray :=
  ffiRuntimeHttpResponseBodyReadImpl responseBody.runtime.handle responseBody.handle
    minBytes maxBytes

@[inline] def release (responseBody : HttpResponseBody) : IO Unit :=
  ffiRuntimeHttpResponseBodyReleaseImpl responseBody.runtime.handle responseBody.handle

end HttpResponseBody

namespace HttpServerRequestBody

@[inline] def readStart (requestBody : HttpServerRequestBody) (minBytes maxBytes : UInt32) :
    IO BytesPromiseRef := do
  return {
    runtime := requestBody.runtime
    handle := (← ffiRuntimeHttpServerRequestBodyReadStartImpl requestBody.runtime.handle
      requestBody.handle minBytes maxBytes)
  }

@[inline] def readAsTask (requestBody : HttpServerRequestBody) (minBytes maxBytes : UInt32) :
    IO (Task (Except IO.Error ByteArray)) := do
  let promise ← requestBody.readStart minBytes maxBytes
  promise.awaitAsTask

@[inline] def readAsPromise (requestBody : HttpServerRequestBody) (minBytes maxBytes : UInt32) :
    IO (Capnp.Async.Promise ByteArray) := do
  pure (Capnp.Async.Promise.ofTask (← requestBody.readAsTask minBytes maxBytes))

@[inline] def read (requestBody : HttpServerRequestBody) (minBytes maxBytes : UInt32) :
    IO ByteArray :=
  ffiRuntimeHttpServerRequestBodyReadImpl requestBody.runtime.handle requestBody.handle
    minBytes maxBytes

@[inline] def release (requestBody : HttpServerRequestBody) : IO Unit :=
  ffiRuntimeHttpServerRequestBodyReleaseImpl requestBody.runtime.handle requestBody.handle

end HttpServerRequestBody

namespace HttpServerResponseBody

@[inline] def writeStart (responseBody : HttpServerResponseBody) (bytes : ByteArray) :
    IO PromiseRef := do
  return {
    runtime := responseBody.runtime
    handle := (← ffiRuntimeHttpServerResponseBodyWriteStartImpl responseBody.runtime.handle
      responseBody.handle bytes)
  }

@[inline] def writeAsTask (responseBody : HttpServerResponseBody) (bytes : ByteArray) :
    IO (Task (Except IO.Error Unit)) := do
  let promise ← responseBody.writeStart bytes
  promise.awaitAsTask

@[inline] def writeAsPromise (responseBody : HttpServerResponseBody) (bytes : ByteArray) :
    IO (Capnp.Async.Promise Unit) := do
  pure (Capnp.Async.Promise.ofTask (← responseBody.writeAsTask bytes))

@[inline] def write (responseBody : HttpServerResponseBody) (bytes : ByteArray) : IO Unit := do
  let promise ← responseBody.writeStart bytes
  promise.await

@[inline] def finishStart (responseBody : HttpServerResponseBody) : IO PromiseRef := do
  return {
    runtime := responseBody.runtime
    handle := (← ffiRuntimeHttpServerResponseBodyFinishStartImpl responseBody.runtime.handle
      responseBody.handle)
  }

@[inline] def finishAsTask (responseBody : HttpServerResponseBody) :
    IO (Task (Except IO.Error Unit)) := do
  let promise ← responseBody.finishStart
  promise.awaitAsTask

@[inline] def finishAsPromise (responseBody : HttpServerResponseBody) :
    IO (Capnp.Async.Promise Unit) := do
  pure (Capnp.Async.Promise.ofTask (← responseBody.finishAsTask))

@[inline] def finish (responseBody : HttpServerResponseBody) : IO Unit := do
  let promise ← responseBody.finishStart
  promise.await

@[inline] def release (responseBody : HttpServerResponseBody) : IO Unit :=
  ffiRuntimeHttpServerResponseBodyReleaseImpl responseBody.runtime.handle responseBody.handle

end HttpServerResponseBody

namespace WebSocket

@[inline] def release (webSocket : WebSocket) : IO Unit :=
  ffiRuntimeWebSocketReleaseImpl webSocket.runtime.handle webSocket.handle

@[inline] def sendTextStart (webSocket : WebSocket) (text : String) : IO PromiseRef := do
  return {
    runtime := webSocket.runtime
    handle := (← ffiRuntimeWebSocketSendTextStartImpl
      webSocket.runtime.handle webSocket.handle text)
  }

@[inline] def sendTextAsTask (webSocket : WebSocket) (text : String) :
    IO (Task (Except IO.Error Unit)) := do
  let promise ← webSocket.sendTextStart text
  promise.awaitAsTask

@[inline] def sendTextAsPromise (webSocket : WebSocket) (text : String) :
    IO (Capnp.Async.Promise Unit) := do
  pure (Capnp.Async.Promise.ofTask (← webSocket.sendTextAsTask text))

@[inline] def sendText (webSocket : WebSocket) (text : String) : IO Unit := do
  let promise ← webSocket.sendTextStart text
  promise.await

@[inline] def sendBinaryStart (webSocket : WebSocket) (bytes : ByteArray) : IO PromiseRef := do
  return {
    runtime := webSocket.runtime
    handle := (← ffiRuntimeWebSocketSendBinaryStartImpl
      webSocket.runtime.handle webSocket.handle bytes)
  }

@[inline] def sendBinaryAsTask (webSocket : WebSocket) (bytes : ByteArray) :
    IO (Task (Except IO.Error Unit)) := do
  let promise ← webSocket.sendBinaryStart bytes
  promise.awaitAsTask

@[inline] def sendBinaryAsPromise (webSocket : WebSocket) (bytes : ByteArray) :
    IO (Capnp.Async.Promise Unit) := do
  pure (Capnp.Async.Promise.ofTask (← webSocket.sendBinaryAsTask bytes))

@[inline] def sendBinary (webSocket : WebSocket) (bytes : ByteArray) : IO Unit := do
  let promise ← webSocket.sendBinaryStart bytes
  promise.await

@[inline] def receiveStart (webSocket : WebSocket) : IO WebSocketMessagePromiseRef := do
  return {
    runtime := webSocket.runtime
    handle := (← ffiRuntimeWebSocketReceiveStartImpl webSocket.runtime.handle webSocket.handle)
  }

@[inline] def receiveStartWithMax (webSocket : WebSocket)
    (maxBytes : UInt32) : IO WebSocketMessagePromiseRef := do
  return {
    runtime := webSocket.runtime
    handle := (← ffiRuntimeWebSocketReceiveStartWithMaxImpl webSocket.runtime.handle
      webSocket.handle maxBytes)
  }

@[inline] def receiveAsTask (webSocket : WebSocket) :
    IO (Task (Except IO.Error WebSocketMessage)) := do
  let promise ← webSocket.receiveStart
  promise.awaitAsTask

@[inline] def receiveAsPromise (webSocket : WebSocket) :
    IO (Capnp.Async.Promise WebSocketMessage) := do
  pure (Capnp.Async.Promise.ofTask (← webSocket.receiveAsTask))

@[inline] def receiveWithMaxAsTask (webSocket : WebSocket) (maxBytes : UInt32) :
    IO (Task (Except IO.Error WebSocketMessage)) := do
  let promise ← webSocket.receiveStartWithMax maxBytes
  promise.awaitAsTask

@[inline] def receiveWithMaxAsPromise (webSocket : WebSocket) (maxBytes : UInt32) :
    IO (Capnp.Async.Promise WebSocketMessage) := do
  pure (Capnp.Async.Promise.ofTask (← webSocket.receiveWithMaxAsTask maxBytes))

@[inline] def receive (webSocket : WebSocket) : IO WebSocketMessage := do
  let promise ← webSocket.receiveStart
  let (tag, closeCode, text, bytes) ←
    ffiRuntimeWebSocketMessagePromiseAwaitImpl webSocket.runtime.handle promise.handle
  decodeWebSocketMessage tag closeCode text bytes

@[inline] def receiveWithMax (webSocket : WebSocket) (maxBytes : UInt32) :
    IO WebSocketMessage := do
  let (tag, closeCode, text, bytes) ←
    ffiRuntimeWebSocketReceiveWithMaxImpl webSocket.runtime.handle webSocket.handle maxBytes
  decodeWebSocketMessage tag closeCode text bytes

@[inline] def closeStart (webSocket : WebSocket) (code : UInt16)
    (reason : String := "") : IO PromiseRef := do
  return {
    runtime := webSocket.runtime
    handle := (← ffiRuntimeWebSocketCloseStartImpl webSocket.runtime.handle
      webSocket.handle code.toUInt32 reason)
  }

@[inline] def closeAsTask (webSocket : WebSocket) (code : UInt16)
    (reason : String := "") : IO (Task (Except IO.Error Unit)) := do
  let promise ← webSocket.closeStart code reason
  promise.awaitAsTask

@[inline] def closeAsPromise (webSocket : WebSocket) (code : UInt16)
    (reason : String := "") : IO (Capnp.Async.Promise Unit) := do
  pure (Capnp.Async.Promise.ofTask (← webSocket.closeAsTask code reason))

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

@[inline] def enableTls : RuntimeM Unit := do
  Runtime.enableTls (← runtime)

@[inline] def configureTls (config : TlsConfig) : RuntimeM Unit := do
  Runtime.configureTls (← runtime) config

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

@[inline] def connectAsTask (address : String) (portHint : UInt32 := 0) :
    RuntimeM (Task (Except IO.Error Connection)) := do
  Runtime.connectAsTask (← runtime) address portHint

@[inline] def connectAsPromise (address : String) (portHint : UInt32 := 0) :
    RuntimeM (Capnp.Async.Promise Connection) := do
  Runtime.connectAsPromise (← runtime) address portHint

@[inline] def connectWithTimeoutNanos? (address : String) (timeoutNanos : UInt64)
    (portHint : UInt32 := 0) : RuntimeM (Option Connection) := do
  Runtime.connectWithTimeoutNanos? (← runtime) address timeoutNanos portHint

@[inline] def connectWithTimeoutMillis? (address : String) (timeoutMillis : UInt32)
    (portHint : UInt32 := 0) : RuntimeM (Option Connection) := do
  Runtime.connectWithTimeoutMillis? (← runtime) address timeoutMillis portHint

@[inline] def connectWithRetry (address : String) (attempts : UInt32)
    (retryDelayMs : UInt32) (portHint : UInt32 := 0) : RuntimeM Connection := do
  Runtime.connectWithRetry (← runtime) address attempts retryDelayMs portHint

@[inline] def connectWithRetryAsTask (address : String) (attempts : UInt32)
    (retryDelayMs : UInt32) (portHint : UInt32 := 0) :
    RuntimeM (Task (Except IO.Error Connection)) := do
  Runtime.connectWithRetryAsTask (← runtime) address attempts retryDelayMs portHint

@[inline] def connectWithRetryAsPromise (address : String) (attempts : UInt32)
    (retryDelayMs : UInt32) (portHint : UInt32 := 0) :
    RuntimeM (Capnp.Async.Promise Connection) := do
  Runtime.connectWithRetryAsPromise (← runtime) address attempts retryDelayMs portHint

@[inline] def withListener (address : String) (action : Listener -> RuntimeM α)
    (portHint : UInt32 := 0) : RuntimeM α := do
  let listener ← listen address portHint
  try
    action listener
  finally
    listener.release

@[inline] def withConnection (address : String) (action : Connection -> RuntimeM α)
    (portHint : UInt32 := 0) : RuntimeM α := do
  let connection ← connect address portHint
  try
    action connection
  finally
    connection.release

@[inline] def releaseListener (listener : Listener) : RuntimeM Unit := do
  Runtime.releaseListener (← runtime) listener

@[inline] def releaseConnection (connection : Connection) : RuntimeM Unit := do
  Runtime.releaseConnection (← runtime) connection

@[inline] def accept (listener : Listener) : RuntimeM Connection := do
  Runtime.listenerAccept (← runtime) listener

@[inline] def acceptStart (listener : Listener) : RuntimeM ConnectionPromiseRef := do
  Runtime.listenerAcceptStart (← runtime) listener

@[inline] def acceptWithTimeoutNanos? (listener : Listener) (timeoutNanos : UInt64) :
    RuntimeM (Option Connection) := do
  Runtime.listenerAcceptWithTimeoutNanos? (← runtime) listener timeoutNanos

@[inline] def acceptWithTimeoutMillis? (listener : Listener) (timeoutMillis : UInt32) :
    RuntimeM (Option Connection) := do
  Runtime.listenerAcceptWithTimeoutMillis? (← runtime) listener timeoutMillis

@[inline] def write (connection : Connection) (bytes : ByteArray) : RuntimeM Unit := do
  Runtime.connectionWrite (← runtime) connection bytes

@[inline] def writeStart (connection : Connection) (bytes : ByteArray) : RuntimeM PromiseRef := do
  Runtime.connectionWriteStart (← runtime) connection bytes

@[inline] def writeAsTask (connection : Connection) (bytes : ByteArray) :
    RuntimeM (Task (Except IO.Error Unit)) := do
  ensureSameRuntime (← runtime) connection.runtime "Connection"
  connection.writeAsTask bytes

@[inline] def writeAsPromise (connection : Connection) (bytes : ByteArray) :
    RuntimeM (Capnp.Async.Promise Unit) := do
  ensureSameRuntime (← runtime) connection.runtime "Connection"
  connection.writeAsPromise bytes

@[inline] def read (connection : Connection) (minBytes maxBytes : UInt32) : RuntimeM ByteArray := do
  Runtime.connectionRead (← runtime) connection minBytes maxBytes

@[inline] def readStart (connection : Connection) (minBytes maxBytes : UInt32) :
    RuntimeM BytesPromiseRef := do
  Runtime.connectionReadStart (← runtime) connection minBytes maxBytes

@[inline] def readAsTask (connection : Connection) (minBytes maxBytes : UInt32) :
    RuntimeM (Task (Except IO.Error ByteArray)) := do
  ensureSameRuntime (← runtime) connection.runtime "Connection"
  connection.readAsTask minBytes maxBytes

@[inline] def readAsPromise (connection : Connection) (minBytes maxBytes : UInt32) :
    RuntimeM (Capnp.Async.Promise ByteArray) := do
  ensureSameRuntime (← runtime) connection.runtime "Connection"
  connection.readAsPromise minBytes maxBytes

@[inline] def readAll (connection : Connection) (chunkSize : UInt32 := 0x1000) :
    RuntimeM ByteArray := do
  connection.readAll chunkSize

@[inline] def shutdownWrite (connection : Connection) : RuntimeM Unit := do
  Runtime.connectionShutdownWrite (← runtime) connection

@[inline] def shutdownWriteStart (connection : Connection) : RuntimeM PromiseRef := do
  Runtime.connectionShutdownWriteStart (← runtime) connection

@[inline] def shutdownWriteAsTask (connection : Connection) :
    RuntimeM (Task (Except IO.Error Unit)) := do
  ensureSameRuntime (← runtime) connection.runtime "Connection"
  connection.shutdownWriteAsTask

@[inline] def shutdownWriteAsPromise (connection : Connection) :
    RuntimeM (Capnp.Async.Promise Unit) := do
  ensureSameRuntime (← runtime) connection.runtime "Connection"
  connection.shutdownWriteAsPromise

@[inline] def writeAndShutdownWrite (connection : Connection) (bytes : ByteArray) :
    RuntimeM Unit := do
  connection.writeAndShutdownWrite bytes

@[inline] def pipeTo (source target : Connection) (chunkSize : UInt32 := 0x1000) :
    RuntimeM UInt64 := do
  source.pipeTo target chunkSize

@[inline] def pipeToAndShutdownWrite (source target : Connection)
    (chunkSize : UInt32 := 0x1000) : RuntimeM UInt64 := do
  source.pipeToAndShutdownWrite target chunkSize

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

@[inline] def promiseThenStart (first second : PromiseRef) : RuntimeM PromiseRef := do
  Runtime.promiseThenStart (← runtime) first second

@[inline] def promiseCatchStart (promise fallback : PromiseRef) : RuntimeM PromiseRef := do
  Runtime.promiseCatchStart (← runtime) promise fallback

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

@[inline] def connectionWhenWriteDisconnectedAsTask (connection : Connection) :
    RuntimeM (Task (Except IO.Error Unit)) := do
  ensureSameRuntime (← runtime) connection.runtime "Connection"
  connection.whenWriteDisconnectedAsTask

@[inline] def connectionWhenWriteDisconnectedAsPromise (connection : Connection) :
    RuntimeM (Capnp.Async.Promise Unit) := do
  ensureSameRuntime (← runtime) connection.runtime "Connection"
  connection.whenWriteDisconnectedAsPromise

@[inline] def connectionAbortRead (connection : Connection) : RuntimeM Unit := do
  connection.abortRead

@[inline] def connectionAbortWrite (connection : Connection)
    (reason : String := "Capnp.KjAsync connection abortWrite") : RuntimeM Unit := do
  connection.abortWrite reason

@[inline] def connectionDupFd? (connection : Connection) : RuntimeM (Option UInt32) := do
  connection.dupFd?

@[inline] def newTwoWayPipe : RuntimeM (Connection × Connection) := do
  Runtime.newTwoWayPipe (← runtime)

@[inline] def newCapabilityPipe : RuntimeM (Connection × Connection) := do
  Runtime.newCapabilityPipe (← runtime)

@[inline] def datagramBind (address : String) (portHint : UInt32 := 0) : RuntimeM DatagramPort := do
  Runtime.datagramBind (← runtime) address portHint

@[inline] def datagramBindEndpoint (endpoint : Endpoint) : RuntimeM DatagramPort := do
  Runtime.datagramBindEndpoint (← runtime) endpoint

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

@[inline] def datagramSendAsTask (port : DatagramPort) (address : String)
    (bytes : ByteArray) (portHint : UInt32 := 0) :
    RuntimeM (Task (Except IO.Error UInt32)) := do
  Runtime.datagramSendAsTask (← runtime) port address bytes portHint

@[inline] def datagramSendAsPromise (port : DatagramPort) (address : String)
    (bytes : ByteArray) (portHint : UInt32 := 0) :
    RuntimeM (Capnp.Async.Promise UInt32) := do
  Runtime.datagramSendAsPromise (← runtime) port address bytes portHint

@[inline] def datagramSendAwait (port : DatagramPort) (address : String)
    (bytes : ByteArray) (portHint : UInt32 := 0) : RuntimeM UInt32 := do
  port.sendAwait address bytes portHint

@[inline] def datagramPeerBind (localAddress remoteAddress : String) (remotePort : UInt32)
    (localPortHint : UInt32 := 0) : RuntimeM DatagramPeer := do
  Runtime.datagramPeerBind (← runtime) localAddress remoteAddress remotePort localPortHint

@[inline] def datagramPeerSend (peer : DatagramPeer) (bytes : ByteArray) : RuntimeM UInt32 := do
  peer.send bytes

@[inline] def datagramPeerSendStart (peer : DatagramPeer)
    (bytes : ByteArray) : RuntimeM UInt32PromiseRef := do
  peer.sendStart bytes

@[inline] def datagramPeerSendAsTask (peer : DatagramPeer) (bytes : ByteArray) :
    RuntimeM (Task (Except IO.Error UInt32)) := do
  ensureSameRuntime (← runtime) peer.port.runtime "DatagramPort"
  peer.sendAsTask bytes

@[inline] def datagramPeerSendAsPromise (peer : DatagramPeer) (bytes : ByteArray) :
    RuntimeM (Capnp.Async.Promise UInt32) := do
  ensureSameRuntime (← runtime) peer.port.runtime "DatagramPort"
  peer.sendAsPromise bytes

@[inline] def datagramPeerSendAwait (peer : DatagramPeer) (bytes : ByteArray) :
    RuntimeM UInt32 := do
  peer.sendAwait bytes

@[inline] def datagramPeerReceive (peer : DatagramPeer)
    (maxBytes : UInt32 := 0x2000) : RuntimeM (String × ByteArray) := do
  peer.receive maxBytes

@[inline] def datagramPeerReceiveStart (peer : DatagramPeer)
    (maxBytes : UInt32 := 0x2000) : RuntimeM DatagramReceivePromiseRef := do
  peer.receiveStart maxBytes

@[inline] def datagramPeerReceiveAsTask (peer : DatagramPeer) (maxBytes : UInt32 := 0x2000) :
    RuntimeM (Task (Except IO.Error (String × ByteArray))) := do
  ensureSameRuntime (← runtime) peer.port.runtime "DatagramPort"
  peer.receiveAsTask maxBytes

@[inline] def datagramPeerReceiveAsPromise (peer : DatagramPeer)
    (maxBytes : UInt32 := 0x2000) : RuntimeM (Capnp.Async.Promise (String × ByteArray)) := do
  ensureSameRuntime (← runtime) peer.port.runtime "DatagramPort"
  peer.receiveAsPromise maxBytes

@[inline] def datagramPeerReceiveMany (peer : DatagramPeer) (count : UInt32)
    (maxBytes : UInt32 := 0x2000) : RuntimeM (Array (String × ByteArray)) := do
  peer.receiveMany count maxBytes

@[inline] def datagramPeerRelease (peer : DatagramPeer) : RuntimeM Unit := do
  peer.release

@[inline] def awaitUInt32 (promise : UInt32PromiseRef) : RuntimeM UInt32 := do
  promise.await

@[inline] def cancelUInt32 (promise : UInt32PromiseRef) : RuntimeM Unit := do
  promise.cancel

@[inline] def releaseUInt32Promise (promise : UInt32PromiseRef) : RuntimeM Unit := do
  promise.release

@[inline] def datagramReceive (port : DatagramPort)
    (maxBytes : UInt32 := 0x2000) : RuntimeM (String × ByteArray) := do
  port.receive maxBytes

@[inline] def datagramReceiveStart (port : DatagramPort)
    (maxBytes : UInt32 := 0x2000) : RuntimeM DatagramReceivePromiseRef := do
  Runtime.datagramReceiveStart (← runtime) port maxBytes

@[inline] def datagramReceiveAsTask (port : DatagramPort) (maxBytes : UInt32 := 0x2000) :
    RuntimeM (Task (Except IO.Error (String × ByteArray))) := do
  Runtime.datagramReceiveAsTask (← runtime) port maxBytes

@[inline] def datagramReceiveAsPromise (port : DatagramPort) (maxBytes : UInt32 := 0x2000) :
    RuntimeM (Capnp.Async.Promise (String × ByteArray)) := do
  Runtime.datagramReceiveAsPromise (← runtime) port maxBytes

@[inline] def datagramReceiveMany (port : DatagramPort) (count : UInt32)
    (maxBytes : UInt32 := 0x2000) : RuntimeM (Array (String × ByteArray)) := do
  port.receiveMany count maxBytes

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

@[inline] def httpRequestWithResponseLimit (method : HttpMethod) (address : String)
    (path : String) (responseBodyLimit : UInt64) (body : ByteArray := ByteArray.empty)
    (portHint : UInt32 := 0) : RuntimeM HttpResponse := do
  Runtime.httpRequestWithResponseLimit
    (← runtime) method address path responseBodyLimit body portHint

@[inline] def httpRequestWithHeaders (method : HttpMethod) (address : String) (path : String)
    (requestHeaders : Array HttpHeader) (body : ByteArray := ByteArray.empty)
    (portHint : UInt32 := 0) : RuntimeM HttpResponseEx := do
  Runtime.httpRequestWithHeaders (← runtime) method address path requestHeaders body portHint

@[inline] def httpRequestWithHeadersSecure (method : HttpMethod) (address : String)
    (path : String) (requestHeaders : Array HttpHeader) (body : ByteArray := ByteArray.empty)
    (portHint : UInt32 := 0) : RuntimeM HttpResponseEx := do
  Runtime.httpRequestWithHeadersSecure (← runtime) method address path requestHeaders body portHint

@[inline] def httpRequestWithHeadersWithResponseLimit (method : HttpMethod) (address : String)
    (path : String) (requestHeaders : Array HttpHeader) (responseBodyLimit : UInt64)
    (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) :
    RuntimeM HttpResponseEx := do
  Runtime.httpRequestWithHeadersWithResponseLimit
    (← runtime) method address path requestHeaders responseBodyLimit body portHint

@[inline] def httpRequestWithHeadersWithResponseLimitSecure (method : HttpMethod)
    (address : String) (path : String) (requestHeaders : Array HttpHeader)
    (responseBodyLimit : UInt64) (body : ByteArray := ByteArray.empty)
    (portHint : UInt32 := 0) : RuntimeM HttpResponseEx := do
  Runtime.httpRequestWithHeadersWithResponseLimitSecure
    (← runtime) method address path requestHeaders responseBodyLimit body portHint

@[inline] def httpRequestStart (method : HttpMethod) (address : String) (path : String)
    (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) :
    RuntimeM HttpResponsePromiseRef := do
  Runtime.httpRequestStart (← runtime) method address path body portHint

@[inline] def httpRequestAsTask (method : HttpMethod) (address : String) (path : String)
    (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) :
    RuntimeM (Task (Except IO.Error HttpResponse)) := do
  Runtime.httpRequestAsTask (← runtime) method address path body portHint

@[inline] def httpRequestAsPromise (method : HttpMethod) (address : String) (path : String)
    (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) :
    RuntimeM (Capnp.Async.Promise HttpResponse) := do
  Runtime.httpRequestAsPromise (← runtime) method address path body portHint

@[inline] def httpRequestStartWithResponseLimit (method : HttpMethod) (address : String)
    (path : String) (responseBodyLimit : UInt64) (body : ByteArray := ByteArray.empty)
    (portHint : UInt32 := 0) : RuntimeM HttpResponsePromiseRef := do
  Runtime.httpRequestStartWithResponseLimit
    (← runtime) method address path responseBodyLimit body portHint

@[inline] def httpRequestStartWithHeaders (method : HttpMethod) (address : String)
    (path : String) (requestHeaders : Array HttpHeader)
    (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) :
    RuntimeM HttpResponsePromiseRef := do
  Runtime.httpRequestStartWithHeaders (← runtime) method address path requestHeaders body portHint

@[inline] def httpRequestWithHeadersAsTask (method : HttpMethod) (address : String)
    (path : String) (requestHeaders : Array HttpHeader) (body : ByteArray := ByteArray.empty)
    (portHint : UInt32 := 0) : RuntimeM (Task (Except IO.Error HttpResponseEx)) := do
  Runtime.httpRequestWithHeadersAsTask (← runtime) method address path requestHeaders body portHint

@[inline] def httpRequestWithHeadersAsPromise (method : HttpMethod) (address : String)
    (path : String) (requestHeaders : Array HttpHeader) (body : ByteArray := ByteArray.empty)
    (portHint : UInt32 := 0) : RuntimeM (Capnp.Async.Promise HttpResponseEx) := do
  Runtime.httpRequestWithHeadersAsPromise
    (← runtime) method address path requestHeaders body portHint

@[inline] def httpRequestStartWithHeadersSecure (method : HttpMethod) (address : String)
    (path : String) (requestHeaders : Array HttpHeader)
    (body : ByteArray := ByteArray.empty) (portHint : UInt32 := 0) :
    RuntimeM HttpResponsePromiseRef := do
  Runtime.httpRequestStartWithHeadersSecure
    (← runtime) method address path requestHeaders body portHint

@[inline] def httpRequestWithHeadersSecureAsTask (method : HttpMethod) (address : String)
    (path : String) (requestHeaders : Array HttpHeader) (body : ByteArray := ByteArray.empty)
    (portHint : UInt32 := 0) : RuntimeM (Task (Except IO.Error HttpResponseEx)) := do
  Runtime.httpRequestWithHeadersSecureAsTask
    (← runtime) method address path requestHeaders body portHint

@[inline] def httpRequestWithHeadersSecureAsPromise (method : HttpMethod) (address : String)
    (path : String) (requestHeaders : Array HttpHeader) (body : ByteArray := ByteArray.empty)
    (portHint : UInt32 := 0) : RuntimeM (Capnp.Async.Promise HttpResponseEx) := do
  Runtime.httpRequestWithHeadersSecureAsPromise
    (← runtime) method address path requestHeaders body portHint

@[inline] def httpRequestStartStreamingWithHeaders (method : HttpMethod) (address : String)
    (path : String) (requestHeaders : Array HttpHeader) (portHint : UInt32 := 0) :
    RuntimeM (Option HttpRequestBody × HttpResponsePromiseRef) := do
  Runtime.httpRequestStartStreamingWithHeaders
    (← runtime) method address path requestHeaders portHint

@[inline] def httpRequestStartStreamingWithHeadersSecure (method : HttpMethod)
    (address : String) (path : String) (requestHeaders : Array HttpHeader)
    (portHint : UInt32 := 0) : RuntimeM (Option HttpRequestBody × HttpResponsePromiseRef) := do
  Runtime.httpRequestStartStreamingWithHeadersSecure
    (← runtime) method address path requestHeaders portHint

@[inline] def httpRequestStartWithHeadersWithResponseLimit (method : HttpMethod)
    (address : String) (path : String) (requestHeaders : Array HttpHeader)
    (responseBodyLimit : UInt64) (body : ByteArray := ByteArray.empty)
    (portHint : UInt32 := 0) : RuntimeM HttpResponsePromiseRef := do
  Runtime.httpRequestStartWithHeadersWithResponseLimit
    (← runtime) method address path requestHeaders responseBodyLimit body portHint

@[inline] def httpRequestStartWithHeadersWithResponseLimitSecure (method : HttpMethod)
    (address : String) (path : String) (requestHeaders : Array HttpHeader)
    (responseBodyLimit : UInt64) (body : ByteArray := ByteArray.empty)
    (portHint : UInt32 := 0) : RuntimeM HttpResponsePromiseRef := do
  Runtime.httpRequestStartWithHeadersWithResponseLimitSecure
    (← runtime) method address path requestHeaders responseBodyLimit body portHint

@[inline] def awaitHttpResponse (promise : HttpResponsePromiseRef) : RuntimeM HttpResponse := do
  promise.await

@[inline] def awaitHttpResponseWithHeaders (promise : HttpResponsePromiseRef) :
    RuntimeM HttpResponseEx := do
  promise.awaitWithHeaders

@[inline] def awaitHttpResponseStreamingWithHeaders (promise : HttpResponsePromiseRef) :
    RuntimeM HttpResponseStreaming := do
  promise.awaitStreamingWithHeaders

@[inline] def cancelHttpResponse (promise : HttpResponsePromiseRef) : RuntimeM Unit := do
  promise.cancel

@[inline] def releaseHttpResponsePromise (promise : HttpResponsePromiseRef) : RuntimeM Unit := do
  promise.release

@[inline] def httpRequestBodyWriteStart (requestBody : HttpRequestBody) (bytes : ByteArray) :
    RuntimeM PromiseRef := do
  Runtime.httpRequestBodyWriteStart (← runtime) requestBody bytes

@[inline] def httpRequestBodyWriteAsTask (requestBody : HttpRequestBody) (bytes : ByteArray) :
    RuntimeM (Task (Except IO.Error Unit)) := do
  ensureSameRuntime (← runtime) requestBody.runtime "HttpRequestBody"
  requestBody.writeAsTask bytes

@[inline] def httpRequestBodyWriteAsPromise (requestBody : HttpRequestBody) (bytes : ByteArray) :
    RuntimeM (Capnp.Async.Promise Unit) := do
  ensureSameRuntime (← runtime) requestBody.runtime "HttpRequestBody"
  requestBody.writeAsPromise bytes

@[inline] def httpRequestBodyWrite (requestBody : HttpRequestBody) (bytes : ByteArray) :
    RuntimeM Unit := do
  Runtime.httpRequestBodyWrite (← runtime) requestBody bytes

@[inline] def httpRequestBodyFinishStart (requestBody : HttpRequestBody) : RuntimeM PromiseRef := do
  Runtime.httpRequestBodyFinishStart (← runtime) requestBody

@[inline] def httpRequestBodyFinishAsTask (requestBody : HttpRequestBody) :
    RuntimeM (Task (Except IO.Error Unit)) := do
  ensureSameRuntime (← runtime) requestBody.runtime "HttpRequestBody"
  requestBody.finishAsTask

@[inline] def httpRequestBodyFinishAsPromise (requestBody : HttpRequestBody) :
    RuntimeM (Capnp.Async.Promise Unit) := do
  ensureSameRuntime (← runtime) requestBody.runtime "HttpRequestBody"
  requestBody.finishAsPromise

@[inline] def httpRequestBodyFinish (requestBody : HttpRequestBody) : RuntimeM Unit := do
  Runtime.httpRequestBodyFinish (← runtime) requestBody

@[inline] def httpRequestBodyRelease (requestBody : HttpRequestBody) : RuntimeM Unit := do
  Runtime.httpRequestBodyRelease (← runtime) requestBody

@[inline] def httpResponseBodyReadStart (responseBody : HttpResponseBody)
    (minBytes maxBytes : UInt32) : RuntimeM BytesPromiseRef := do
  Runtime.httpResponseBodyReadStart (← runtime) responseBody minBytes maxBytes

@[inline] def httpResponseBodyReadAsTask (responseBody : HttpResponseBody)
    (minBytes maxBytes : UInt32) : RuntimeM (Task (Except IO.Error ByteArray)) := do
  ensureSameRuntime (← runtime) responseBody.runtime "HttpResponseBody"
  responseBody.readAsTask minBytes maxBytes

@[inline] def httpResponseBodyReadAsPromise (responseBody : HttpResponseBody)
    (minBytes maxBytes : UInt32) : RuntimeM (Capnp.Async.Promise ByteArray) := do
  ensureSameRuntime (← runtime) responseBody.runtime "HttpResponseBody"
  responseBody.readAsPromise minBytes maxBytes

@[inline] def httpResponseBodyRead (responseBody : HttpResponseBody)
    (minBytes maxBytes : UInt32) : RuntimeM ByteArray := do
  Runtime.httpResponseBodyRead (← runtime) responseBody minBytes maxBytes

@[inline] def httpResponseBodyRelease (responseBody : HttpResponseBody) : RuntimeM Unit := do
  Runtime.httpResponseBodyRelease (← runtime) responseBody

@[inline] def httpServerRequestBodyReadStart (requestBody : HttpServerRequestBody)
    (minBytes maxBytes : UInt32) : RuntimeM BytesPromiseRef := do
  Runtime.httpServerRequestBodyReadStart (← runtime) requestBody minBytes maxBytes

@[inline] def httpServerRequestBodyReadAsTask (requestBody : HttpServerRequestBody)
    (minBytes maxBytes : UInt32) : RuntimeM (Task (Except IO.Error ByteArray)) := do
  ensureSameRuntime (← runtime) requestBody.runtime "HttpServerRequestBody"
  requestBody.readAsTask minBytes maxBytes

@[inline] def httpServerRequestBodyReadAsPromise (requestBody : HttpServerRequestBody)
    (minBytes maxBytes : UInt32) : RuntimeM (Capnp.Async.Promise ByteArray) := do
  ensureSameRuntime (← runtime) requestBody.runtime "HttpServerRequestBody"
  requestBody.readAsPromise minBytes maxBytes

@[inline] def httpServerRequestBodyRead (requestBody : HttpServerRequestBody)
    (minBytes maxBytes : UInt32) : RuntimeM ByteArray := do
  Runtime.httpServerRequestBodyRead (← runtime) requestBody minBytes maxBytes

@[inline] def httpServerRequestBodyRelease (requestBody : HttpServerRequestBody) :
    RuntimeM Unit := do
  Runtime.httpServerRequestBodyRelease (← runtime) requestBody

@[inline] def httpGet (address : String) (path : String) (portHint : UInt32 := 0) :
    RuntimeM HttpResponse := do
  Runtime.httpGet (← runtime) address path portHint

@[inline] def httpPost (address : String) (path : String) (body : ByteArray)
    (portHint : UInt32 := 0) : RuntimeM HttpResponse := do
  Runtime.httpPost (← runtime) address path body portHint

@[inline] def httpServerListen (address : String) (portHint : UInt32 := 0) :
    RuntimeM HttpServer := do
  Runtime.httpServerListen (← runtime) address portHint

@[inline] def httpServerListenSecure (address : String) (portHint : UInt32 := 0) :
    RuntimeM HttpServer := do
  Runtime.httpServerListenSecure (← runtime) address portHint

@[inline] def httpServerListenWithConfig (address : String) (config : HttpServerConfig)
    (portHint : UInt32 := 0) : RuntimeM HttpServer := do
  Runtime.httpServerListenWithConfig (← runtime) address config portHint

@[inline] def httpServerListenSecureWithConfig (address : String) (config : HttpServerConfig)
    (portHint : UInt32 := 0) : RuntimeM HttpServer := do
  Runtime.httpServerListenSecureWithConfig (← runtime) address config portHint

@[inline] def httpServerRelease (server : HttpServer) : RuntimeM Unit := do
  server.release

@[inline] def httpServerDrainStart (server : HttpServer) : RuntimeM PromiseRef := do
  Runtime.httpServerDrainStart (← runtime) server

@[inline] def httpServerDrainAsTask (server : HttpServer) :
    RuntimeM (Task (Except IO.Error Unit)) := do
  ensureSameRuntime (← runtime) server.runtime "HttpServer"
  server.drainAsTask

@[inline] def httpServerDrainAsPromise (server : HttpServer) :
    RuntimeM (Capnp.Async.Promise Unit) := do
  ensureSameRuntime (← runtime) server.runtime "HttpServer"
  server.drainAsPromise

@[inline] def httpServerDrain (server : HttpServer) : RuntimeM Unit := do
  Runtime.httpServerDrain (← runtime) server

@[inline] def httpServerPollRequest? (server : HttpServer) :
    RuntimeM (Option HttpServerRequest) := do
  server.pollRequest?

@[inline] def httpServerPollRequestStreaming? (server : HttpServer) :
    RuntimeM (Option HttpServerRequest) := do
  server.pollRequestStreaming?

@[inline] def httpServerRespond (server : HttpServer) (requestId : UInt32) (status : UInt32)
    (statusText : String) (responseHeaders : Array HttpHeader := #[])
    (body : ByteArray := ByteArray.empty) : RuntimeM Unit := do
  server.respond requestId status statusText responseHeaders body

@[inline] def httpServerRespondWebSocket (server : HttpServer) (requestId : UInt32)
    (responseHeaders : Array HttpHeader := #[]) : RuntimeM WebSocket := do
  server.respondWebSocket requestId responseHeaders

@[inline] def httpServerRespondStartStreaming (server : HttpServer) (requestId : UInt32)
    (status : UInt32) (statusText : String) (responseHeaders : Array HttpHeader := #[]) :
    RuntimeM HttpServerResponseBody := do
  Runtime.httpServerRespondStartStreaming
    (← runtime) server requestId status statusText responseHeaders

@[inline] def httpServerResponseBodyWriteStart (responseBody : HttpServerResponseBody)
    (bytes : ByteArray) : RuntimeM PromiseRef := do
  Runtime.httpServerResponseBodyWriteStart (← runtime) responseBody bytes

@[inline] def httpServerResponseBodyWriteAsTask (responseBody : HttpServerResponseBody)
    (bytes : ByteArray) : RuntimeM (Task (Except IO.Error Unit)) := do
  ensureSameRuntime (← runtime) responseBody.runtime "HttpServerResponseBody"
  responseBody.writeAsTask bytes

@[inline] def httpServerResponseBodyWriteAsPromise (responseBody : HttpServerResponseBody)
    (bytes : ByteArray) : RuntimeM (Capnp.Async.Promise Unit) := do
  ensureSameRuntime (← runtime) responseBody.runtime "HttpServerResponseBody"
  responseBody.writeAsPromise bytes

@[inline] def httpServerResponseBodyWrite (responseBody : HttpServerResponseBody)
    (bytes : ByteArray) : RuntimeM Unit := do
  Runtime.httpServerResponseBodyWrite (← runtime) responseBody bytes

@[inline] def httpServerResponseBodyFinishStart (responseBody : HttpServerResponseBody) :
    RuntimeM PromiseRef := do
  Runtime.httpServerResponseBodyFinishStart (← runtime) responseBody

@[inline] def httpServerResponseBodyFinishAsTask (responseBody : HttpServerResponseBody) :
    RuntimeM (Task (Except IO.Error Unit)) := do
  ensureSameRuntime (← runtime) responseBody.runtime "HttpServerResponseBody"
  responseBody.finishAsTask

@[inline] def httpServerResponseBodyFinishAsPromise (responseBody : HttpServerResponseBody) :
    RuntimeM (Capnp.Async.Promise Unit) := do
  ensureSameRuntime (← runtime) responseBody.runtime "HttpServerResponseBody"
  responseBody.finishAsPromise

@[inline] def httpServerResponseBodyFinish (responseBody : HttpServerResponseBody) :
    RuntimeM Unit := do
  Runtime.httpServerResponseBodyFinish (← runtime) responseBody

@[inline] def httpServerResponseBodyRelease (responseBody : HttpServerResponseBody) :
    RuntimeM Unit := do
  Runtime.httpServerResponseBodyRelease (← runtime) responseBody

@[inline] def webSocketConnect (address : String) (path : String) (portHint : UInt32 := 0) :
    RuntimeM WebSocket := do
  Runtime.webSocketConnect (← runtime) address path portHint

@[inline] def webSocketConnectSecure (address : String) (path : String)
    (portHint : UInt32 := 0) : RuntimeM WebSocket := do
  Runtime.webSocketConnectSecure (← runtime) address path portHint

@[inline] def webSocketConnectWithHeaders (address : String) (path : String)
    (requestHeaders : Array HttpHeader) (portHint : UInt32 := 0) :
    RuntimeM WebSocket := do
  Runtime.webSocketConnectWithHeaders (← runtime) address path requestHeaders portHint

@[inline] def webSocketConnectWithHeadersSecure (address : String) (path : String)
    (requestHeaders : Array HttpHeader) (portHint : UInt32 := 0) :
    RuntimeM WebSocket := do
  Runtime.webSocketConnectWithHeadersSecure (← runtime) address path requestHeaders portHint

@[inline] def webSocketConnectStart (address : String) (path : String)
    (portHint : UInt32 := 0) : RuntimeM WebSocketPromiseRef := do
  Runtime.webSocketConnectStart (← runtime) address path portHint

@[inline] def webSocketConnectAsTask (address : String) (path : String)
    (portHint : UInt32 := 0) : RuntimeM (Task (Except IO.Error WebSocket)) := do
  Runtime.webSocketConnectAsTask (← runtime) address path portHint

@[inline] def webSocketConnectAsPromise (address : String) (path : String)
    (portHint : UInt32 := 0) : RuntimeM (Capnp.Async.Promise WebSocket) := do
  Runtime.webSocketConnectAsPromise (← runtime) address path portHint

@[inline] def webSocketConnectStartSecure (address : String) (path : String)
    (portHint : UInt32 := 0) : RuntimeM WebSocketPromiseRef := do
  Runtime.webSocketConnectStartSecure (← runtime) address path portHint

@[inline] def webSocketConnectSecureAsTask (address : String) (path : String)
    (portHint : UInt32 := 0) : RuntimeM (Task (Except IO.Error WebSocket)) := do
  Runtime.webSocketConnectSecureAsTask (← runtime) address path portHint

@[inline] def webSocketConnectSecureAsPromise (address : String) (path : String)
    (portHint : UInt32 := 0) : RuntimeM (Capnp.Async.Promise WebSocket) := do
  Runtime.webSocketConnectSecureAsPromise (← runtime) address path portHint

@[inline] def webSocketConnectStartWithHeaders (address : String) (path : String)
    (requestHeaders : Array HttpHeader) (portHint : UInt32 := 0) :
    RuntimeM WebSocketPromiseRef := do
  Runtime.webSocketConnectStartWithHeaders (← runtime) address path requestHeaders portHint

@[inline] def webSocketConnectWithHeadersAsTask (address : String) (path : String)
    (requestHeaders : Array HttpHeader) (portHint : UInt32 := 0) :
    RuntimeM (Task (Except IO.Error WebSocket)) := do
  Runtime.webSocketConnectWithHeadersAsTask (← runtime) address path requestHeaders portHint

@[inline] def webSocketConnectWithHeadersAsPromise (address : String) (path : String)
    (requestHeaders : Array HttpHeader) (portHint : UInt32 := 0) :
    RuntimeM (Capnp.Async.Promise WebSocket) := do
  Runtime.webSocketConnectWithHeadersAsPromise
    (← runtime) address path requestHeaders portHint

@[inline] def webSocketConnectStartWithHeadersSecure (address : String) (path : String)
    (requestHeaders : Array HttpHeader) (portHint : UInt32 := 0) :
    RuntimeM WebSocketPromiseRef := do
  Runtime.webSocketConnectStartWithHeadersSecure (← runtime) address path requestHeaders portHint

@[inline] def webSocketConnectWithHeadersSecureAsTask (address : String) (path : String)
    (requestHeaders : Array HttpHeader) (portHint : UInt32 := 0) :
    RuntimeM (Task (Except IO.Error WebSocket)) := do
  Runtime.webSocketConnectWithHeadersSecureAsTask
    (← runtime) address path requestHeaders portHint

@[inline] def webSocketConnectWithHeadersSecureAsPromise (address : String) (path : String)
    (requestHeaders : Array HttpHeader) (portHint : UInt32 := 0) :
    RuntimeM (Capnp.Async.Promise WebSocket) := do
  Runtime.webSocketConnectWithHeadersSecureAsPromise
    (← runtime) address path requestHeaders portHint

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

@[inline] def webSocketSendTextAsTask (webSocket : WebSocket) (text : String) :
    RuntimeM (Task (Except IO.Error Unit)) := do
  ensureSameRuntime (← runtime) webSocket.runtime "WebSocket"
  webSocket.sendTextAsTask text

@[inline] def webSocketSendTextAsPromise (webSocket : WebSocket) (text : String) :
    RuntimeM (Capnp.Async.Promise Unit) := do
  ensureSameRuntime (← runtime) webSocket.runtime "WebSocket"
  webSocket.sendTextAsPromise text

@[inline] def webSocketSendText (webSocket : WebSocket) (text : String) : RuntimeM Unit := do
  webSocket.sendText text

@[inline] def webSocketSendBinaryStart (webSocket : WebSocket) (bytes : ByteArray) :
    RuntimeM PromiseRef := do
  Runtime.webSocketSendBinaryStart (← runtime) webSocket bytes

@[inline] def webSocketSendBinaryAsTask (webSocket : WebSocket) (bytes : ByteArray) :
    RuntimeM (Task (Except IO.Error Unit)) := do
  ensureSameRuntime (← runtime) webSocket.runtime "WebSocket"
  webSocket.sendBinaryAsTask bytes

@[inline] def webSocketSendBinaryAsPromise (webSocket : WebSocket) (bytes : ByteArray) :
    RuntimeM (Capnp.Async.Promise Unit) := do
  ensureSameRuntime (← runtime) webSocket.runtime "WebSocket"
  webSocket.sendBinaryAsPromise bytes

@[inline] def webSocketSendBinary (webSocket : WebSocket) (bytes : ByteArray) : RuntimeM Unit := do
  webSocket.sendBinary bytes

@[inline] def webSocketReceiveStart (webSocket : WebSocket) :
    RuntimeM WebSocketMessagePromiseRef := do
  Runtime.webSocketReceiveStart (← runtime) webSocket

@[inline] def webSocketReceiveAsTask (webSocket : WebSocket) :
    RuntimeM (Task (Except IO.Error WebSocketMessage)) := do
  ensureSameRuntime (← runtime) webSocket.runtime "WebSocket"
  webSocket.receiveAsTask

@[inline] def webSocketReceiveAsPromise (webSocket : WebSocket) :
    RuntimeM (Capnp.Async.Promise WebSocketMessage) := do
  ensureSameRuntime (← runtime) webSocket.runtime "WebSocket"
  webSocket.receiveAsPromise

@[inline] def webSocketReceiveStartWithMax (webSocket : WebSocket) (maxBytes : UInt32) :
    RuntimeM WebSocketMessagePromiseRef := do
  Runtime.webSocketReceiveStartWithMax (← runtime) webSocket maxBytes

@[inline] def webSocketReceiveWithMaxAsTask (webSocket : WebSocket) (maxBytes : UInt32) :
    RuntimeM (Task (Except IO.Error WebSocketMessage)) := do
  ensureSameRuntime (← runtime) webSocket.runtime "WebSocket"
  webSocket.receiveWithMaxAsTask maxBytes

@[inline] def webSocketReceiveWithMaxAsPromise (webSocket : WebSocket) (maxBytes : UInt32) :
    RuntimeM (Capnp.Async.Promise WebSocketMessage) := do
  ensureSameRuntime (← runtime) webSocket.runtime "WebSocket"
  webSocket.receiveWithMaxAsPromise maxBytes

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

@[inline] def webSocketReceiveWithMax (webSocket : WebSocket) (maxBytes : UInt32) :
    RuntimeM WebSocketMessage := do
  webSocket.receiveWithMax maxBytes

@[inline] def webSocketCloseStart (webSocket : WebSocket) (code : UInt16)
    (reason : String := "") : RuntimeM PromiseRef := do
  Runtime.webSocketCloseStart (← runtime) webSocket code reason

@[inline] def webSocketCloseAsTask (webSocket : WebSocket) (code : UInt16)
    (reason : String := "") : RuntimeM (Task (Except IO.Error Unit)) := do
  ensureSameRuntime (← runtime) webSocket.runtime "WebSocket"
  webSocket.closeAsTask code reason

@[inline] def webSocketCloseAsPromise (webSocket : WebSocket) (code : UInt16)
    (reason : String := "") : RuntimeM (Capnp.Async.Promise Unit) := do
  ensureSameRuntime (← runtime) webSocket.runtime "WebSocket"
  webSocket.closeAsPromise code reason

@[inline] def webSocketClose (webSocket : WebSocket) (code : UInt16)
    (reason : String := "") : RuntimeM Unit := do
  webSocket.close code reason

@[inline] def webSocketDisconnect (webSocket : WebSocket) : RuntimeM Unit := do
  webSocket.disconnect

@[inline] def webSocketAbort (webSocket : WebSocket) : RuntimeM Unit := do
  webSocket.abort

@[inline] def newWebSocketPipe : RuntimeM (WebSocket × WebSocket) := do
  Runtime.newWebSocketPipe (← runtime)

@[inline] def listenEndpoint (endpoint : Endpoint) : RuntimeM Listener := do
  Runtime.listenEndpoint (← runtime) endpoint

@[inline] def connectEndpoint (endpoint : Endpoint) : RuntimeM Connection := do
  Runtime.connectEndpoint (← runtime) endpoint

@[inline] def connectStartEndpoint (endpoint : Endpoint) : RuntimeM ConnectionPromiseRef := do
  Runtime.connectStartEndpoint (← runtime) endpoint

@[inline] def connectAsTaskEndpoint (endpoint : Endpoint) :
    RuntimeM (Task (Except IO.Error Connection)) := do
  Runtime.connectAsTaskEndpoint (← runtime) endpoint

@[inline] def connectAsPromiseEndpoint (endpoint : Endpoint) :
    RuntimeM (Capnp.Async.Promise Connection) := do
  Runtime.connectAsPromiseEndpoint (← runtime) endpoint

@[inline] def httpRequestEndpoint (method : HttpMethod) (endpoint : HttpEndpoint) (path : String)
    (body : ByteArray := ByteArray.empty) : RuntimeM HttpResponse := do
  Runtime.httpRequestEndpoint (← runtime) method endpoint path body

@[inline] def httpRequestWithHeadersEndpoint (method : HttpMethod) (endpoint : HttpEndpoint)
    (path : String) (requestHeaders : Array HttpHeader)
    (body : ByteArray := ByteArray.empty) : RuntimeM HttpResponseEx := do
  Runtime.httpRequestWithHeadersEndpoint (← runtime) method endpoint path requestHeaders body

@[inline] def webSocketConnectEndpoint (endpoint : HttpEndpoint) (path : String) :
    RuntimeM WebSocket := do
  Runtime.webSocketConnectEndpoint (← runtime) endpoint path

@[inline] def webSocketConnectWithHeadersEndpoint (endpoint : HttpEndpoint) (path : String)
    (requestHeaders : Array HttpHeader) : RuntimeM WebSocket := do
  Runtime.webSocketConnectWithHeadersEndpoint (← runtime) endpoint path requestHeaders

@[inline] def httpServerListenEndpoint (endpoint : Endpoint) : RuntimeM HttpServer := do
  Runtime.httpServerListenEndpoint (← runtime) endpoint

@[inline] def httpServerListenSecureEndpoint (endpoint : Endpoint) : RuntimeM HttpServer := do
  Runtime.httpServerListenSecureEndpoint (← runtime) endpoint

@[inline] def httpServerListenWithConfigEndpoint (endpoint : Endpoint)
    (config : HttpServerConfig) : RuntimeM HttpServer := do
  Runtime.httpServerListenWithConfigEndpoint (← runtime) endpoint config

@[inline] def httpServerListenSecureWithConfigEndpoint (endpoint : Endpoint)
    (config : HttpServerConfig) : RuntimeM HttpServer := do
  Runtime.httpServerListenSecureWithConfigEndpoint (← runtime) endpoint config

end RuntimeM

end KjAsync
end Capnp
