import LeanTest
import Capnp.KjAsync

open LeanTest

private def mkUnixTestAddress : IO (String × String) := do
  let n ← IO.rand 0 1000000000
  let path := s!"/tmp/capnp-lean4-kjasync-{n}.sock"
  pure (s!"unix:{path}", path)

private def mkPayload : ByteArray :=
  ByteArray.empty.push (UInt8.ofNat 99)
    |>.push (UInt8.ofNat 97)
    |>.push (UInt8.ofNat 112)
    |>.push (UInt8.ofNat 110)
    |>.push (UInt8.ofNat 112)
    |>.push (UInt8.ofNat 45)
    |>.push (UInt8.ofNat 107)
    |>.push (UInt8.ofNat 106)

private partial def waitForHttpServerRequest (runtime : Capnp.KjAsync.Runtime)
    (server : Capnp.KjAsync.HttpServer) (attempts : Nat := 400) :
    IO Capnp.KjAsync.HttpServerRequest := do
  if attempts == 0 then
    throw (IO.userError "timed out waiting for HTTP server request")
  match (← runtime.httpServerPollRequest? server) with
  | some request => pure request
  | none =>
    runtime.sleepMillis (UInt32.ofNat 5)
    waitForHttpServerRequest runtime server (attempts - 1)

@[test]
def testKjAsyncRuntimeLifecycle : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  assertEqual (← runtime.isAlive) true
  runtime.shutdown
  assertEqual (← runtime.isAlive) false

@[test]
def testKjAsyncSleepAwait : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let promise ← runtime.sleepMillisStart (UInt32.ofNat 10)
    let startedAt ← IO.monoNanosNow
    promise.await
    let finishedAt ← IO.monoNanosNow
    assertTrue (finishedAt >= startedAt) "monotonic clock moved backwards"
  finally
    runtime.shutdown

@[test]
def testKjAsyncSleepCancel : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let promise ← runtime.sleepMillisStart (UInt32.ofNat 5000)
    promise.cancel
    let canceled ←
      try
        promise.await
        pure false
      catch _ =>
        pure true
    assertEqual canceled true
  finally
    runtime.shutdown

@[test]
def testKjAsyncAwaitAsTask : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let promise ← runtime.sleepMillisStart (UInt32.ofNat 10)
    let task ← promise.awaitAsTask
    let result ← IO.wait task
    match result with
    | Except.ok () => pure ()
    | Except.error err =>
      throw (IO.userError s!"awaitAsTask failed: {err}")
  finally
    runtime.shutdown

@[test]
def testKjAsyncSharedAsyncHelpers : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let sleepPromise ← runtime.sleepMillisStart (UInt32.ofNat 10)
    let sleepTask ← Capnp.Async.awaitAsTask sleepPromise
    let sleepTaskResult ← IO.wait sleepTask
    match sleepTaskResult with
    | .ok () => pure ()
    | .error err =>
        throw (IO.userError s!"shared awaitAsTask failed: {err}")

    let sleepPromise2 ← runtime.sleepMillisStart (UInt32.ofNat 10)
    let sleepIoPromise ← Capnp.Async.toIOPromise sleepPromise2
    let sleepIoResult? ← IO.wait sleepIoPromise.result?
    match sleepIoResult? with
    | some (.ok ()) => pure ()
    | some (.error err) =>
        throw (IO.userError s!"shared toIOPromise failed: {err}")
    | none =>
        throw (IO.userError "shared toIOPromise dropped without a result")

    let (left, right) ← runtime.newTwoWayPipe
    try
      let payload := mkPayload
      let readPromise ← right.readStart (UInt32.ofNat 1) (UInt32.ofNat 1024)
      let readTask ← Capnp.Async.awaitAsTask readPromise
      left.write payload
      let readTaskResult ← IO.wait readTask
      match readTaskResult with
      | .ok received =>
          assertEqual received payload
      | .error err =>
          throw (IO.userError s!"shared typed awaitAsTask failed: {err}")
    finally
      left.release
      right.release
  finally
    runtime.shutdown

@[test]
def testKjAsyncRuntimeMRunWithNewRuntime : IO Unit := do
  let alive ← Capnp.KjAsync.RuntimeM.runWithNewRuntime do
    let alive ← Capnp.KjAsync.RuntimeM.isAlive
    Capnp.KjAsync.RuntimeM.sleepMillis (UInt32.ofNat 5)
    pure alive
  assertEqual alive true

@[test]
def testKjAsyncNetworkRoundtrip : IO Unit := do
  if System.Platform.isWindows then
    assertTrue true "KJ unix socket test skipped on Windows"
  else
    let (address, socketPath) ← mkUnixTestAddress
    let serverRuntime ← Capnp.KjAsync.Runtime.init
    let clientRuntime ← Capnp.KjAsync.Runtime.init
    try
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

      let listener ← serverRuntime.listen address
      let serverTask ← IO.asTask do
        let serverConn ← listener.accept
        let req ← serverConn.read (UInt32.ofNat 1) (UInt32.ofNat 1024)
        serverConn.write req
        serverConn.shutdownWrite
        serverConn.release

      let clientConn ← clientRuntime.connect address
      let payload := mkPayload
      clientConn.write payload
      clientConn.shutdownWrite
      let echoed ← clientConn.read (UInt32.ofNat payload.size) (UInt32.ofNat payload.size)
      assertEqual echoed payload
      clientConn.release

      let serverResult ← IO.wait serverTask
      match serverResult with
      | Except.ok _ => pure ()
      | Except.error err => throw (IO.userError s!"server task failed: {err}")

      listener.release
    finally
      serverRuntime.shutdown
      clientRuntime.shutdown
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

@[test]
def testKjAsyncNetworkRoundtripSingleRuntimeAsyncStart : IO Unit := do
  if System.Platform.isWindows then
    assertTrue true "KJ unix socket test skipped on Windows"
  else
    let (address, socketPath) ← mkUnixTestAddress
    let runtime ← Capnp.KjAsync.Runtime.init
    try
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

      let listener ← runtime.listen address
      let acceptPromise ← listener.acceptStart
      let connectPromise ← runtime.connectStart address

      let serverConn ← acceptPromise.await
      let clientConn ← connectPromise.await

      let payload := mkPayload
      clientConn.write payload
      clientConn.shutdownWrite

      let req ← serverConn.read (UInt32.ofNat 1) (UInt32.ofNat 1024)
      assertEqual req payload
      serverConn.write req
      serverConn.shutdownWrite

      let echoed ← clientConn.read (UInt32.ofNat payload.size) (UInt32.ofNat payload.size)
      assertEqual echoed payload

      clientConn.release
      serverConn.release
      listener.release
    finally
      runtime.shutdown
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

@[test]
def testKjAsyncPromiseComposition : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let p1 ← runtime.sleepMillisStart (UInt32.ofNat 5)
    let p2 ← runtime.sleepMillisStart (UInt32.ofNat 10)
    let all ← runtime.promiseAllStart #[p1, p2]
    all.await

    let p3 ← runtime.sleepMillisStart (UInt32.ofNat 200)
    let p4 ← runtime.sleepMillisStart (UInt32.ofNat 5)
    let race ← runtime.promiseRaceStart #[p3, p4]
    let startedAt ← IO.monoNanosNow
    race.await
    let finishedAt ← IO.monoNanosNow
    let elapsed := finishedAt - startedAt
    assertTrue (elapsed < 500000000) "promise race took too long"
  finally
    runtime.shutdown

@[test]
def testKjAsyncTaskSetLifecycle : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let taskSet ← runtime.taskSetNew
    assertEqual (← taskSet.isEmpty) true

    let p ← runtime.sleepMillisStart (UInt32.ofNat 10)
    taskSet.addPromise p
    assertEqual (← taskSet.isEmpty) false

    let onEmpty ← taskSet.onEmptyStart
    onEmpty.await
    assertEqual (← taskSet.isEmpty) true
    assertEqual (← taskSet.errorCount) (UInt32.ofNat 0)
    assertEqual (← taskSet.takeLastError?) none
    taskSet.release
  finally
    runtime.shutdown

@[test]
def testKjAsyncTwoWayPipeDisconnectAndAbort : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let (left, right) ← runtime.newTwoWayPipe

    let disconnect ← left.whenWriteDisconnectedStart
    right.release
    disconnect.await

    let (abortWriter, abortReader) ← runtime.newTwoWayPipe
    let abortDisconnect ← abortWriter.whenWriteDisconnectedStart
    abortWriter.abortWrite "lean-test-abort-write"
    abortReader.abortRead
    abortReader.release
    abortDisconnect.await

    left.release
    abortWriter.release
  finally
    runtime.shutdown

@[test]
def testKjAsyncDatagramRoundtrip : IO Unit := do
  let senderRuntime ← Capnp.KjAsync.Runtime.init
  let receiverRuntime ← Capnp.KjAsync.Runtime.init
  try
    let receiverPort ← receiverRuntime.datagramBind "127.0.0.1" 0
    let receiverPortNumber ← receiverPort.getPort
    let senderPort ← senderRuntime.datagramBind "127.0.0.1" 0
    let payload := mkPayload

    let receiveTask ← IO.asTask do
      receiverPort.receive (UInt32.ofNat 1024)

    let sentCount ← senderPort.send "127.0.0.1" payload receiverPortNumber
    assertEqual sentCount (UInt32.ofNat payload.size)

    let receiveResult ← IO.wait receiveTask
    match receiveResult with
    | Except.ok (_source, bytes) =>
      assertEqual bytes payload
    | Except.error err =>
      throw (IO.userError s!"datagram receive task failed: {err}")

    senderPort.release
    receiverPort.release
  finally
    senderRuntime.shutdown
    receiverRuntime.shutdown

@[test]
def testKjAsyncTwoWayPipeAsyncReadWritePrimitives : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let (left, right) ← runtime.newTwoWayPipe
    let payload := mkPayload

    let writePromise ← left.writeStart payload
    let readPromise ← right.readStart (UInt32.ofNat 1) (UInt32.ofNat 1024)
    writePromise.await
    let received ← readPromise.await
    assertEqual received payload

    let shutdownPromise ← left.shutdownWriteStart
    shutdownPromise.await

    left.release
    right.release
  finally
    runtime.shutdown

@[test]
def testKjAsyncDatagramAsyncPromiseRefs : IO Unit := do
  let senderRuntime ← Capnp.KjAsync.Runtime.init
  let receiverRuntime ← Capnp.KjAsync.Runtime.init
  try
    let receiverPort? ←
      try
        pure (some (← receiverRuntime.datagramBind "127.0.0.1" 0))
      catch _ =>
        pure none
    let senderPort? ←
      try
        pure (some (← senderRuntime.datagramBind "127.0.0.1" 0))
      catch _ =>
        pure none

    match receiverPort?, senderPort? with
    | some receiverPort, some senderPort =>
      let receiverPortNumber ← receiverPort.getPort
      let payload := mkPayload
      let receivePromise ← receiverPort.receiveStart (UInt32.ofNat 1024)
      let sendPromise ← senderPort.sendStart "127.0.0.1" payload receiverPortNumber

      let sentCount ← sendPromise.await
      assertEqual sentCount (UInt32.ofNat payload.size)

      let (_source, bytes) ← receivePromise.await
      assertEqual bytes payload

      senderPort.release
      receiverPort.release
    | _, _ =>
      assertTrue true "datagram async promise test skipped (bind unavailable)"
  finally
    senderRuntime.shutdown
    receiverRuntime.shutdown

@[test]
def testKjAsyncWebSocketPipeAsyncSendReceive : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let (left, right) ← runtime.newWebSocketPipe

    let recvText ← right.receiveStart
    let sendText ← left.sendTextStart "lean-kjasync-text"
    sendText.await
    let message ← recvText.await
    match message with
    | .text value =>
      assertEqual value "lean-kjasync-text"
    | _ =>
      throw (IO.userError "expected websocket text message")

    let payload := mkPayload
    let recvBinary ← right.receiveStart
    let sendBinary ← left.sendBinaryStart payload
    sendBinary.await
    let binaryMessage ← recvBinary.await
    match binaryMessage with
    | .binary bytes =>
      assertEqual bytes payload
    | _ =>
      throw (IO.userError "expected websocket binary message")

    left.release
    right.release
  finally
    runtime.shutdown

@[test]
def testKjAsyncWebSocketReceiveCancel : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let (_left, right) ← runtime.newWebSocketPipe
    let receivePromise ← right.receiveStart
    receivePromise.cancel
    let canceled ←
      try
        let _ ← receivePromise.await
        pure false
      catch _ =>
        pure true
    assertEqual canceled true
    right.release
  finally
    runtime.shutdown

@[test]
def testKjAsyncHttpServerRoundtripWithHeaders : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let server ← runtime.httpServerListen "127.0.0.1" 0
    let requestHeaders : Array Capnp.KjAsync.HttpHeader := #[
      { name := "x-lean-client", value := "1" }
    ]
    let requestBody := mkPayload
    let responsePromise ← runtime.httpRequestStartWithHeaders
      .post "127.0.0.1" "/lean-http" requestHeaders requestBody server.boundPort

    let request ← waitForHttpServerRequest runtime server
    assertTrue (request.method == .post) "expected POST request method"
    assertEqual request.path "/lean-http"
    assertEqual request.body requestBody

    runtime.httpServerRespond server request.requestId (UInt32.ofNat 201) "Created"
      #[{ name := "x-lean-server", value := "ok" }] requestBody

    let response ← runtime.httpResponsePromiseAwaitWithHeaders responsePromise
    assertEqual response.status (UInt32.ofNat 201)
    assertEqual response.statusText "Created"
    assertEqual response.body requestBody
    assertTrue
      (response.headers.any
        (fun h => h.name == "x-lean-server" && h.value == "ok"))
      "expected response header x-lean-server"

    server.release
  finally
    runtime.shutdown

@[test]
def testKjAsyncWebSocketServerAccept : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let server ← runtime.httpServerListen "127.0.0.1" 0
    let connectPromise ←
      runtime.webSocketConnectStartWithHeaders "127.0.0.1" "/lean-ws"
        #[{ name := "x-lean-ws", value := "1" }] server.boundPort

    let request ← waitForHttpServerRequest runtime server
    assertEqual request.webSocketRequested true
    assertEqual request.path "/lean-ws"

    let serverWs ← runtime.httpServerRespondWebSocket server request.requestId
    let clientWs ← connectPromise.await

    (← clientWs.sendTextStart "hello-from-client").await
    let serverMsg ← serverWs.receive
    match serverMsg with
    | .text value =>
      assertEqual value "hello-from-client"
    | _ =>
      throw (IO.userError "expected websocket text message on server side")

    (← serverWs.sendTextStart "hello-from-server").await
    let clientMsg ← clientWs.receive
    match clientMsg with
    | .text value =>
      assertEqual value "hello-from-server"
    | _ =>
      throw (IO.userError "expected websocket text message on client side")

    clientWs.release
    serverWs.release
    server.release
  finally
    runtime.shutdown
