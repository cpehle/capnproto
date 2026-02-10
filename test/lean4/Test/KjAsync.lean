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

private def appendByteArray (dst src : ByteArray) : ByteArray :=
  Id.run do
    let mut out := dst
    for b in src do
      out := out.push b
    pure out

private def tlsSelfSignedCertPem : String :=
  String.intercalate "\n" [
    "-----BEGIN CERTIFICATE-----",
    "MIIDHzCCAgegAwIBAgIUV7L9GipFL+gICKG4kfi2jLAoGiIwDQYJKoZIhvcNAQEL",
    "BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDIxMDAyNTMzNVoXDTM2MDIw",
    "ODAyNTMzNVowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF",
    "AAOCAQ8AMIIBCgKCAQEA1SEmm5FXCI56n7aKcrTSar1AqLzPhJNau/GqfYKzM8CC",
    "97lHb03F3izgWl3ahTQyb1ZQl0TSjWs1BIZ4iB9txwDoxHE0fiZX/Q05JcHOowjO",
    "joQ/ac6s6c1UqY1a0ZlwP4TYpd4DayF8aynkgjneS4iGXGpjX8/RZAs0kcLuInK7",
    "jNQfnv+dCKpQRLCCogzdCuPCGCR61UK7cV537N6Q5m1+7Vr0T97xbzldTekXpUdF",
    "IVadn7smCopTGCRyWMMFgxxnFXTTggETHAzuMWZZGt6MWHp2tVYucqRc5yqHKSeu",
    "rQCgkOlCLQfgLoRyAUaxUQEmo08EOljCBURIZBBDQQIDAQABo2kwZzAdBgNVHQ4E",
    "FgQUVJlXR0v/AVF1towFmGBo4yTUwjUwHwYDVR0jBBgwFoAUVJlXR0v/AVF1towF",
    "mGBo4yTUwjUwDwYDVR0TAQH/BAUwAwEB/zAUBgNVHREEDTALgglsb2NhbGhvc3Qw",
    "DQYJKoZIhvcNAQELBQADggEBACBJMUhkjlN6odfxdqJoUbuecYYaeL8szXH2+/51",
    "g26WmFO9Jxv98/w5spTiYT6yJwHZTwVZmjTWuD7iPmGWyrxHMUwmO86rQ9sJu0z6",
    "yc8PBDajsBCtw/sa1nhA/XJVUVdVwCTJ2A21Odhe1ONAuAKX1FNHHpgMXrKKwwDT",
    "nY6G+EYP2EWSJCl+uMO8D+yDwNlGxTrCXqJphQifR5XyPnojYw2vc//FPODLNCpr",
    "v0WxhCVjgh/XxJKjmKNEIkLpU2tUuAapvU5IuRif62o7OVG7PqpuR8zehFSbK706",
    "IQdNs0jRB9vRLFFR7A17RL79972kmFNe4j6thhjtakX6Eho=",
    "-----END CERTIFICATE-----",
    ""
  ]

private def tlsSelfSignedKeyPem : String :=
  String.intercalate "\n" [
    "-----BEGIN PRIVATE KEY-----",
    "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDVISabkVcIjnqf",
    "topytNJqvUCovM+Ek1q78ap9grMzwIL3uUdvTcXeLOBaXdqFNDJvVlCXRNKNazUE",
    "hniIH23HAOjEcTR+Jlf9DTklwc6jCM6OhD9pzqzpzVSpjVrRmXA/hNil3gNrIXxr",
    "KeSCOd5LiIZcamNfz9FkCzSRwu4icruM1B+e/50IqlBEsIKiDN0K48IYJHrVQrtx",
    "Xnfs3pDmbX7tWvRP3vFvOV1N6RelR0UhVp2fuyYKilMYJHJYwwWDHGcVdNOCARMc",
    "DO4xZlka3oxYena1Vi5ypFznKocpJ66tAKCQ6UItB+AuhHIBRrFRASajTwQ6WMIF",
    "REhkEENBAgMBAAECggEAAZPTjGOXr2D3FkEojotpMogsrycJGeoJihIyhOfrjcCx",
    "M3ZOCZxLsG6YeUDANBvQGv/6fBkivhBW4c4BPMAvTMGUZ3ZPSTb9UBqZw35XCkC/",
    "nyFKUx0YDWmzNIdO3pXmNVklTZp6X9/NQwGKDv7wxtI3jN3udvxDuGvnD2RgBPYd",
    "GE7puZVmjw8eQJ7Ri0Lpo2nNaJhgbvxbWgkCRqk5HAlf7k60474mDYHQz8qCSsup",
    "ila4boobm+eAN3mJll0ESebJzdLT0QUKBOd5LVR6pIRiCrPhMgr4VOKFFZv97Er0",
    "2Oha9vFPcH4qjLLLScMuq4K+afz9M9iFs2TRLSZtQQKBgQD0T+GjqQV2Vzahq+4t",
    "na4mkVTEqiK4bty5KFIB7uHFVMrDiyGfBAXu31I7Cglg0G/HdrX8Ul+vkGhmz+gf",
    "rbwIIlLvy20EFDKDk5pfZFbqC5BfTrn08quv4adbKUbNw6yWeq2hBIn9EN+IMNfA",
    "W6cloXKF2rmpoWwZJWoAqkZQ4QKBgQDfU1+EPfN6mK9zwbj599NNo9gdHeZM4SYi",
    "zMzVKhboqCxMY/cxPKd5jq+VQxPzwn4v2nNc++2kUge8EgFgziKEscqk2VlPMj7S",
    "lt23tTIEWpj9IYr+BkPHLYy1JbhuI2idXVRJPtfca0zFLZ5zid12RnphuY9qMAtH",
    "UJZkX9leYQKBgBujZc1T87A9kYqcnqc+bVMjoclVzfO7ZvDzZMOfOJ9QRlf0x2rr",
    "05gAX5caPZFcQyj3fwL6dqSv23+2CXZ7+weYinViOAT8G/LSoeYkvchgYobFqzfQ",
    "tCeDiaFAfCgO+NlVK4tJriqY3BDWJbI3LCOPrhsCcXqFLmtx1hoZKTdBAoGBALxL",
    "lphwh47Rl/VY3DbezqmCwN/j6t7dYwMqfHYqk8A8s4UBMFWyV161gLOwJ+16Cl0c",
    "qfI3c+n9RAo9gC33/8C0CzEtFREiQzfZ/j07qF1laeLb2k5OR+1zKVU+5Z7vefBc",
    "1YkgVG7DhaomyZePIUvtJpipFROqSIgrmXIuIp9hAoGBAL332byiu/CqGAdXDQYO",
    "qGuCopLuVt9kIwerDnGF6DJaR69HBYN03jALNMsQ6whXLHQILvIALJex96vOwBeU",
    "VZ7kkjIl0eKSgOUhh/z4r8UtfFXfENV/uhR6zafty6qLN/9pM799jo8YLMXbAz7n",
    "ASrGhFMZlqDXB3vTwmCiQEqp",
    "-----END PRIVATE KEY-----",
    ""
  ]

private partial def readHttpServerRequestBodyAll
    (requestBody : Capnp.KjAsync.HttpServerRequestBody) : IO ByteArray := do
  let mut out := ByteArray.empty
  let mut done := false
  while !done do
    let chunk ← requestBody.read (UInt32.ofNat 1) (UInt32.ofNat 4096)
    if chunk.size == 0 then
      done := true
    else
      out := appendByteArray out chunk
  requestBody.release
  pure out

private partial def waitForHttpServerRequestRaw (runtime : Capnp.KjAsync.Runtime)
    (server : Capnp.KjAsync.HttpServer) (attempts : Nat := 400) :
    IO Capnp.KjAsync.HttpServerRequest := do
  if attempts == 0 then
    throw (IO.userError "timed out waiting for HTTP server request")
  match (← runtime.httpServerPollRequestStreaming? server) with
  | some request => pure request
  | none =>
    runtime.sleepMillis (UInt32.ofNat 5)
    waitForHttpServerRequestRaw runtime server (attempts - 1)

private partial def waitForHttpServerRequest (runtime : Capnp.KjAsync.Runtime)
    (server : Capnp.KjAsync.HttpServer) (attempts : Nat := 400) :
    IO Capnp.KjAsync.HttpServerRequest := do
  let request ← waitForHttpServerRequestRaw runtime server attempts
  match request.bodyStream? with
  | some requestBody =>
    let body ← readHttpServerRequestBodyAll requestBody
    pure { request with body := body, bodyStream? := none }
  | none =>
    pure request

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
def testKjAsyncWebSocketReceiveWithMaxRejectsOversize : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let server ← runtime.httpServerListen "127.0.0.1" 0
    let connectPromise ← runtime.webSocketConnectStart "127.0.0.1" "/lean-ws-max" server.boundPort
    let request ← waitForHttpServerRequest runtime server
    assertEqual request.webSocketRequested true
    let serverWs ← runtime.httpServerRespondWebSocket server request.requestId
    let clientWs ← connectPromise.await

    let receivePromise ← serverWs.receiveStartWithMax (UInt32.ofNat 4)
    let sendFailed ←
      try
        (← clientWs.sendTextStart "oversized-websocket-message").await
        pure false
      catch _ =>
        pure true
    let receiveFailed ←
      try
        let _ ← receivePromise.await
        pure false
      catch _ =>
        pure true
    assertEqual (sendFailed || receiveFailed) true

    clientWs.release
    serverWs.release
    server.release
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
def testKjAsyncHttpResponseBodyLimit : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let server ← runtime.httpServerListen "127.0.0.1" 0
    let responsePromise ← runtime.httpRequestStartWithResponseLimit
      .post "127.0.0.1" "/lean-http-limit" (UInt64.ofNat 8) mkPayload server.boundPort
    let request ← waitForHttpServerRequest runtime server
    assertEqual request.path "/lean-http-limit"

    let body := ByteArray.empty
      |>.push (UInt8.ofNat 49)
      |>.push (UInt8.ofNat 50)
      |>.push (UInt8.ofNat 51)
      |>.push (UInt8.ofNat 52)
      |>.push (UInt8.ofNat 53)
      |>.push (UInt8.ofNat 54)
      |>.push (UInt8.ofNat 55)
      |>.push (UInt8.ofNat 56)
      |>.push (UInt8.ofNat 57)
      |>.push (UInt8.ofNat 48)
    runtime.httpServerRespond server request.requestId (UInt32.ofNat 200) "OK" #[] body

    let failed ←
      try
        let _ ← responsePromise.await
        pure false
      catch _ =>
        pure true
    assertEqual failed true
    server.release
  finally
    runtime.shutdown

@[test]
def testKjAsyncHttpStreamingRequestAndResponse : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let server ← runtime.httpServerListen "127.0.0.1" 0
    let requestPartA := "stream-request-part-a".toUTF8
    let requestPartB := "-and-b".toUTF8
    let expectedRequestBody := appendByteArray requestPartA requestPartB

    let (requestBody?, responsePromise) ← runtime.httpRequestStartStreamingWithHeaders
      .post "127.0.0.1" "/lean-http-stream" #[{ name := "x-stream", value := "1" }]
      server.boundPort
    let requestBody ←
      match requestBody? with
      | some body => pure body
      | none => throw (IO.userError "streaming HTTP request did not provide a request body handle")
    requestBody.write requestPartA
    requestBody.write requestPartB
    requestBody.finish

    let request ← waitForHttpServerRequest runtime server
    assertEqual request.path "/lean-http-stream"
    assertEqual request.body expectedRequestBody

    let responseBody := "streaming-response-body-for-lean".toUTF8
    runtime.httpServerRespond server request.requestId (UInt32.ofNat 200) "OK"
      #[{ name := "x-stream-response", value := "1" }] responseBody

    let response ← responsePromise.awaitStreamingWithHeaders
    assertEqual response.status (UInt32.ofNat 200)
    assertEqual response.statusText "OK"
    assertTrue
      (response.headers.any
        (fun h => h.name == "x-stream-response" && h.value == "1"))
      "expected response header x-stream-response"

    let mut received := ByteArray.empty
    let mut done := false
    while !done do
      let chunk ← response.body.read (UInt32.ofNat 1) (UInt32.ofNat 5)
      if chunk.size == 0 then
        done := true
      else
        received := appendByteArray received chunk
    assertEqual received responseBody
    response.body.release
    server.release
  finally
    runtime.shutdown

@[test]
def testKjAsyncHttpServerStreamingRequestAndResponse : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let server ← runtime.httpServerListen "127.0.0.1" 0
    let requestPartA := "server-stream-request-a".toUTF8
    let requestPartB := "-and-b".toUTF8
    let expectedRequestBody := appendByteArray requestPartA requestPartB

    let (requestBody?, responsePromise) ← runtime.httpRequestStartStreamingWithHeaders
      .post "127.0.0.1" "/lean-http-server-stream" #[{ name := "x-server-stream", value := "1" }]
      server.boundPort
    let requestBody ←
      match requestBody? with
      | some body => pure body
      | none => throw (IO.userError "expected HTTP request body stream")
    requestBody.write requestPartA
    requestBody.write requestPartB
    requestBody.finish

    let request ← waitForHttpServerRequestRaw runtime server
    assertEqual request.path "/lean-http-server-stream"
    assertEqual request.body.size 0
    let requestBodyStream ←
      match request.bodyStream? with
      | some body => pure body
      | none => throw (IO.userError "expected server request body stream handle")
    let streamedRequestBody ← readHttpServerRequestBodyAll requestBodyStream
    assertEqual streamedRequestBody expectedRequestBody

    let responseBody ← runtime.httpServerRespondStartStreaming server request.requestId
      (UInt32.ofNat 202) "Accepted" #[{ name := "x-server-stream-response", value := "1" }]
    let responsePartA := "server-stream-response-a".toUTF8
    let responsePartB := "-and-b".toUTF8
    let expectedResponseBody := appendByteArray responsePartA responsePartB
    responseBody.write responsePartA
    responseBody.write responsePartB
    responseBody.finish

    let response ← responsePromise.awaitStreamingWithHeaders
    assertEqual response.status (UInt32.ofNat 202)
    assertEqual response.statusText "Accepted"
    assertTrue
      (response.headers.any
        (fun h => h.name == "x-server-stream-response" && h.value == "1"))
      "expected response header x-server-stream-response"

    let mut received := ByteArray.empty
    let mut done := false
    while !done do
      let chunk ← response.body.read (UInt32.ofNat 1) (UInt32.ofNat 4)
      if chunk.size == 0 then
        done := true
      else
        received := appendByteArray received chunk
    assertEqual received expectedResponseBody
    response.body.release
    server.release
  finally
    runtime.shutdown

@[test]
def testKjAsyncTlsEnableIsExplicit : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let errBeforeEnable ←
      try
        let p ← runtime.httpRequestStartWithHeadersSecure
          .get "localhost" "/" #[] ByteArray.empty (UInt32.ofNat 1)
        p.release
        pure ""
      catch e =>
        pure (toString e)
    assertTrue (errBeforeEnable.contains "TLS is not enabled")
      "expected secure request to fail before Runtime.enableTls"

    runtime.enableTls
    runtime.enableTls

    let errAfterEnable ←
      try
        let p ← runtime.httpRequestStartWithHeadersSecure
          .get "localhost" "/" #[] ByteArray.empty (UInt32.ofNat 1)
        p.release
        pure ""
      catch e =>
        pure (toString e)
    assertTrue (!errAfterEnable.contains "TLS is not enabled")
      "expected secure request error to no longer be the TLS-not-enabled guard after Runtime.enableTls"
  finally
    runtime.shutdown

@[test]
def testKjAsyncHttpsAndWssWithCustomTlsConfig : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    runtime.configureTls {
      useSystemTrustStore := false
      verifyClients := false
      minVersion := some .tls12
      trustedCertificatesPem := tlsSelfSignedCertPem
      certificateChainPem := tlsSelfSignedCertPem
      privateKeyPem := tlsSelfSignedKeyPem
      acceptTimeoutNanos := 2000000000
    }
    let server ← runtime.httpServerListenSecure "localhost" 0
    assertTrue (server.boundPort != UInt32.ofNat 0) "secure server must bind a non-zero port"
    let requestBody := "https-request-body".toUTF8
    let responsePromise ← runtime.httpRequestStartWithHeadersSecure
      .post "localhost" "/lean-https" #[] requestBody server.boundPort

    let request ←
      try
        waitForHttpServerRequest runtime server
      catch _ =>
        responsePromise.cancel
        let err ←
          try
            let _ ← responsePromise.awaitWithHeaders
            pure "request unexpectedly completed without reaching server"
          catch e =>
            pure (toString e)
        throw (IO.userError s!"timed out waiting for HTTPS request; client error: {err}")
    assertEqual request.path "/lean-https"
    assertEqual request.body requestBody

    let responseBody := "https-response-body".toUTF8
    runtime.httpServerRespond server request.requestId (UInt32.ofNat 200) "OK"
      #[{ name := "x-https", value := "1" }] responseBody

    let response ← responsePromise.awaitWithHeaders
    assertEqual response.status (UInt32.ofNat 200)
    assertEqual response.body responseBody
    assertTrue
      (response.headers.any (fun h => h.name == "x-https" && h.value == "1"))
      "expected response header x-https"

    let wsPromise ← runtime.webSocketConnectStartWithHeadersSecure
      "localhost" "/lean-wss" #[] server.boundPort
    let wsRequest ← waitForHttpServerRequestRaw runtime server
    assertEqual wsRequest.path "/lean-wss"
    assertEqual wsRequest.webSocketRequested true

    let serverWs ← runtime.httpServerRespondWebSocket server wsRequest.requestId
    let clientWs ← wsPromise.await

    (← clientWs.sendTextStart "hello-over-wss").await
    let serverMessage ← serverWs.receive
    match serverMessage with
    | .text value =>
      assertEqual value "hello-over-wss"
    | _ =>
      throw (IO.userError "expected websocket text message on secure server side")

    (← serverWs.sendTextStart "hello-over-wss-reply").await
    let clientMessage ← clientWs.receive
    match clientMessage with
    | .text value =>
      assertEqual value "hello-over-wss-reply"
    | _ =>
      throw (IO.userError "expected websocket text message on secure client side")

    clientWs.release
    serverWs.release
    server.release
  finally
    runtime.shutdown

@[test]
def testKjAsyncHttpServerListenWithConfigAndDrainStart : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let server ← runtime.httpServerListenWithConfig "127.0.0.1" {
      headerTimeoutNanos := 12000000000
      pipelineTimeoutNanos := 4000000000
      canceledUploadGracePeriodNanos := 1500000000
      canceledUploadGraceBytes := 32768
      webSocketCompressionMode := .manual
    } 0
    assertTrue (server.boundPort != 0) "server with config must bind a non-zero port"

    let responsePromise ← runtime.httpRequestStartWithHeaders
      .get "127.0.0.1" "/config-listen" #[] ByteArray.empty server.boundPort
    let request ← waitForHttpServerRequest runtime server
    assertEqual request.path "/config-listen"
    runtime.httpServerRespond server request.requestId 204 "No Content"
      #[{ name := "x-config", value := "1" }] ByteArray.empty

    let response ← responsePromise.awaitWithHeaders
    assertEqual response.status 204
    assertTrue (response.headers.any (fun h => h.name == "x-config" && h.value == "1"))
      "expected response header x-config"

    let drainPromise ← server.drainStart
    drainPromise.await
    server.release
  finally
    runtime.shutdown

@[test]
def testKjAsyncRuntimeMismatchGuardForHttpServer : IO Unit := do
  let runtimeA ← Capnp.KjAsync.Runtime.init
  let runtimeB ← Capnp.KjAsync.Runtime.init
  try
    let server ← runtimeA.httpServerListen "127.0.0.1" 0
    let mismatchErr ←
      try
        runtimeB.httpServerRelease server
        pure ""
      catch e =>
        pure (toString e)
    assertTrue (mismatchErr.contains "different Capnp.KjAsync runtime")
      "expected runtime mismatch guard when using a server handle with another runtime"
    server.release
  finally
    runtimeA.shutdown
    runtimeB.shutdown

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
