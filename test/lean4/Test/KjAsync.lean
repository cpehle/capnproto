import LeanTest
import Capnp.KjAsync
import Capnp.Rpc
import Capnp.RpcKjAsync

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
      out := ByteArray.append out chunk
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
    runtime.pump
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

    let server ← runtime.httpServerListen "127.0.0.1" 0
    try
      let connectTask ←
        runtime.webSocketConnectAsTask "127.0.0.1" "/lean-ws-task-helper" server.boundPort
      let requestA ← waitForHttpServerRequest runtime server
      assertEqual requestA.path "/lean-ws-task-helper"
      assertEqual requestA.webSocketRequested true
      let serverWsA ← runtime.httpServerRespondWebSocket server requestA.requestId
      let clientWsA ←
        match (← IO.wait connectTask) with
        | .ok webSocket => pure webSocket
        | .error err =>
          throw (IO.userError s!"Runtime.webSocketConnectAsTask failed: {err}")
      (← clientWsA.sendTextAsPromise "task-connect-payload").await
      match (← serverWsA.receive) with
      | .text value =>
        assertEqual value "task-connect-payload"
      | _ =>
        throw (IO.userError "expected text websocket message for connect task helper")
      clientWsA.release
      serverWsA.release

      let connectPromise ← runtime.webSocketConnectWithHeadersAsPromise
        "127.0.0.1" "/lean-ws-promise-helper"
        #[{ name := "x-ws-helper", value := "1" }] server.boundPort
      let requestB ← waitForHttpServerRequest runtime server
      assertEqual requestB.path "/lean-ws-promise-helper"
      assertTrue
        (requestB.headers.any (fun h => h.name == "x-ws-helper" && h.value == "1"))
        "expected x-ws-helper request header for promise helper"
      let serverWsB ← runtime.httpServerRespondWebSocket server requestB.requestId
      let clientWsB ← connectPromise.await
      (← serverWsB.sendTextAsPromise "promise-connect-payload").await
      match (← clientWsB.receive) with
      | .text value =>
        assertEqual value "promise-connect-payload"
      | _ =>
        throw (IO.userError "expected text websocket message for connect promise helper")
      clientWsB.release
      serverWsB.release
    finally
      server.release
  finally
    runtime.shutdown

@[test]
def testCapnpAsyncPromiseCatch : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let sleep ← runtime.sleepMillisStart (UInt32.ofNat 5000)
    sleep.cancel
    let p ← Capnp.Async.Promise.fromAwaitable (α := Unit) sleep
    let recovered :=
      Capnp.Async.Promise.catch p (fun _ => Capnp.Async.Promise.pure ())
    recovered.await
  finally
    runtime.shutdown

@[test]
def testCapnpAsyncPromiseAll : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let p1 ← Capnp.Async.Promise.fromAwaitable (α := Unit)
      (← runtime.sleepMillisStart (UInt32.ofNat 5))
    let p2 ← Capnp.Async.Promise.fromAwaitable (α := Unit)
      (← runtime.sleepMillisStart (UInt32.ofNat 5))
    let all := Capnp.Async.Promise.all #[p1, p2]
    let results ← all.await
    assertEqual results.size 2
  finally
    runtime.shutdown

@[test]
def testCapnpAsyncPromiseRace : IO Unit := do
  let fast : Capnp.Async.Promise UInt32 :=
    Capnp.Async.Promise.ofTask (Task.pure (.ok (UInt32.ofNat 1)))
  let slowTask ← IO.asTask do
    IO.sleep (UInt32.ofNat 50)
    pure (UInt32.ofNat 2)
  let slow := Capnp.Async.Promise.ofTask slowTask
  let raced ← Capnp.Async.Promise.race fast slow
  let value ← raced.await
  assertEqual value (UInt32.ofNat 1)

@[test]
def testKjAsyncRuntimeMRunWithNewRuntime : IO Unit := do
  let alive ← Capnp.KjAsync.RuntimeM.runWithNewRuntime do
    let alive ← Capnp.KjAsync.RuntimeM.isAlive
    Capnp.KjAsync.RuntimeM.sleepMillis (UInt32.ofNat 5)
    pure alive
  assertEqual alive true

@[test]
def testKjAsyncPromiseOpsOnRpcRuntimeHandle : IO Unit := do
  let rpcRuntime ← Capnp.Rpc.Runtime.init
  let runtime := rpcRuntime.asKjAsyncRuntime
  try
    assertEqual (← runtime.isAlive) true

    let p0 ← runtime.sleepMillisStart (UInt32.ofNat 5)
    p0.await

    let p1 ← runtime.sleepMillisStart (UInt32.ofNat 1)
    let p2 ← runtime.sleepMillisStart (UInt32.ofNat 1)
    let seq ← runtime.promiseThenStart p1 p2
    seq.await

    let p3 ← runtime.sleepMillisStart (UInt32.ofNat 1)
    let p4 ← runtime.sleepMillisStart (UInt32.ofNat 1)
    let all ← runtime.promiseAllStart #[p3, p4]
    all.await

    let slow ← runtime.sleepMillisStart (UInt32.ofNat 250)
    let fast ← runtime.sleepMillisStart (UInt32.ofNat 5)
    let race ← runtime.promiseRaceStart #[slow, fast]
    race.await

    let fail ← runtime.sleepMillisStart (UInt32.ofNat 5000)
    fail.cancel
    let fallback ← runtime.sleepMillisStart (UInt32.ofNat 1)
    let recovered ← runtime.promiseCatchStart fail fallback
    recovered.await
  finally
    rpcRuntime.shutdown

@[test]
def testKjAsyncRuntimeShutdownViaRpcHandle : IO Unit := do
  let rpcRuntime ← Capnp.Rpc.Runtime.init
  let runtime := rpcRuntime.asKjAsyncRuntime
  try
    assertEqual (← rpcRuntime.isAlive) true
    assertEqual (← runtime.isAlive) true

    runtime.shutdown

    assertEqual (← runtime.isAlive) false
    assertEqual (← rpcRuntime.isAlive) false

    let failedAfterShutdown ←
      try
        let _ ← runtime.sleepMillisStart (UInt32.ofNat 1)
        pure false
      catch _ =>
        pure true
    assertEqual failedAfterShutdown true
  finally
    if (← rpcRuntime.isAlive) then
      rpcRuntime.shutdown

@[test]
def testKjAsyncTaskSetOpsOnRpcRuntimeHandle : IO Unit := do
  let rpcRuntime ← Capnp.Rpc.Runtime.init
  let runtime := rpcRuntime.asKjAsyncRuntime
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
    rpcRuntime.shutdown

@[test]
def testKjAsyncPipeFdOpsOnRpcRuntimeHandle : IO Unit := do
  if System.Platform.isWindows then
    pure ()
  else
    let rpcRuntime ← Capnp.Rpc.Runtime.init
    let runtime := rpcRuntime.asKjAsyncRuntime
    try
      let (a, b) ← runtime.newTwoWayPipe
      let (c, d) ← runtime.newCapabilityPipe

      let aFd? ← a.dupFd?
      let bFd? ← b.dupFd?
      let cFd? ← c.dupFd?
      let dFd? ← d.dupFd?

      match (aFd?, bFd?, cFd?, dFd?) with
      | (some aFd, some bFd, some cFd, some dFd) =>
          let aT ← rpcRuntime.newTransportFromFd aFd
          let bT ← rpcRuntime.newTransportFromFd bFd
          let cT ← rpcRuntime.newTransportFromFd cFd
          let dT ← rpcRuntime.newTransportFromFd dFd
          rpcRuntime.releaseTransport aT
          rpcRuntime.releaseTransport bT
          rpcRuntime.releaseTransport cT
          rpcRuntime.releaseTransport dT
      | _ =>
          throw (IO.userError
            "expected rpc-runtime-backed Capnp.KjAsync pipe connections to expose fds")

      a.release
      b.release
      c.release
      d.release
    finally
      rpcRuntime.shutdown

@[test]
def testRpcRuntimeMRunKjAsyncBridge : IO Unit := do
  let rpcRuntime ← Capnp.Rpc.Runtime.init
  try
    let alive ← Capnp.Rpc.RuntimeM.run rpcRuntime do
      Capnp.Rpc.RuntimeM.runKjAsync do
        Capnp.KjAsync.RuntimeM.isAlive
    assertEqual alive true
  finally
    rpcRuntime.shutdown

@[test]
def testRpcRuntimeRunKjAsyncBridgeHelpers : IO Unit := do
  let rpcRuntime ← Capnp.Rpc.Runtime.init
  try
    let alive ← rpcRuntime.runKjAsync do
      Capnp.KjAsync.RuntimeM.isAlive
    assertEqual alive true

    let sameHandle ← rpcRuntime.withKjAsyncRuntime fun runtime => do
      runtime.sleepMillis (UInt32.ofNat 1)
      pure (runtime.handle == rpcRuntime.handle)
    assertEqual sameHandle true
    assertEqual (← rpcRuntime.isAlive) true
  finally
    rpcRuntime.shutdown

@[test]
def testRpcRuntimeMWithKjAsyncRuntimeHelpers : IO Unit := do
  let rpcRuntime ← Capnp.Rpc.Runtime.init
  try
    let (sameHandle, alive) ← Capnp.Rpc.RuntimeM.run rpcRuntime do
      let runtime ← Capnp.Rpc.RuntimeM.kjAsyncRuntime
      let sameHandle := runtime.handle == rpcRuntime.handle
      let alive ← Capnp.Rpc.RuntimeM.withKjAsyncRuntime fun borrowedRuntime => do
        borrowedRuntime.sleepMillis (UInt32.ofNat 1)
        borrowedRuntime.isAlive
      pure (sameHandle, alive)
    assertEqual sameHandle true
    assertEqual alive true
  finally
    rpcRuntime.shutdown

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
def testKjAsyncNetworkRoundtripSingleRuntimeAsyncPromise : IO Unit := do
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
      let acceptPromise ← listener.acceptAsPromise
      let connectPromise ← runtime.connectAsPromise address

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
def testKjAsyncNetworkRoundtripSingleRuntimeConnectAsTaskEndpoint : IO Unit := do
  if System.Platform.isWindows then
    assertTrue true "KJ unix socket endpoint task test skipped on Windows"
  else
    let (_address, socketPath) ← mkUnixTestAddress
    let runtime ← Capnp.KjAsync.Runtime.init
    try
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

      let endpoint := Capnp.KjAsync.Endpoint.unix socketPath
      let listener ← runtime.listenEndpoint endpoint
      let acceptPromise ← listener.acceptAsPromise
      let connectTask ← runtime.connectAsTaskEndpoint endpoint
      let clientConn ←
        match (← IO.wait connectTask) with
        | .ok connection => pure connection
        | .error err =>
          throw (IO.userError s!"Runtime.connectAsTaskEndpoint failed: {err}")
      let serverConn ← acceptPromise.await

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
def testKjAsyncRuntimeMConnectAsPromiseEndpointRoundtrip : IO Unit := do
  if System.Platform.isWindows then
    assertTrue true "KJ unix socket endpoint promise test skipped on Windows"
  else
    let (_address, socketPath) ← mkUnixTestAddress
    let runtime ← Capnp.KjAsync.Runtime.init
    try
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

      let endpoint := Capnp.KjAsync.Endpoint.unix socketPath
      Capnp.KjAsync.RuntimeM.run runtime do
        let listener ← Capnp.KjAsync.RuntimeM.listenEndpoint endpoint
        let acceptPromise ← listener.acceptAsPromise
        let connectPromise ← Capnp.KjAsync.RuntimeM.connectAsPromiseEndpoint endpoint
        let clientConn ← connectPromise.await
        let serverConn ← acceptPromise.await

        let payload := ByteArray.append mkPayload (ByteArray.empty.push (UInt8.ofNat 11))
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
def testKjAsyncListenerAcceptWithTimeoutMillisExpires : IO Unit := do
  if System.Platform.isWindows then
    assertTrue true "KJ unix socket timeout test skipped on Windows"
  else
    let (address, socketPath) ← mkUnixTestAddress
    let runtime ← Capnp.KjAsync.Runtime.init
    try
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

      let listener ← runtime.listen address
      let startedAt ← IO.monoNanosNow
      let accepted? ← listener.acceptWithTimeoutMillis? (UInt32.ofNat 60)
      let finishedAt ← IO.monoNanosNow
      let elapsed := finishedAt - startedAt

      assertTrue (accepted?.isNone) "acceptWithTimeoutMillis? should return none on timeout"
      assertTrue (elapsed >= 30000000)
        "accept timeout elapsed too quickly"
      listener.release
    finally
      runtime.shutdown
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

@[test]
def testKjAsyncRuntimeConnectWithTimeoutMillisSuccess : IO Unit := do
  if System.Platform.isWindows then
    assertTrue true "KJ unix socket timeout test skipped on Windows"
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
        serverConn.release

      let clientConn? ←
        clientRuntime.connectWithTimeoutMillis? address (UInt32.ofNat 250)
      match clientConn? with
      | some clientConn =>
        clientConn.release
      | none =>
        throw (IO.userError "connectWithTimeoutMillis? unexpectedly timed out")

      let serverResult ← IO.wait serverTask
      match serverResult with
      | .ok _ => pure ()
      | .error err =>
        throw (IO.userError s!"server task failed: {err}")

      listener.release
    finally
      serverRuntime.shutdown
      clientRuntime.shutdown
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

@[test]
def testKjAsyncConnectionPromiseTimeoutThenCancelRelease : IO Unit := do
  if System.Platform.isWindows then
    assertTrue true "KJ unix socket timeout edge test skipped on Windows"
  else
    let (address, socketPath) ← mkUnixTestAddress
    let runtime ← Capnp.KjAsync.Runtime.init
    try
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

      let listener ← runtime.listen address
      let pending ← listener.acceptStart
      let startedAt ← IO.monoNanosNow
      let accepted? ← pending.awaitWithTimeoutMillis? (UInt32.ofNat 40)
      let finishedAt ← IO.monoNanosNow
      let elapsed := finishedAt - startedAt

      assertTrue (accepted?.isNone)
        "ConnectionPromiseRef.awaitWithTimeoutMillis? should return none on timeout"
      assertTrue (elapsed >= 20000000)
        "connection promise timeout elapsed too quickly"
      assertTrue (elapsed < 6000000000)
        "connection promise timeout appears to hang"

      let cancelErr ←
        try
          pending.cancel
          pure ""
        catch e =>
          pure (toString e)
      assertTrue (cancelErr.contains "unknown KJ connection promise id")
        "timed-out connection promise should be cleaned up before cancel"

      let releaseErr ←
        try
          pending.release
          pure ""
        catch e =>
          pure (toString e)
      assertTrue (releaseErr.contains "unknown KJ connection promise id")
        "timed-out connection promise should be cleaned up before release"

      let probe ← runtime.sleepMillisStart (UInt32.ofNat 1)
      probe.await
      listener.release
    finally
      runtime.shutdown
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

@[test]
def testKjAsyncRuntimeConnectWithRetryExhaustedNoHang : IO Unit := do
  if System.Platform.isWindows then
    assertTrue true "KJ unix socket retry edge test skipped on Windows"
  else
    let (address, socketPath) ← mkUnixTestAddress
    let runtime ← Capnp.KjAsync.Runtime.init
    try
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

      let startedAt ← IO.monoNanosNow
      let errMsg ←
        try
          let unexpected ←
            runtime.connectWithRetry address (UInt32.ofNat 3) (UInt32.ofNat 25)
          unexpected.release
          pure ""
        catch e =>
          pure (toString e)
      let finishedAt ← IO.monoNanosNow
      let elapsed := finishedAt - startedAt

      assertTrue (!errMsg.isEmpty)
        "connectWithRetry should fail after attempts are exhausted"
      assertTrue (elapsed >= 20000000)
        "connectWithRetry exhaustion elapsed too quickly"
      assertTrue (elapsed < 6000000000)
        "connectWithRetry exhaustion appears to hang"

      let probe ← runtime.sleepMillisStart (UInt32.ofNat 1)
      probe.await
    finally
      runtime.shutdown
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

@[test]
def testKjAsyncRuntimeConnectWithRetryAsPromiseExhaustedNoHang : IO Unit := do
  if System.Platform.isWindows then
    assertTrue true "KJ unix socket retry edge test skipped on Windows"
  else
    let (address, socketPath) ← mkUnixTestAddress
    let runtime ← Capnp.KjAsync.Runtime.init
    try
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

      let connectPromise ←
        runtime.connectWithRetryAsPromise address (UInt32.ofNat 3) (UInt32.ofNat 25)
      let startedAt ← IO.monoNanosNow
      let errMsg ←
        try
          let unexpected ← connectPromise.await
          unexpected.release
          pure ""
        catch e =>
          pure (toString e)
      let finishedAt ← IO.monoNanosNow
      let elapsed := finishedAt - startedAt

      assertTrue (!errMsg.isEmpty)
        "connectWithRetryAsPromise should fail after attempts are exhausted"
      assertTrue (elapsed >= 20000000)
        "connectWithRetryAsPromise exhaustion elapsed too quickly"
      assertTrue (elapsed < 6000000000)
        "connectWithRetryAsPromise exhaustion appears to hang"

      let probe ← runtime.sleepMillisStart (UInt32.ofNat 1)
      probe.await
    finally
      runtime.shutdown
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

@[test]
def testKjAsyncRuntimeConnectWithRetryListenerAppears : IO Unit := do
  if System.Platform.isWindows then
    assertTrue true "KJ unix socket retry helper test skipped on Windows"
  else
    let (address, socketPath) ← mkUnixTestAddress
    let serverRuntime ← Capnp.KjAsync.Runtime.init
    let clientRuntime ← Capnp.KjAsync.Runtime.init
    try
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

      let serverTask ← IO.asTask do
        serverRuntime.sleepMillis (UInt32.ofNat 60)
        let listener ← serverRuntime.listen address
        try
          let serverConn ← listener.accept
          serverConn.release
        finally
          listener.release

      let clientConn ←
        clientRuntime.connectWithRetry address (UInt32.ofNat 6) (UInt32.ofNat 20)
      clientConn.release

      let serverResult ← IO.wait serverTask
      match serverResult with
      | .ok _ => pure ()
      | .error err =>
        throw (IO.userError s!"server task failed: {err}")
    finally
      serverRuntime.shutdown
      clientRuntime.shutdown
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

@[test]
def testKjAsyncRuntimeConnectWithRetryAsPromiseListenerAppears : IO Unit := do
  if System.Platform.isWindows then
    assertTrue true "KJ unix socket retry helper test skipped on Windows"
  else
    let (address, socketPath) ← mkUnixTestAddress
    let serverRuntime ← Capnp.KjAsync.Runtime.init
    let clientRuntime ← Capnp.KjAsync.Runtime.init
    try
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

      let serverTask ← IO.asTask do
        serverRuntime.sleepMillis (UInt32.ofNat 60)
        let listener ← serverRuntime.listen address
        try
          let serverConn ← listener.accept
          serverConn.release
        finally
          listener.release

      let connectPromise ←
        clientRuntime.connectWithRetryAsPromise address (UInt32.ofNat 6) (UInt32.ofNat 20)
      let clientConn ← connectPromise.await
      clientConn.release

      let serverResult ← IO.wait serverTask
      match serverResult with
      | .ok _ => pure ()
      | .error err =>
        throw (IO.userError s!"server task failed: {err}")
    finally
      serverRuntime.shutdown
      clientRuntime.shutdown
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

@[test]
def testKjAsyncRuntimeWithConnectionHelper : IO Unit := do
  if System.Platform.isWindows then
    assertTrue true "KJ unix socket helper test skipped on Windows"
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
        let req ← serverConn.read (1 : UInt32) (1024 : UInt32)
        serverConn.write req
        serverConn.shutdownWrite
        serverConn.release
      clientRuntime.withConnection address (fun clientConn => do
        let payload := mkPayload
        clientConn.write payload
        clientConn.shutdownWrite
        let echoed ← clientConn.read (UInt32.ofNat payload.size) (UInt32.ofNat payload.size)
        assertEqual echoed payload
      )
      let serverResult ← IO.wait serverTask
      match serverResult with
      | .ok _ => pure ()
      | .error err =>
        throw (IO.userError s!"server task failed: {err}")
      listener.release
    finally
      serverRuntime.shutdown
      clientRuntime.shutdown
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

@[test]
def testKjAsyncListenerWithAcceptHelper : IO Unit := do
  if System.Platform.isWindows then
    assertTrue true "KJ unix socket helper test skipped on Windows"
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
        listener.withAccept fun serverConn => do
          let req ← serverConn.read (1 : UInt32) (1024 : UInt32)
          serverConn.write req
          serverConn.shutdownWrite
      let clientConn ← clientRuntime.connect address
      let payload := mkPayload
      clientConn.write payload
      clientConn.shutdownWrite
      let echoed ← clientConn.read (UInt32.ofNat payload.size) (UInt32.ofNat payload.size)
      assertEqual echoed payload
      clientConn.release
      let serverResult ← IO.wait serverTask
      match serverResult with
      | .ok _ => pure ()
      | .error err =>
        throw (IO.userError s!"server task failed: {err}")
      listener.release
    finally
      serverRuntime.shutdown
      clientRuntime.shutdown
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()

@[test]
def testKjAsyncRuntimeWithListenerHelper : IO Unit := do
  if System.Platform.isWindows then
    assertTrue true "KJ unix socket helper test skipped on Windows"
  else
    let (address, socketPath) ← mkUnixTestAddress
    let runtime ← Capnp.KjAsync.Runtime.init
    try
      try
        IO.FS.removeFile socketPath
      catch _ =>
        pure ()
      let opened ← runtime.withListener address (fun _ => pure true)
      assertEqual opened true
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

    let p3 ← runtime.sleepMillisStart (UInt32.ofNat 1)
    let p4 ← runtime.sleepMillisStart (UInt32.ofNat 1)
    let seq ← runtime.promiseThenStart p3 p4
    seq.await

    let fail ← runtime.sleepMillisStart (UInt32.ofNat 5000)
    fail.cancel
    let fallback ← runtime.sleepMillisStart (UInt32.ofNat 1)
    let recovered ← runtime.promiseCatchStart fail fallback
    recovered.await

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
def testKjAsyncPromiseRefCombinatorSugar : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let p1 ← runtime.sleepMillisStart (UInt32.ofNat 1)
    let p2 ← runtime.sleepMillisStart (UInt32.ofNat 1)
    let seq ← p1.then p2
    seq.await

    let p3 ← runtime.sleepMillisStart (UInt32.ofNat 1)
    let p4 ← runtime.sleepMillisStart (UInt32.ofNat 1)
    let all ← p3.all #[p4]
    all.await

    let fail ← runtime.sleepMillisStart (UInt32.ofNat 5000)
    fail.cancel
    let fallback ← runtime.sleepMillisStart (UInt32.ofNat 1)
    let recovered ← fail.catch fallback
    recovered.await

    let slow ← runtime.sleepMillisStart (UInt32.ofNat 200)
    let fast ← runtime.sleepMillisStart (UInt32.ofNat 5)
    let race ← slow.race #[fast]
    let startedAt ← IO.monoNanosNow
    race.await
    let finishedAt ← IO.monoNanosNow
    let elapsed := finishedAt - startedAt
    assertTrue (elapsed < 500000000) "PromiseRef.race helper took too long"
  finally
    runtime.shutdown

@[test]
def testKjAsyncPromiseRefFlowHelpers : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let p1 ← runtime.sleepMillisStart (UInt32.ofNat 1)
    let p2 ← runtime.sleepMillisStart (UInt32.ofNat 1)
    p1.thenAwait p2

    let p3 ← runtime.sleepMillisStart (UInt32.ofNat 1)
    let p4 ← runtime.sleepMillisStart (UInt32.ofNat 1)
    p3.allAwait #[p4]

    let fail ← runtime.sleepMillisStart (UInt32.ofNat 5000)
    fail.cancel
    let fallback ← runtime.sleepMillisStart (UInt32.ofNat 1)
    fail.catchAwait fallback

    let pending ← runtime.sleepMillisStart (UInt32.ofNat 5000)
    pending.cancelAndRelease

    let probe ← runtime.sleepMillisStart (UInt32.ofNat 1)
    probe.await
  finally
    runtime.shutdown

@[test]
def testKjAsyncPromiseRefCombinatorRuntimeMismatch : IO Unit := do
  let runtimeA ← Capnp.KjAsync.Runtime.init
  let runtimeB ← Capnp.KjAsync.Runtime.init
  try
    let promiseA ← runtimeA.sleepMillisStart (UInt32.ofNat 1)
    let promiseB ← runtimeB.sleepMillisStart (UInt32.ofNat 1)
    let mismatchErr ←
      try
        let combined ← promiseA.allStart #[promiseB]
        combined.await
        pure ""
      catch e =>
        pure (toString e)
    assertTrue (mismatchErr.contains "different Capnp.KjAsync runtime")
      "expected PromiseRef.allStart runtime mismatch guard"
    promiseA.release
    promiseB.release
  finally
    runtimeA.shutdown
    runtimeB.shutdown

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
def testKjAsyncRuntimeMDatagramBindEndpoint : IO Unit := do
  let senderRuntime ← Capnp.KjAsync.Runtime.init
  let receiverRuntime ← Capnp.KjAsync.Runtime.init
  try
    let receiverEndpoint := Capnp.KjAsync.Endpoint.tcp "127.0.0.1" 0
    let senderEndpoint := Capnp.KjAsync.Endpoint.tcp "127.0.0.1" 0
    let receiverPort ← Capnp.KjAsync.RuntimeM.run receiverRuntime do
      Capnp.KjAsync.RuntimeM.datagramBindEndpoint receiverEndpoint
    let receiverPortNumber ← receiverPort.getPort
    let senderPort ← Capnp.KjAsync.RuntimeM.run senderRuntime do
      Capnp.KjAsync.RuntimeM.datagramBindEndpoint senderEndpoint
    let payload := ByteArray.append mkPayload (ByteArray.empty.push (UInt8.ofNat 21))

    let receiveTask ← receiverRuntime.datagramReceiveAsTask receiverPort (UInt32.ofNat 1024)
    let sendPromise ←
      senderRuntime.datagramSendAsPromise senderPort "127.0.0.1" payload receiverPortNumber
    let sentCount ← sendPromise.await
    assertEqual sentCount (UInt32.ofNat payload.size)

    let (_source, bytes) ←
      match (← IO.wait receiveTask) with
      | .ok datagram => pure datagram
      | .error err =>
        throw (IO.userError s!"RuntimeM.datagramBindEndpoint receive failed: {err}")
    assertEqual bytes payload

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
def testKjAsyncTwoWayPipeAsyncTaskAndPromiseHelpers : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let (left, right) ← runtime.newTwoWayPipe
    let payload := mkPayload

    let writeTask ← left.writeAsTask payload
    let readTask ← right.readAsTask (UInt32.ofNat 1) (UInt32.ofNat 1024)

    match (← IO.wait writeTask) with
    | .ok _ => pure ()
    | .error err =>
      throw (IO.userError s!"writeAsTask failed: {err}")

    let received ←
      match (← IO.wait readTask) with
      | .ok value => pure value
      | .error err => throw (IO.userError s!"readAsTask failed: {err}")
    assertEqual received payload

    let shutdownPromise ← left.shutdownWriteAsPromise
    shutdownPromise.await

    let disconnectPromise ← right.whenWriteDisconnectedAsPromise
    left.release
    disconnectPromise.await
    right.release
  finally
    runtime.shutdown

@[test]
def testKjAsyncConnectionReadAllAndPipeHelpers : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let payload1 := mkPayload
    let payload2 := ByteArray.append (ByteArray.empty.push (UInt8.ofNat 45)) mkPayload
    let expected := ByteArray.append payload1 payload2

    let (writer, reader) ← runtime.newTwoWayPipe
    writer.write payload1
    writer.write payload2
    writer.shutdownWrite
    let all ← reader.readAll (4 : UInt32)
    assertEqual all expected
    writer.release
    reader.release

    let (sourceWrite, sourceRead) ← runtime.newTwoWayPipe
    let (targetWrite, targetRead) ← runtime.newTwoWayPipe
    sourceWrite.write payload1
    sourceWrite.write payload2
    sourceWrite.shutdownWrite
    let copied ← sourceRead.pipeToAndShutdownWrite targetWrite (4 : UInt32)
    assertEqual copied expected.size.toUInt64
    let piped ← targetRead.readAll (4 : UInt32)
    assertEqual piped expected
    sourceWrite.release
    sourceRead.release
    targetWrite.release
    targetRead.release
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
def testKjAsyncDatagramSendAwaitAndReceiveManyHelpers : IO Unit := do
  let senderRuntime ← Capnp.KjAsync.Runtime.init
  let receiverRuntime ← Capnp.KjAsync.Runtime.init
  try
    let receiverPort ← receiverRuntime.datagramBind "127.0.0.1" 0
    let receiverPortNumber ← receiverPort.getPort
    let senderPort ← senderRuntime.datagramBind "127.0.0.1" 0
    let payload1 := mkPayload
    let payload2 := ByteArray.append mkPayload (ByteArray.empty.push (UInt8.ofNat 1))

    let receiveTask ← IO.asTask do
      receiverPort.receiveMany (2 : UInt32) (1024 : UInt32)

    let sent1 ← senderPort.sendAwait "127.0.0.1" payload1 receiverPortNumber
    let sent2 ← senderPort.sendAwait "127.0.0.1" payload2 receiverPortNumber
    assertEqual sent1 (UInt32.ofNat payload1.size)
    assertEqual sent2 (UInt32.ofNat payload2.size)

    let receiveResult ← IO.wait receiveTask
    let received ←
      match receiveResult with
      | .ok datagrams => pure datagrams
      | .error err =>
        throw (IO.userError s!"datagram receiveMany task failed: {err}")
    assertEqual received.size 2
    assertTrue (received.any (fun (_, bytes) => bytes == payload1))
      "receiveMany missing payload1"
    assertTrue (received.any (fun (_, bytes) => bytes == payload2))
      "receiveMany missing payload2"

    senderPort.release
    receiverPort.release
  finally
    senderRuntime.shutdown
    receiverRuntime.shutdown

@[test]
def testKjAsyncDatagramPeerRoundtripConveniences : IO Unit := do
  let leftRuntime ← Capnp.KjAsync.Runtime.init
  let rightRuntime ← Capnp.KjAsync.Runtime.init
  try
    let leftSeedPeer ← leftRuntime.datagramPeerBind "127.0.0.1" "127.0.0.1" 0
    let rightSeedPeer ← rightRuntime.datagramPeerBind "127.0.0.1" "127.0.0.1" 0
    let leftPortNumber ← leftSeedPeer.port.getPort
    let rightPortNumber ← rightSeedPeer.port.getPort
    let leftPeer : Capnp.KjAsync.DatagramPeer := { leftSeedPeer with remotePort := rightPortNumber }
    let rightPeer : Capnp.KjAsync.DatagramPeer := { rightSeedPeer with remotePort := leftPortNumber }
    let payloadA := mkPayload
    let payloadB := ByteArray.append mkPayload (ByteArray.empty.push (UInt8.ofNat 7))
    try
      assertEqual leftPeer.remoteAddress "127.0.0.1"
      assertEqual leftPeer.remotePort rightPortNumber
      assertEqual rightPeer.remoteAddress "127.0.0.1"
      assertEqual rightPeer.remotePort leftPortNumber

      let receiveAtRight ← IO.asTask do
        rightPeer.receive (UInt32.ofNat 1024)
      let sentForward ← leftPeer.sendAwait payloadA
      assertEqual sentForward (UInt32.ofNat payloadA.size)
      match (← IO.wait receiveAtRight) with
      | .ok (_source, bytes) =>
        assertEqual bytes payloadA
      | .error err =>
        throw (IO.userError s!"datagram peer forward receive task failed: {err}")

      let receiveAtLeft ← IO.asTask do
        leftPeer.receive (UInt32.ofNat 1024)
      let sentBack ← rightPeer.sendAwait payloadB
      assertEqual sentBack (UInt32.ofNat payloadB.size)
      match (← IO.wait receiveAtLeft) with
      | .ok (_source, bytes) =>
        assertEqual bytes payloadB
      | .error err =>
        throw (IO.userError s!"datagram peer reverse receive task failed: {err}")
    finally
      leftPeer.release
      rightPeer.release
  finally
    leftRuntime.shutdown
    rightRuntime.shutdown

@[test]
def testKjAsyncDatagramTaskAndPromiseHelpers : IO Unit := do
  let senderRuntime ← Capnp.KjAsync.Runtime.init
  let receiverRuntime ← Capnp.KjAsync.Runtime.init
  try
    let receiverPort ← receiverRuntime.datagramBind "127.0.0.1" 0
    let receiverPortNumber ← receiverPort.getPort
    let senderPort ← senderRuntime.datagramBind "127.0.0.1" 0
    let payload := ByteArray.append mkPayload (ByteArray.empty.push (UInt8.ofNat 9))

    let receiveTask ← receiverRuntime.datagramReceiveAsTask receiverPort (UInt32.ofNat 1024)
    let sendPromise ←
      senderRuntime.datagramSendAsPromise senderPort "127.0.0.1" payload receiverPortNumber
    let sentCount ← sendPromise.await
    assertEqual sentCount (UInt32.ofNat payload.size)
    let (_source, bytes) ←
      match (← IO.wait receiveTask) with
      | .ok datagram => pure datagram
      | .error err =>
        throw (IO.userError s!"datagramReceiveAsTask failed: {err}")
    assertEqual bytes payload

    let receivePromise ← receiverPort.receiveAsPromise (UInt32.ofNat 1024)
    let sendTask ← senderPort.sendAsTask "127.0.0.1" payload receiverPortNumber
    let sentCount2 ←
      match (← IO.wait sendTask) with
      | .ok value => pure value
      | .error err =>
        throw (IO.userError s!"DatagramPort.sendAsTask failed: {err}")
    assertEqual sentCount2 (UInt32.ofNat payload.size)
    let (_source2, bytes2) ← receivePromise.await
    assertEqual bytes2 payload

    let leftSeedPeer ← senderRuntime.datagramPeerBind "127.0.0.1" "127.0.0.1" 0
    let rightSeedPeer ← receiverRuntime.datagramPeerBind "127.0.0.1" "127.0.0.1" 0
    let leftPortNumber ← leftSeedPeer.port.getPort
    let rightPortNumber ← rightSeedPeer.port.getPort
    let leftPeer : Capnp.KjAsync.DatagramPeer := { leftSeedPeer with remotePort := rightPortNumber }
    let rightPeer : Capnp.KjAsync.DatagramPeer := { rightSeedPeer with remotePort := leftPortNumber }
    let peerPayload := ByteArray.append payload (ByteArray.empty.push (UInt8.ofNat 3))
    try
      let peerReceivePromise ← rightPeer.receiveAsPromise (UInt32.ofNat 1024)
      let peerSendTask ← leftPeer.sendAsTask peerPayload
      let peerSentCount ←
        match (← IO.wait peerSendTask) with
        | .ok value => pure value
        | .error err =>
          throw (IO.userError s!"DatagramPeer.sendAsTask failed: {err}")
      assertEqual peerSentCount (UInt32.ofNat peerPayload.size)
      let (_peerSource, peerBytes) ← peerReceivePromise.await
      assertEqual peerBytes peerPayload

      let reverseReceiveTask ← leftPeer.receiveAsTask (UInt32.ofNat 1024)
      let reverseSendPromise ← rightPeer.sendAsPromise payload
      let reverseSentCount ← reverseSendPromise.await
      assertEqual reverseSentCount (UInt32.ofNat payload.size)
      let (_reverseSource, reverseBytes) ←
        match (← IO.wait reverseReceiveTask) with
        | .ok datagram => pure datagram
        | .error err =>
          throw (IO.userError s!"DatagramPeer.receiveAsTask failed: {err}")
      assertEqual reverseBytes payload
    finally
      leftPeer.release
      rightPeer.release

    senderPort.release
    receiverPort.release
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
def testKjAsyncWebSocketTaskAndPromiseHelpers : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let (left, right) ← runtime.newWebSocketPipe
    try
      let receiveTextPromise ← right.receiveAsPromise
      let sendTextTask ← left.sendTextAsTask "lean-kjasync-text-task"
      match (← IO.wait sendTextTask) with
      | .ok () => pure ()
      | .error err =>
        throw (IO.userError s!"WebSocket.sendTextAsTask failed: {err}")
      let textMessage ← receiveTextPromise.await
      match textMessage with
      | .text value =>
        assertEqual value "lean-kjasync-text-task"
      | _ =>
        throw (IO.userError "expected websocket text message from receiveAsPromise")

      let payload := ByteArray.append mkPayload (ByteArray.empty.push (UInt8.ofNat 5))
      let receiveBinaryTask ← right.receiveWithMaxAsTask (UInt32.ofNat 1024)
      let sendBinaryPromise ← left.sendBinaryAsPromise payload
      sendBinaryPromise.await
      let binaryMessage ←
        match (← IO.wait receiveBinaryTask) with
        | .ok message => pure message
        | .error err =>
          throw (IO.userError s!"WebSocket.receiveWithMaxAsTask failed: {err}")
      match binaryMessage with
      | .binary bytes =>
        assertEqual bytes payload
      | _ =>
        throw (IO.userError "expected websocket binary message from receiveWithMaxAsTask")

    finally
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
def testKjAsyncWebSocketCloseCodeHelpers : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  let checkClose
      (sendClose : Capnp.KjAsync.WebSocket -> IO (Option Capnp.KjAsync.PromiseRef))
      (expectedCode : UInt16) (expectedReason : String) : IO Unit := do
    let (sender, receiver) ← runtime.newWebSocketPipe
    try
      let closePromise? ← sendClose sender
      match (← receiver.receive) with
      | .close code reason => do
          assertEqual code expectedCode
          assertEqual reason expectedReason
      | _ =>
          throw (IO.userError "expected websocket close message")
      match closePromise? with
      | some closePromise =>
          closePromise.await
      | none =>
          pure ()
    finally
      sender.release
      receiver.release
  try
    checkClose
      (fun ws => do
        let closePromise ← runtime.webSocketCloseStartCode ws (4000 : UInt32) "runtime-close-code"
        pure (some closePromise))
      (UInt16.ofNat 4000) "runtime-close-code"
    checkClose
      (fun ws => do
        let closePromise ← ws.closeStartCode (4001 : UInt32) "ws-close-code"
        pure (some closePromise))
      (UInt16.ofNat 4001) "ws-close-code"
    checkClose
      (fun ws => do
        let closePromise ← ws.closeStartCode (4002 : UInt32) "ws-close-code-2"
        pure (some closePromise))
      (UInt16.ofNat 4002) "ws-close-code-2"
  finally
    runtime.shutdown

@[test]
def testKjAsyncHttpRequestTaskAndPromiseHelpers : IO Unit := do
  let serverRuntime ← Capnp.KjAsync.Runtime.init
  let clientRuntime ← Capnp.KjAsync.Runtime.init
  try
    let server ← serverRuntime.httpServerListen "127.0.0.1" 0
    let requestBodyA := "http-task-helper-a".toUTF8
    let responseTask ← clientRuntime.httpRequestWithHeadersAsTask
      .post "127.0.0.1" "/lean-http-task" #[{ name := "x-http-helper", value := "task" }]
      requestBodyA server.boundPort

    let requestA ← waitForHttpServerRequest serverRuntime server
    assertEqual requestA.path "/lean-http-task"
    assertEqual requestA.body requestBodyA
    serverRuntime.httpServerRespond server requestA.requestId (UInt32.ofNat 201) "Created"
      #[{ name := "x-http-helper", value := "task-ok" }] requestBodyA
    -- Pump the server runtime loop so the response flushes before awaiting on client helpers.
    serverRuntime.pump

    let responseA ←
      match (← IO.wait responseTask) with
      | .ok response => pure response
      | .error err =>
        throw (IO.userError s!"httpRequestWithHeadersAsTask failed: {err}")
    assertEqual responseA.status (UInt32.ofNat 201)
    assertEqual responseA.statusText "Created"
    assertEqual responseA.body requestBodyA
    assertTrue
      (responseA.headers.any (fun h => h.name == "x-http-helper" && h.value == "task-ok"))
      "expected x-http-helper response header for task helper"

    let requestBodyB := "http-task-helper-b".toUTF8
    let responsePromise ← clientRuntime.httpRequestWithHeadersAsPromise
      .post "127.0.0.1" "/lean-http-promise" #[{ name := "x-http-helper", value := "promise" }]
      requestBodyB server.boundPort

    let requestB ← waitForHttpServerRequest serverRuntime server
    assertEqual requestB.path "/lean-http-promise"
    assertEqual requestB.body requestBodyB
    serverRuntime.httpServerRespond server requestB.requestId (UInt32.ofNat 202) "Accepted"
      #[{ name := "x-http-helper", value := "promise-ok" }] requestBodyB
    serverRuntime.pump

    let responseB ← responsePromise.await
    assertEqual responseB.status (UInt32.ofNat 202)
    assertEqual responseB.statusText "Accepted"
    assertEqual responseB.body requestBodyB
    assertTrue
      (responseB.headers.any (fun h => h.name == "x-http-helper" && h.value == "promise-ok"))
      "expected x-http-helper response header for promise helper"

    server.release
  finally
    clientRuntime.shutdown
    serverRuntime.shutdown

@[test]
def testKjAsyncHttpEncodedHeaderApis : IO Unit := do
  let requestHeaders := #[{ name := "x-http-encoded", value := "request" }]
  let encodedRequestHeaders := Capnp.KjAsync.encodeHttpHeaders requestHeaders
  let decodedRequestHeaders ← Capnp.KjAsync.decodeHttpHeaders encodedRequestHeaders
  assertTrue (decodedRequestHeaders == requestHeaders)
    "decodeHttpHeaders should roundtrip encodeHttpHeaders for request headers"

  let serverRuntime ← Capnp.KjAsync.Runtime.init
  let clientRuntime ← Capnp.KjAsync.Runtime.init
  try
    let server ← serverRuntime.httpServerListen "127.0.0.1" 0
    let responseHeaders := #[{ name := "x-http-encoded", value := "response" }]
    let encodedResponseHeaders := Capnp.KjAsync.encodeHttpHeaders responseHeaders

    let requestBody := "http-encoded".toUTF8
    let responseTask ← clientRuntime.httpRequestWithHeadersAsTask
      .post "127.0.0.1" "/lean-http-encoded"
      requestHeaders requestBody server.boundPort

    let request ← waitForHttpServerRequest serverRuntime server
    assertEqual request.path "/lean-http-encoded"
    assertEqual request.body requestBody
    assertTrue
      (request.headers.any (fun h => h.name == "x-http-encoded" && h.value == "request"))
      "expected x-http-encoded request header for encoded request helper"

    serverRuntime.httpServerRespondWithEncodedHeaders server request.requestId
      (UInt32.ofNat 203) "Non-Authoritative Information" encodedResponseHeaders requestBody
    serverRuntime.pump

    let response ←
      match (← IO.wait responseTask) with
      | .ok response => pure response
      | .error err =>
        throw (IO.userError s!"httpRequestWithHeadersAsTask failed: {err}")
    assertEqual response.status (UInt32.ofNat 203)
    assertEqual response.statusText "Non-Authoritative Information"
    assertEqual response.body requestBody
    assertTrue
      (response.headers.any (fun h => h.name == "x-http-encoded" && h.value == "response"))
      "expected x-http-encoded response header for runtime encoded response helper"

    server.release
  finally
    clientRuntime.shutdown
    serverRuntime.shutdown

@[test]
def testKjAsyncHttpEncodedResponsePromiseAwaitApi : IO Unit := do
  let requestHeaders := #[{ name := "x-http-encoded", value := "request" }]
  let responseHeaders := #[{ name := "x-http-encoded", value := "response" }]
  let encodedResponseHeaders := Capnp.KjAsync.encodeHttpHeaders responseHeaders

  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let server ← runtime.httpServerListen "127.0.0.1" 0
    let requestBody := "http-encoded-promise".toUTF8
    let responseRef ← runtime.httpRequestStartWithHeaders
      .post "127.0.0.1" "/lean-http-encoded-promise"
      requestHeaders requestBody server.boundPort

    let request ← waitForHttpServerRequest runtime server
    assertEqual request.path "/lean-http-encoded-promise"
    assertEqual request.body requestBody
    runtime.httpServerRespondWithEncodedHeaders server request.requestId
      (UInt32.ofNat 201) "Created" encodedResponseHeaders requestBody
    runtime.pump

    let encodedResponse ← responseRef.awaitWithEncodedHeaders
    assertEqual encodedResponse.status (UInt32.ofNat 201)
    assertEqual encodedResponse.statusText "Created"
    assertEqual encodedResponse.body requestBody
    let decodedHeaders ← Capnp.KjAsync.decodeHttpHeaders encodedResponse.encodedHeaders
    assertTrue (decodedHeaders.size >= responseHeaders.size)
      "expected encoded promise response headers to include user-provided headers"
    for expected in responseHeaders do
      assertTrue (decodedHeaders.any (fun h => h == expected))
        s!"expected encoded promise response header {expected.name}"

    server.release
  finally
    runtime.shutdown

@[test]
def testKjAsyncHttpEncodedResponseRequestApi : IO Unit := do
  let requestHeaders := #[{ name := "x-http-encoded", value := "request" }]
  let encodedRequestHeaders := Capnp.KjAsync.encodeHttpHeaders requestHeaders
  let responseHeaders := #[{ name := "x-http-encoded", value := "response" }]
  let encodedResponseHeaders := Capnp.KjAsync.encodeHttpHeaders responseHeaders

  let serverRuntime ← Capnp.KjAsync.Runtime.init
  let clientRuntime ← Capnp.KjAsync.Runtime.init
  try
    let server ← serverRuntime.httpServerListen "127.0.0.1" 0
    let requestBody := "http-encoded-raw".toUTF8
    let responseTask ← IO.asTask do
      clientRuntime.httpRequestWithEncodedHeadersAndEncodedResponseHeaders
        .post "127.0.0.1" "/lean-http-encoded-raw"
        encodedRequestHeaders requestBody server.boundPort

    let request ← waitForHttpServerRequest serverRuntime server
    assertEqual request.path "/lean-http-encoded-raw"
    assertEqual request.body requestBody
    serverRuntime.httpServerRespondWithEncodedHeaders server request.requestId
      (UInt32.ofNat 200) "OK" encodedResponseHeaders requestBody
    serverRuntime.pump

    let encodedResponse ←
      match (← IO.wait responseTask) with
      | .ok response => pure response
      | .error err =>
        throw (IO.userError s!"httpRequestWithEncodedHeadersAndEncodedResponseHeaders failed: {err}")
    assertEqual encodedResponse.status (UInt32.ofNat 200)
    assertEqual encodedResponse.statusText "OK"
    assertEqual encodedResponse.body requestBody
    let decodedHeaders ← Capnp.KjAsync.decodeHttpHeaders encodedResponse.encodedHeaders
    assertTrue (decodedHeaders.size >= responseHeaders.size)
      "expected encoded response headers to include user-provided headers"
    for expected in responseHeaders do
      assertTrue (decodedHeaders.any (fun h => h == expected))
        s!"expected encoded response header {expected.name}"

    server.release
  finally
    clientRuntime.shutdown
    serverRuntime.shutdown

@[test]
def testKjAsyncHttpStreamingBodyTaskAndPromiseHelpers : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let server ← runtime.httpServerListen "127.0.0.1" 0
    let requestPartA := "stream-helper-request-a".toUTF8
    let requestPartB := "-and-b".toUTF8
    let expectedRequestBody := ByteArray.append requestPartA requestPartB
    let (requestBody?, responsePromise) ← runtime.httpRequestStartStreamingWithHeaders
      .post "127.0.0.1" "/lean-http-stream-helper"
      #[{ name := "x-stream-helper", value := "1" }] server.boundPort
    let requestBody ←
      match requestBody? with
      | some body => pure body
      | none => throw (IO.userError "expected streaming HTTP request body handle")

    let writeTask ← requestBody.writeAsTask requestPartA
    match (← IO.wait writeTask) with
    | .ok () => pure ()
    | .error err =>
      throw (IO.userError s!"HttpRequestBody.writeAsTask failed: {err}")
    (← requestBody.writeAsPromise requestPartB).await
    (← requestBody.finishAsPromise).await

    let request ← waitForHttpServerRequestRaw runtime server
    assertEqual request.path "/lean-http-stream-helper"
    let requestBodyStream ←
      match request.bodyStream? with
      | some body => pure body
      | none => throw (IO.userError "expected streamed server request body")
    let firstChunk ←
      match (← IO.wait (← requestBodyStream.readAsTask (UInt32.ofNat 1) (UInt32.ofNat 64))) with
      | .ok chunk => pure chunk
      | .error err =>
        throw (IO.userError s!"HttpServerRequestBody.readAsTask failed: {err}")
    let mut receivedRequestBody := ByteArray.empty
    let mut done := false
    if firstChunk.size == 0 then
      done := true
    else
      receivedRequestBody := ByteArray.append receivedRequestBody firstChunk
    while !done do
      let chunk ← (← requestBodyStream.readAsPromise (UInt32.ofNat 1) (UInt32.ofNat 64)).await
      if chunk.size == 0 then
        done := true
      else
        receivedRequestBody := ByteArray.append receivedRequestBody chunk
    requestBodyStream.release
    assertEqual receivedRequestBody expectedRequestBody

    let responseBody ← runtime.httpServerRespondStartStreaming server request.requestId
      (UInt32.ofNat 203) "Non-Authoritative Information"
      #[{ name := "x-stream-helper-response", value := "1" }]
    let responsePartA := "stream-helper-response-a".toUTF8
    let responsePartB := "-and-b".toUTF8
    let expectedResponseBody := ByteArray.append responsePartA responsePartB
    match (← IO.wait (← responseBody.writeAsTask responsePartA)) with
    | .ok () => pure ()
    | .error err =>
      throw (IO.userError s!"HttpServerResponseBody.writeAsTask failed: {err}")
    (← responseBody.writeAsPromise responsePartB).await
    (← responseBody.finishAsPromise).await

    let response ← responsePromise.awaitStreamingWithHeaders
    assertEqual response.status (UInt32.ofNat 203)
    assertEqual response.statusText "Non-Authoritative Information"
    assertTrue
      (response.headers.any
        (fun h => h.name == "x-stream-helper-response" && h.value == "1"))
      "expected x-stream-helper-response header"
    let firstResponseChunk ←
      match (← IO.wait (← response.body.readAsTask (UInt32.ofNat 1) (UInt32.ofNat 64))) with
      | .ok chunk => pure chunk
      | .error err =>
        throw (IO.userError s!"HttpResponseBody.readAsTask failed: {err}")
    let mut receivedResponseBody := ByteArray.empty
    let mut responseDone := false
    if firstResponseChunk.size == 0 then
      responseDone := true
    else
      receivedResponseBody := ByteArray.append receivedResponseBody firstResponseChunk
    while !responseDone do
      let chunk ← (← response.body.readAsPromise (UInt32.ofNat 1) (UInt32.ofNat 64)).await
      if chunk.size == 0 then
        responseDone := true
      else
        receivedResponseBody := ByteArray.append receivedResponseBody chunk
    assertEqual receivedResponseBody expectedResponseBody
    response.body.release

    let drainPromise ← server.drainAsPromise
    drainPromise.await
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
def testKjAsyncHttpServerRoundtripHeaderDecodeStress : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  let mkHeader : String → Nat → Capnp.KjAsync.HttpHeader := fun namePrefix idx =>
    { name := s!"{namePrefix}-{idx}"
    , value := s!"{namePrefix}-value-{idx}-{String.ofList (List.replicate (idx + 4) 'v')}"
    }
  let requestHeaders : Array Capnp.KjAsync.HttpHeader := Id.run do
    let mut out : Array Capnp.KjAsync.HttpHeader := #[]
    for i in [:16] do
      out := out.push (mkHeader "x-req" i)
    pure out
  let responseHeaders : Array Capnp.KjAsync.HttpHeader := Id.run do
    let mut out : Array Capnp.KjAsync.HttpHeader := #[]
    for i in [:14] do
      out := out.push (mkHeader "x-resp" i)
    pure out
  try
    let server ← runtime.httpServerListen "127.0.0.1" 0
    let requestPath := s!"/lean-http-stress/{String.ofList (List.replicate 96 'p')}"
    let requestBody := ByteArray.append mkPayload ("-header-stress".toUTF8)
    let responsePromise ← runtime.httpRequestStartWithHeaders
      .post "127.0.0.1" requestPath requestHeaders requestBody server.boundPort

    let request ← waitForHttpServerRequest runtime server
    assertEqual request.path requestPath
    assertEqual request.body requestBody
    assertTrue (request.headers.size >= requestHeaders.size)
      "expected request header count to include at least all user-provided headers"
    for expected in requestHeaders do
      assertTrue (request.headers.any (fun h => h == expected))
        s!"expected request header {expected.name}"

    runtime.httpServerRespond server request.requestId (UInt32.ofNat 207) "Multi-Status"
      responseHeaders requestBody
    let response ← runtime.httpResponsePromiseAwaitWithHeaders responsePromise
    assertEqual response.status (UInt32.ofNat 207)
    assertEqual response.statusText "Multi-Status"
    assertEqual response.body requestBody
    assertTrue (response.headers.size >= responseHeaders.size)
      "expected response header count to include at least all user-provided headers"
    for expected in responseHeaders do
      assertTrue (response.headers.any (fun h => h == expected))
        s!"expected response header {expected.name}"

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
    let expectedRequestBody := ByteArray.append requestPartA requestPartB

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
        received := ByteArray.append received chunk
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
    let expectedRequestBody := ByteArray.append requestPartA requestPartB

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
    let expectedResponseBody := ByteArray.append responsePartA responsePartB
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
        received := ByteArray.append received chunk
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
def testKjAsyncRuntimeMismatchGuardForCorePrimitives : IO Unit := do
  let runtimeA ← Capnp.KjAsync.Runtime.init
  let runtimeB ← Capnp.KjAsync.Runtime.init
  try
    let promiseA ← runtimeA.sleepMillisStart (UInt32.ofNat 1)
    let promiseB ← runtimeB.sleepMillisStart (UInt32.ofNat 1)
    let promiseAllErr ←
      try
        let _ ← runtimeA.promiseAllStart #[promiseA, promiseB]
        pure ""
      catch e =>
        pure (toString e)
    assertTrue (promiseAllErr.contains "different Capnp.KjAsync runtime")
      "expected runtime mismatch guard for promiseAllStart"
    let promiseRaceErr ←
      try
        let _ ← runtimeA.promiseRaceStart #[promiseA, promiseB]
        pure ""
      catch e =>
        pure (toString e)
    assertTrue (promiseRaceErr.contains "different Capnp.KjAsync runtime")
      "expected runtime mismatch guard for promiseRaceStart"

    let taskSet ← runtimeA.taskSetNew
    let taskSetErr ←
      try
        runtimeA.taskSetAddPromise taskSet promiseB
        pure ""
      catch e =>
        pure (toString e)
    assertTrue (taskSetErr.contains "different Capnp.KjAsync runtime")
      "expected runtime mismatch guard for taskSetAddPromise"
    taskSet.release
    promiseA.release
    promiseB.release

    let (connA, connB) ← runtimeA.newTwoWayPipe
    let connErr ←
      try
        runtimeB.connectionWrite connA ByteArray.empty
        pure ""
      catch e =>
        pure (toString e)
    assertTrue (connErr.contains "different Capnp.KjAsync runtime")
      "expected runtime mismatch guard for connectionWrite"
    connA.release
    connB.release
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

@[test]
def testKjAsyncWebSocketEncodedHeaderApis : IO Unit := do
  let runtime ← Capnp.KjAsync.Runtime.init
  try
    let server ← runtime.httpServerListen "127.0.0.1" 0
    try
      let encodedHeaders := Capnp.KjAsync.encodeHttpHeaders
        #[{ name := "x-lean-ws-encoded", value := "1" }]

      let connectPromiseA ← runtime.webSocketConnectStartWithEncodedHeaders
        "127.0.0.1" "/lean-ws-encoded-start" encodedHeaders server.boundPort
      let requestA ← waitForHttpServerRequest runtime server
      assertEqual requestA.path "/lean-ws-encoded-start"
      assertTrue
        (requestA.headers.any (fun h => h.name == "x-lean-ws-encoded" && h.value == "1"))
        "expected x-lean-ws-encoded request header for encoded start helper"
      let serverWsA ← runtime.httpServerRespondWebSocket server requestA.requestId
      let clientWsA ← connectPromiseA.await

      (← clientWsA.sendTextStart "hello-encoded-start").await
      match (← serverWsA.receive) with
      | .text value =>
        assertEqual value "hello-encoded-start"
      | _ =>
        throw (IO.userError "expected websocket text message for encoded start helper")

      clientWsA.release
      serverWsA.release

      let connectPromiseB ← runtime.webSocketConnectStartWithEncodedHeaders
        "127.0.0.1" "/lean-ws-encoded-direct" encodedHeaders server.boundPort
      let requestB ← waitForHttpServerRequest runtime server
      assertEqual requestB.path "/lean-ws-encoded-direct"
      assertTrue
        (requestB.headers.any (fun h => h.name == "x-lean-ws-encoded" && h.value == "1"))
        "expected x-lean-ws-encoded request header for encoded direct helper"
      let serverWsB ← runtime.httpServerRespondWebSocket server requestB.requestId
      let clientWsB ← connectPromiseB.await

      (← serverWsB.sendTextStart "hello-encoded-direct").await
      match (← clientWsB.receive) with
      | .text value =>
        assertEqual value "hello-encoded-direct"
      | _ =>
        throw (IO.userError "expected websocket text message for encoded direct helper")

      clientWsB.release
      serverWsB.release
    finally
      server.release
  finally
    runtime.shutdown
