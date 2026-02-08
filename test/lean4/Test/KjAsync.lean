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
