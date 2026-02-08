import LeanTest
import Capnp.KjAsync

open LeanTest

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
