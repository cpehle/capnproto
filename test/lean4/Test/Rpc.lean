import LeanTest
import Capnp.Runtime
import Capnp.Gen.c__.src.capnp.rpc

open LeanTest
open Capnp.Gen.c__.src.capnp.rpc

private def buildMessage (act : Capnp.BuilderM Unit) : Capnp.Message :=
  let init := Capnp.initMessageBuilder 8
  let (_, st) := Capnp.runBuilder act init
  Capnp.buildMessage st

private def buildReceiverAnswerCap : CapDescriptor.Reader :=
  let msg := buildMessage (do
    let cap ← CapDescriptor.initRoot
    CapDescriptor.Builder.setAttachedFd cap (UInt8.ofNat 3)
    let pa ← CapDescriptor.Builder.initReceiverAnswer cap
    PromisedAnswer.Builder.setQuestionId pa (UInt32.ofNat 77)
    let ops ← PromisedAnswer.Builder.initTransform pa 2
    match ops.toList with
    | op0 :: op1 :: _ => do
      PromisedAnswer.Op.Builder.setNoop op0
      PromisedAnswer.Op.Builder.setGetPointerField op1 (UInt16.ofNat 9)
    | _ => pure ())
  CapDescriptor.read (Capnp.getRoot msg)

@[test]
def testRpcPayloadRoundtrip : IO Unit := do
  let (idx, table) := Capnp.capTableAdd Capnp.emptyCapTable 7
  assertEqual idx (UInt32.ofNat 0)
  assertEqual (Capnp.capTableGet table idx) (some 7)
  let init := Capnp.initMessageBuilder 8
  let (_, st) := Capnp.runBuilder (do
    let payload ← Payload.initRoot
    Capnp.writeText (Capnp.getPointerBuilder payload.struct 0) "hello"
    let caps ← Payload.Builder.initCapTable payload 1
    match caps.toList with
    | cap :: _ => CapDescriptor.Builder.setNone cap
    | [] => pure ()
    ) init
  let msg := Capnp.buildMessage st
  let r := Payload.read (Capnp.getRoot msg)
  assertEqual (Capnp.readText r.getContent) "hello"
  let table := r.getCapTable
  assertEqual table.size 1
  match table.toList with
  | cap :: _ => assertEqual (cap.which == CapDescriptor.Which.none ()) true
  | [] => assertEqual true false

@[test]
def testRpcPayloadValueSetter : IO Unit := do
  let capVal : CapDescriptor :=
    { attachedFd := UInt8.ofNat 255
    , which := CapDescriptor.Which.senderHosted (UInt32.ofNat 321)
    }
  let msg := buildMessage (do
    let payload ← Payload.initRoot
    Capnp.writeText (Capnp.getPointerBuilder payload.struct 0) "value-setter"
    Payload.Builder.setCapTable payload #[capVal])
  let r := Payload.read (Capnp.getRoot msg)
  assertEqual (Capnp.readText r.getContent) "value-setter"
  match r.getCapTable.toList with
  | cap :: _ => do
    assertEqual cap.getAttachedFd (UInt8.ofNat 255)
    match cap.which with
    | CapDescriptor.Which.senderHosted v => assertEqual v (UInt32.ofNat 321)
    | _ => assertEqual true false
  | [] => assertEqual true false

@[test]
def testRpcPayloadReaderSetter : IO Unit := do
  let capReader := buildReceiverAnswerCap
  let msg := buildMessage (do
    let payload ← Payload.initRoot
    Payload.Builder.setCapTableFromReaders payload #[capReader])
  let r := Payload.read (Capnp.getRoot msg)
  match r.getCapTable.toList with
  | cap :: _ => do
    assertEqual cap.getAttachedFd (UInt8.ofNat 3)
    match cap.which with
    | CapDescriptor.Which.receiverAnswer pa => do
      assertEqual pa.getQuestionId (UInt32.ofNat 77)
      match pa.getTransform.toList with
      | op0 :: op1 :: _ => do
        assertEqual (op0.which == PromisedAnswer.Op.Which.noop ()) true
        match op1.which with
        | PromisedAnswer.Op.Which.getPointerField v => assertEqual v (UInt16.ofNat 9)
        | _ => assertEqual true false
      | _ => assertEqual true false
    | _ => assertEqual true false
  | [] => assertEqual true false

@[test]
def testRpcMessageSetFromValue : IO Unit := do
  let srcMsg := buildMessage (do
    let msg ← Message.initRoot
    let call ← Message.Builder.initCall msg
    Call.Builder.setQuestionId call (UInt32.ofNat 42)
    Call.Builder.setInterfaceId call (UInt64.ofNat 9001)
    Call.Builder.setMethodId call (UInt16.ofNat 7)
    let target ← Call.Builder.initTarget call
    MessageTarget.Builder.setImportedCap target (UInt32.ofNat 11))
  let src := Message.read (Capnp.getRoot srcMsg)
  let dstMsg := buildMessage (do
    let msg ← Message.initRoot
    Message.Builder.setFromValue msg src)
  let dst := Message.read (Capnp.getRoot dstMsg)
  match dst.which with
  | Message.Which.call call => do
    assertEqual call.getQuestionId (UInt32.ofNat 42)
    assertEqual call.getInterfaceId (UInt64.ofNat 9001)
    assertEqual call.getMethodId (UInt16.ofNat 7)
    match call.getTarget.which with
    | MessageTarget.Which.importedCap v => assertEqual v (UInt32.ofNat 11)
    | _ => assertEqual true false
  | _ => assertEqual true false

@[test]
def testRpcCallPromisedAnswerTarget : IO Unit := do
  let msg := buildMessage (do
    let m ← Message.initRoot
    let call ← Message.Builder.initCall m
    Call.Builder.setQuestionId call (UInt32.ofNat 8)
    Call.Builder.setInterfaceId call (UInt64.ofNat 999)
    Call.Builder.setMethodId call (UInt16.ofNat 3)
    let target ← Call.Builder.initTarget call
    let promised ← MessageTarget.Builder.initPromisedAnswer target
    PromisedAnswer.Builder.setQuestionId promised (UInt32.ofNat 41)
    let ops ← PromisedAnswer.Builder.initTransform promised 2
    match ops.toList with
    | op0 :: op1 :: _ => do
      PromisedAnswer.Op.Builder.setNoop op0
      PromisedAnswer.Op.Builder.setGetPointerField op1 (UInt16.ofNat 6)
    | _ => pure ())
  let r := Message.read (Capnp.getRoot msg)
  match r.which with
  | Message.Which.call call => do
    match call.getTarget.which with
    | MessageTarget.Which.promisedAnswer promised => do
      assertEqual promised.getQuestionId (UInt32.ofNat 41)
      match promised.getTransform.toList with
      | op0 :: op1 :: _ => do
        assertEqual (op0.which == PromisedAnswer.Op.Which.noop ()) true
        match op1.which with
        | PromisedAnswer.Op.Which.getPointerField idx =>
          assertEqual idx (UInt16.ofNat 6)
        | _ => assertEqual true false
      | _ => assertEqual true false
    | _ => assertEqual true false
  | _ => assertEqual true false

@[test]
def testRpcReturnTakeFromOtherQuestion : IO Unit := do
  let msg := buildMessage (do
    let m ← Message.initRoot
    let ret ← Message.Builder.init_return m
    Return.Builder.setAnswerId ret (UInt32.ofNat 100)
    Return.Builder.setTakeFromOtherQuestion ret (UInt32.ofNat 55))
  let r := Message.read (Capnp.getRoot msg)
  match r.which with
  | Message.Which._return ret => do
    assertEqual ret.getAnswerId (UInt32.ofNat 100)
    match ret.which with
    | Return.Which.takeFromOtherQuestion q => assertEqual q (UInt32.ofNat 55)
    | _ => assertEqual true false
  | _ => assertEqual true false

@[test]
def testRpcDisembargoSenderLoopback : IO Unit := do
  let msg := buildMessage (do
    let m ← Message.initRoot
    let dis ← Message.Builder.initDisembargo m
    let target ← Disembargo.Builder.initTarget dis
    MessageTarget.Builder.setImportedCap target (UInt32.ofNat 21)
    let context := Disembargo.Builder.getContext dis
    Disembargo.ContextGroup.Builder.setSenderLoopback context (UInt32.ofNat 77))
  let r := Message.read (Capnp.getRoot msg)
  match r.which with
  | Message.Which.disembargo dis => do
    match dis.getTarget.which with
    | MessageTarget.Which.importedCap capId => assertEqual capId (UInt32.ofNat 21)
    | _ => assertEqual true false
    match dis.getContext.which with
    | Disembargo.ContextGroup.Which.senderLoopback v =>
      assertEqual v (UInt32.ofNat 77)
    | _ => assertEqual true false
  | _ => assertEqual true false

@[test]
def testRpcFinishMessageRoundtrip : IO Unit := do
  let msg := buildMessage (do
    let m ← Message.initRoot
    let fin ← Message.Builder.initFinish m
    Finish.Builder.setQuestionId fin (UInt32.ofNat 17)
    Finish.Builder.setReleaseResultCaps fin true
    Finish.Builder.setRequireEarlyCancellationWorkaround fin true)
  let r := Message.read (Capnp.getRoot msg)
  match r.which with
  | Message.Which.finish fin => do
    assertEqual fin.getQuestionId (UInt32.ofNat 17)
    assertEqual fin.getReleaseResultCaps true
    assertEqual fin.getRequireEarlyCancellationWorkaround true
  | _ => assertEqual true false

@[test]
def testRpcResolveCapRoundtrip : IO Unit := do
  let msg := buildMessage (do
    let m ← Message.initRoot
    let resolve ← Message.Builder.initResolve m
    Resolve.Builder.setPromiseId resolve (UInt32.ofNat 12)
    let cap ← Resolve.Builder.initCap resolve
    CapDescriptor.Builder.setAttachedFd cap (UInt8.ofNat 5)
    CapDescriptor.Builder.setSenderHosted cap (UInt32.ofNat 88))
  let r := Message.read (Capnp.getRoot msg)
  match r.which with
  | Message.Which.resolve resolve => do
    assertEqual resolve.getPromiseId (UInt32.ofNat 12)
    match resolve.which with
    | Resolve.Which.cap cap => do
      assertEqual cap.getAttachedFd (UInt8.ofNat 5)
      match cap.which with
      | CapDescriptor.Which.senderHosted v => assertEqual v (UInt32.ofNat 88)
      | _ => assertEqual true false
    | _ => assertEqual true false
  | _ => assertEqual true false

@[test]
def testRpcResolveExceptionRoundtrip : IO Unit := do
  let msg := buildMessage (do
    let m ← Message.initRoot
    let resolve ← Message.Builder.initResolve m
    Resolve.Builder.setPromiseId resolve (UInt32.ofNat 44)
    let exc ← Resolve.Builder.initException resolve
    Exception.Builder.setReason exc "rpc failure"
    Exception.Builder.setType exc Exception.Type.disconnected)
  let r := Message.read (Capnp.getRoot msg)
  match r.which with
  | Message.Which.resolve resolve => do
    assertEqual resolve.getPromiseId (UInt32.ofNat 44)
    match resolve.which with
    | Resolve.Which.exception exc => do
      assertEqual exc.getReason "rpc failure"
      assertTrue (exc.getType == Exception.Type.disconnected)
        "resolve exception type mismatch"
    | _ => assertEqual true false
  | _ => assertEqual true false

@[test]
def testRpcReleaseMessageRoundtrip : IO Unit := do
  let msg := buildMessage (do
    let m ← Message.initRoot
    let rel ← Message.Builder.initRelease m
    Release.Builder.setId rel (UInt32.ofNat 9)
    Release.Builder.setReferenceCount rel (UInt32.ofNat 3))
  let r := Message.read (Capnp.getRoot msg)
  match r.which with
  | Message.Which.release rel => do
    assertEqual rel.getId (UInt32.ofNat 9)
    assertEqual rel.getReferenceCount (UInt32.ofNat 3)
  | _ => assertEqual true false

@[test]
def testRpcCallSendResultsToYourself : IO Unit := do
  let msg := buildMessage (do
    let m ← Message.initRoot
    let call ← Message.Builder.initCall m
    Call.Builder.setQuestionId call (UInt32.ofNat 2)
    Call.Builder.setInterfaceId call (UInt64.ofNat 7)
    Call.Builder.setMethodId call (UInt16.ofNat 1)
    let sendResultsTo := Call.Builder.getSendResultsTo call
    Call.SendResultsToGroup.Builder.setYourself sendResultsTo)
  let r := Message.read (Capnp.getRoot msg)
  match r.which with
  | Message.Which.call call => do
    match call.getSendResultsTo.which with
    | Call.SendResultsToGroup.Which.yourself _ => pure ()
    | _ => assertEqual true false
  | _ => assertEqual true false

@[test]
def testRpcReturnCanceled : IO Unit := do
  let msg := buildMessage (do
    let m ← Message.initRoot
    let ret ← Message.Builder.init_return m
    Return.Builder.setAnswerId ret (UInt32.ofNat 77)
    Return.Builder.setCanceled ret)
  let r := Message.read (Capnp.getRoot msg)
  match r.which with
  | Message.Which._return ret => do
    assertEqual ret.getAnswerId (UInt32.ofNat 77)
    match ret.which with
    | Return.Which.canceled _ => pure ()
    | _ => assertEqual true false
  | _ => assertEqual true false

@[test]
def testRpcReturnResultsSentElsewhere : IO Unit := do
  let msg := buildMessage (do
    let m ← Message.initRoot
    let ret ← Message.Builder.init_return m
    Return.Builder.setAnswerId ret (UInt32.ofNat 91)
    Return.Builder.setResultsSentElsewhere ret)
  let r := Message.read (Capnp.getRoot msg)
  match r.which with
  | Message.Which._return ret => do
    assertEqual ret.getAnswerId (UInt32.ofNat 91)
    match ret.which with
    | Return.Which.resultsSentElsewhere _ => pure ()
    | _ => assertEqual true false
  | _ => assertEqual true false

@[test]
def testRpcAdvancedDeferTaskHelper : IO Unit := do
  let task : Task (Except IO.Error Capnp.Rpc.AdvancedHandlerResult) :=
    Task.pure (.ok (Capnp.Rpc.Advanced.respond Capnp.emptyRpcEnvelope))
  let reply :=
    Capnp.Rpc.Advanced.deferTask task
      { releaseParams := true, allowCancellation := true }
  match reply with
  | .control opts next => do
      assertEqual opts.releaseParams true
      assertEqual opts.allowCancellation true
      match next with
      | .deferred deferredTask =>
          match (← IO.wait deferredTask) with
          | .ok (.respond payload) =>
              assertEqual payload.capTable.caps.size 0
          | _ =>
              throw (IO.userError "deferTask did not produce expected deferred respond payload")
      | _ =>
          throw (IO.userError "deferTask control wrapper did not contain deferred task")
  | _ =>
      throw (IO.userError "deferTask with control options did not emit control wrapper")

@[test]
def testRpcAdvancedDeferPromiseHelper : IO Unit := do
  let promise : Capnp.Async.Promise Capnp.Rpc.AdvancedHandlerResult :=
    Capnp.Async.Promise.ofTask
      (Task.pure (.ok (Capnp.Rpc.Advanced.respond Capnp.emptyRpcEnvelope)))
  let reply := Capnp.Rpc.Advanced.deferPromise promise
  match reply with
  | .deferred deferredTask =>
      match (← IO.wait deferredTask) with
      | .ok (.respond payload) =>
          assertEqual payload.capTable.caps.size 0
      | _ =>
          throw (IO.userError "deferPromise did not produce expected deferred respond payload")
  | _ =>
      throw (IO.userError "deferPromise did not emit deferred reply")
