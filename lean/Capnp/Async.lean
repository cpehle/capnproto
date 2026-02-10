import Init
import Init.System.Promise

namespace Capnp
namespace Async

class Awaitable (promise : Type u) (α : Type) where
  await : promise -> IO α

class Cancelable (promise : Type u) where
  cancel : promise -> IO Unit

class Releasable (promise : Type u) where
  release : promise -> IO Unit

@[inline] def await {ρ : Type u} {α : Type} [inst : Awaitable ρ α] (ref : ρ) : IO α :=
  inst.await ref

@[inline] def cancel {ρ : Type u} [inst : Cancelable ρ] (ref : ρ) : IO Unit :=
  inst.cancel ref

@[inline] def release {ρ : Type u} [inst : Releasable ρ] (ref : ρ) : IO Unit :=
  inst.release ref

@[inline] def awaitAsTask {ρ : Type u} {α : Type} [Awaitable ρ α] (ref : ρ) :
    IO (Task (Except IO.Error α)) :=
  IO.asTask (await ref)

def toIOPromise {ρ : Type u} {α : Type} [Awaitable ρ α]
    (ref : ρ) : IO (IO.Promise (Except String α)) := do
  let out ← IO.Promise.new
  let _task ← IO.asTask do
    let result ←
      try
        let value ← await ref
        pure (Except.ok value)
      catch e =>
        pure (Except.error e.toString)
    out.resolve result
  pure out

end Async
end Capnp
