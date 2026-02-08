import Lake
open System
open Lake DSL

def capnpBridgeLinkArgs : Array String :=
  if System.Platform.isOSX then
    #[
      "-L../../build-lean4-apple/c++/src/capnp",
      "-L../../build-lean4-apple/c++/src/kj",
      "-L../../build-lean4/c++/src/capnp",
      "-L../../build-lean4/c++/src/kj",
      "-lcapnp-rpc", "-lcapnp", "-lkj-async", "-lkj", "-lc++"
    ]
  else
    #[
      "-L../../build-lean4/c++/src/capnp",
      "-L../../build-lean4/c++/src/kj",
      "-L../../build-lean4-apple/c++/src/capnp",
      "-L../../build-lean4-apple/c++/src/kj",
      "-lcapnp-rpc", "-lcapnp", "-lkj-async", "-lkj", "-lstdc++", "-pthread"
    ]

def capnpBridgeCompileArgs : Array String :=
  if System.Platform.isOSX then
    #["-fPIC", "-std=c++23"]
  else
    #["-fPIC", "-std=c++23", "-pthread"]

package capnp_lean4_test where
  moreLeanArgs := #["-DmaxHeartbeats=2000000"]
  moreLinkArgs := capnpBridgeLinkArgs

require LeanTest from "LeanTest"

target rpc_bridge.o pkg : FilePath := do
  let srcJob ← inputTextFile <| pkg.dir / "c" / "rpc_bridge.cpp"
  let oFile := pkg.buildDir / "c" / "rpc_bridge.o"
  let weakArgs := #[
    "-I", (← getLeanIncludeDir).toString,
    "-I", (pkg.dir / ".." / ".." / "c++" / "src").toString
  ]
  buildO oFile srcJob weakArgs capnpBridgeCompileArgs "c++" getLeanTrace

target kj_async_bridge.o pkg : FilePath := do
  let srcJob ← inputTextFile <| pkg.dir / "c" / "kj_async_bridge.cpp"
  let oFile := pkg.buildDir / "c" / "kj_async_bridge.o"
  let weakArgs := #[
    "-I", (← getLeanIncludeDir).toString,
    "-I", (pkg.dir / ".." / ".." / "c++" / "src").toString
  ]
  buildO oFile srcJob weakArgs capnpBridgeCompileArgs "c++" getLeanTrace

target libleanrpcbridge pkg : FilePath := do
  let bridgeO ← rpc_bridge.o.fetch
  let kjAsyncBridgeO ← kj_async_bridge.o.fetch
  let name := nameToStaticLib "leanrpcbridge"
  buildStaticLib (pkg.staticLibDir / name) #[bridgeO, kjAsyncBridgeO]

lean_lib CapnpRuntime where
  srcDir := "../../lean"
  roots := #[`Capnp.Runtime]
  globs := #[.submodules `Capnp]
  moreLinkObjs := #[libleanrpcbridge]

lean_lib CapnpGen where
  srcDir := "out"
  roots := #[`Capnp.Gen]
  globs := #[.submodules `Capnp.Gen]

lean_lib CapnpTest where
  srcDir := "src"
  roots := #[`Main]

lean_lib CapnpLeanTests where
  roots := #[`Test]
  globs := #[.submodules `Test]

lean_exe test_full where
  root := `TestDriver
  supportInterpreter := true

@[test_driver]
lean_exe test where
  root := `TestDriverRpc
  supportInterpreter := true
