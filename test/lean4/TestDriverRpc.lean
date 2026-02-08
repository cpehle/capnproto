/-
Copyright (c) 2026 The Cap'n Proto Authors.
Released under the MIT License.

Fast test driver for Lean4 backend work. This excludes conformance tests, which
pull in large generated modules and are run via the full test driver.
-/

import LeanTest
import Lean.Util.Path
import Test.Runtime
import Test.Generated
import Test.Builder
import Test.Capability
import Test.Packed
import Test.Checked
import Test.KjAsync
import Test.Rpc
import Test.RpcClient

/-- Parse command line arguments into a RunConfig. -/
def parseArgs (args : List String) : IO LeanTest.RunConfig := do
  let mut config : LeanTest.RunConfig := {}
  let mut remaining := args
  while _h : !remaining.isEmpty do
    match remaining with
    | "--filter" :: pattern :: rest =>
      config := { config with filter := some pattern }
      remaining := rest
    | "--ignored" :: rest =>
      config := { config with includeIgnored := true }
      remaining := rest
    | "--fail-fast" :: rest =>
      config := { config with failFast := true }
      remaining := rest
    | "--help" :: _ =>
      IO.println "Usage: lake test [OPTIONS]"
      IO.println ""
      IO.println "Options:"
      IO.println "  --filter PATTERN  Only run tests matching PATTERN"
      IO.println "  --ignored         Include tests marked as ignored"
      IO.println "  --fail-fast       Stop on first failure"
      IO.println "  --help            Show this help"
      IO.Process.exit 0
    | _ :: rest =>
      remaining := rest
    | [] =>
      remaining := []
  return config

/-- Main entry point for the fast test driver. -/
unsafe def main (args : List String) : IO UInt32 := do
  let config ← parseArgs args

  Lean.initSearchPath (← Lean.findSysroot)
  Lean.enableInitializersExecution

  let env ← Lean.importModules
    #[{ module := `LeanTest }, { module := `Test.Runtime }, { module := `Test.Generated },
      { module := `Test.Builder }, { module := `Test.Capability }, { module := `Test.Packed },
      { module := `Test.Checked }, { module := `Test.KjAsync }, { module := `Test.Rpc },
      { module := `Test.RpcClient }]
    {}

  LeanTest.runTestsAndExit env {} config
