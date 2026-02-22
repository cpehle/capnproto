cmake_minimum_required(VERSION 3.16)

foreach(var CAPNP_EXECUTABLE CAPNPC_LEAN4_EXECUTABLE LAKE_EXECUTABLE SOURCE_ROOT)
  if(NOT DEFINED ${var} OR "${${var}}" STREQUAL "")
    message(FATAL_ERROR "Missing required variable: ${var}")
  endif()
endforeach()

set(TEST_ROOT "${SOURCE_ROOT}/test/lean4")
set(OUT_ROOT "${TEST_ROOT}/out")
set(GOLDEN_EXPECTED "${TEST_ROOT}/expected/Capnp/Gen/test/lean4/addressbook.lean")
set(GOLDEN_ACTUAL "${OUT_ROOT}/Capnp/Gen/test/lean4/addressbook.lean")

set(SCHEMAS
  "${SOURCE_ROOT}/test/lean4/addressbook.capnp"
  "${SOURCE_ROOT}/test/lean4/fixtures/defaults.capnp"
  "${SOURCE_ROOT}/test/lean4/fixtures/capability.capnp"
  "${SOURCE_ROOT}/test/lean4/fixtures/rpc_echo.capnp"
  "${SOURCE_ROOT}/c++/src/capnp/test.capnp"
  "${SOURCE_ROOT}/c++/src/capnp/rpc.capnp"
  "${SOURCE_ROOT}/c++/src/capnp/rpc-twoparty.capnp"
  "${SOURCE_ROOT}/c++/src/capnp/stream.capnp"
)

message(STATUS "Regenerating Lean4 schema output for test suite")
execute_process(
  COMMAND "${CMAKE_COMMAND}" -E rm -rf "${OUT_ROOT}/Capnp/Gen"
  RESULT_VARIABLE clean_result
)
if(NOT clean_result EQUAL 0)
  message(FATAL_ERROR "Failed to clean ${OUT_ROOT}/Capnp/Gen")
endif()

execute_process(
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${OUT_ROOT}"
  RESULT_VARIABLE mkdir_result
)
if(NOT mkdir_result EQUAL 0)
  message(FATAL_ERROR "Failed to create ${OUT_ROOT}")
endif()

execute_process(
  COMMAND "${CAPNP_EXECUTABLE}" compile
    -o "${CAPNPC_LEAN4_EXECUTABLE}:${OUT_ROOT}"
    --src-prefix "${SOURCE_ROOT}"
    -I "${SOURCE_ROOT}/c++/src"
    -I "${SOURCE_ROOT}/test/lean4"
    ${SCHEMAS}
  WORKING_DIRECTORY "${SOURCE_ROOT}"
  RESULT_VARIABLE compile_result
  OUTPUT_VARIABLE compile_stdout
  ERROR_VARIABLE compile_stderr
)
if(NOT compile_result EQUAL 0)
  message(FATAL_ERROR
    "Lean4 code generation failed with exit code ${compile_result}\n"
    "stdout:\n${compile_stdout}\n"
    "stderr:\n${compile_stderr}")
endif()

message(STATUS "Checking Lean4 golden output")
execute_process(
  COMMAND "${CMAKE_COMMAND}" -E compare_files "${GOLDEN_EXPECTED}" "${GOLDEN_ACTUAL}"
  RESULT_VARIABLE compare_result
)
if(NOT compare_result EQUAL 0)
  message(FATAL_ERROR
    "Golden output mismatch for addressbook.\n"
    "Expected: ${GOLDEN_EXPECTED}\n"
    "Actual:   ${GOLDEN_ACTUAL}\n"
    "Regenerate and inspect the diff in test/lean4/expected and test/lean4/out.")
endif()

find_program(PYTHON3_EXECUTABLE NAMES python3 python)
if(NOT PYTHON3_EXECUTABLE)
  message(FATAL_ERROR "Unable to find a Python interpreter for parity matrix validation")
endif()

message(STATUS "Validating Lean4 RPC parity matrix")
execute_process(
  COMMAND "${PYTHON3_EXECUTABLE}" "${TEST_ROOT}/scripts/validate_parity_matrix.py"
  WORKING_DIRECTORY "${SOURCE_ROOT}"
  RESULT_VARIABLE parity_result
  OUTPUT_VARIABLE parity_stdout
  ERROR_VARIABLE parity_stderr
)
if(NOT parity_result EQUAL 0)
  message(FATAL_ERROR
    "Parity matrix validation failed with exit code ${parity_result}\n"
    "stdout:\n${parity_stdout}\n"
    "stderr:\n${parity_stderr}")
endif()

message(STATUS "Running Lean4 tests via lake")
execute_process(
  COMMAND "${LAKE_EXECUTABLE}" test
  WORKING_DIRECTORY "${TEST_ROOT}"
  RESULT_VARIABLE lake_result
  OUTPUT_VARIABLE lake_stdout
  ERROR_VARIABLE lake_stderr
)
if(NOT lake_result EQUAL 0)
  message(FATAL_ERROR
    "lake test failed with exit code ${lake_result}\n"
    "stdout:\n${lake_stdout}\n"
    "stderr:\n${lake_stderr}")
endif()
