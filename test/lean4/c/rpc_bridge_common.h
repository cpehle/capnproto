#pragma once

#include <lean/lean.h>
#include <kj/common.h>
#include <kj/exception.h>
#include <vector>
#include <string>
#include <atomic>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>

namespace capnp_lean_rpc {

inline constexpr uint32_t kRuntimeDefaultMaxFdsPerMessage = 16;

inline bool debugEnabled() {
  static bool enabled = []() {
    const char* flag = std::getenv("CAPNP_LEAN_RPC_DEBUG");
    return flag != nullptr && flag[0] != '\0';
  }();
  return enabled;
}

inline void debugLog(const char* label, const std::string& message) {
  if (!debugEnabled()) return;
  std::fprintf(stderr, "[capnp-lean-rpc] %s: %s\n", label, message.c_str());
  std::fflush(stderr);
}

// Lean object helpers
lean_obj_res mkByteArrayCopy(const uint8_t* data, size_t size);
lean_obj_res mkIoUserError(const std::string& message);
void mkIoOkUnit(lean_obj_res& out);
inline lean_obj_res mkIoOkUnit() { lean_obj_res out; mkIoOkUnit(out); return out; }

// KJ Exception helpers
const char* kjExceptionTypeName(kj::Exception::Type type);
std::string describeKjException(const kj::Exception& e);

// Serialization helpers
uint32_t readUint32Le(const uint8_t* data);
uint16_t readUint16Le(const uint8_t* data);
uint64_t readUint64Le(const uint8_t* data);
void appendUint32Le(std::vector<uint8_t>& out, uint32_t value);
void appendUint64Le(std::vector<uint8_t>& out, uint64_t value);

// CapTable and PipelineOps decoding
std::vector<uint32_t> decodeCapTable(const uint8_t* data, size_t size);
std::vector<uint32_t> decodeCapTable(b_lean_obj_arg bytes);
std::vector<uint16_t> decodePipelineOps(const uint8_t* data, size_t size);
std::vector<uint16_t> decodePipelineOps(b_lean_obj_arg bytes);
std::vector<uint8_t> copyByteArray(b_lean_obj_arg bytes);

// Deferred Task structures
struct DeferredLeanTask {
  explicit DeferredLeanTask(lean_object* task): task(task) {}
  ~DeferredLeanTask() { lean_dec(task); }
  lean_object* task;
};

struct DeferredLeanTaskState {
  DeferredLeanTaskState(kj::Own<DeferredLeanTask>&& waitTask,
                        kj::Own<DeferredLeanTask>&& cancelTask,
                        bool allowCancellation);
  ~DeferredLeanTaskState();

  bool requestCancellation();

  kj::Own<DeferredLeanTask> waitTask;
  kj::Own<DeferredLeanTask> cancelTask;
  bool allowCancellation;
  std::atomic<bool> completed = false;
  std::atomic<bool> cancellationRequested = false;
};

// Advanced handler structures
struct LeanAdvancedHandlerAction {
  enum class Kind : uint8_t {
    RETURN_PAYLOAD = 0,
    ASYNC_CALL = 1,
    TAIL_CALL = 2,
    THROW_REMOTE = 3,
    AWAIT_TASK = 4
  };

  Kind kind = Kind::RETURN_PAYLOAD;
  bool releaseParams = false;
  bool allowCancellation = false;
  bool isStreaming = false;
  bool sendResultsToCaller = false;
  bool noPromisePipelining = false;
  bool onlyPromisePipeline = false;
  bool hasPipeline = false;
  uint32_t target = 0;
  uint64_t interfaceId = 0;
  uint16_t methodId = 0;
  std::vector<uint8_t> pipelineBytes;
  std::vector<uint8_t> pipelineCaps;
  std::vector<uint8_t> payloadBytes;
  std::vector<uint8_t> payloadCaps;
  std::string message;
  kj::Exception::Type remoteExceptionType = kj::Exception::Type::FAILED;
  std::vector<uint8_t> detailBytes;
  kj::Own<DeferredLeanTask> deferredWaitTask;
  kj::Own<DeferredLeanTask> deferredCancelTask;
};

// Completion structures used for synchronization between worker and Lean threads
struct RawCallResult {
  std::vector<uint8_t> response;
  std::vector<uint8_t> responseCaps;
};

struct RawCallCompletion {
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  bool ok = false;
  std::string error;
  uint8_t exceptionTypeTag = 0;
  std::string exceptionDescription;
  std::string remoteTrace;
  std::vector<uint8_t> detailBytes;
  RawCallResult result;
};

struct RegisterTargetCompletion {
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  bool ok = false;
  std::string error;
  uint32_t targetId = 0;
};

struct UnitCompletion {
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  bool ok = false;
  std::string error;
};

struct UInt64Completion {
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  bool ok = false;
  std::string error;
  uint64_t value = 0;
};

struct BoolCompletion {
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  bool ok = false;
  std::string error;
  bool value = false;
};

struct OptionalStringCompletion {
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  bool ok = false;
  std::string error;
  bool hasValue = false;
  std::string value;
};

struct Int64Completion {
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  bool ok = false;
  std::string error;
  int64_t value = 0;
};

struct RegisterPairCompletion {
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  bool ok = false;
  std::string error;
  uint32_t first = 0;
  uint32_t second = 0;
};

struct KjPromiseIdCompletion {
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  bool ok = false;
  std::string error;
  uint32_t promiseId = 0;
};

class RuntimeLoop;

// Shared runtime id generator used by both the RPC and KjAsync test runtimes to avoid collisions
// when a single process creates both kinds of runtimes.
extern std::atomic<uint64_t> gNextRuntimeId;

void completeSuccess(const std::shared_ptr<RawCallCompletion>& completion, RawCallResult result);
void completeFailure(const std::shared_ptr<RawCallCompletion>& completion, std::string message);
void completeFailureKj(const std::shared_ptr<RawCallCompletion>& completion, const kj::Exception& e);
void completeRegisterSuccess(const std::shared_ptr<RegisterTargetCompletion>& completion, uint32_t targetId);
void completeRegisterFailure(const std::shared_ptr<RegisterTargetCompletion>& completion, std::string message);
void completeUnitSuccess(const std::shared_ptr<UnitCompletion>& completion);
void completeUnitFailure(const std::shared_ptr<UnitCompletion>& completion, std::string message);
void completeUInt64Success(const std::shared_ptr<UInt64Completion>& completion, uint64_t value);
void completeUInt64Failure(const std::shared_ptr<UInt64Completion>& completion, std::string message);
void completeBoolSuccess(const std::shared_ptr<BoolCompletion>& completion, bool value);
void completeBoolFailure(const std::shared_ptr<BoolCompletion>& completion, std::string message);
void completeOptionalStringSuccess(const std::shared_ptr<OptionalStringCompletion>& completion,
                                   kj::Maybe<std::string> value);
void completeOptionalStringFailure(const std::shared_ptr<OptionalStringCompletion>& completion,
                                   std::string message);
void completeInt64Success(const std::shared_ptr<Int64Completion>& completion, int64_t value);
void completeInt64Failure(const std::shared_ptr<Int64Completion>& completion, std::string message);
void completeRegisterPairSuccess(const std::shared_ptr<RegisterPairCompletion>& completion, uint32_t first, uint32_t second);
void completeRegisterPairFailure(const std::shared_ptr<RegisterPairCompletion>& completion, std::string message);
void completeKjPromiseIdSuccess(const std::shared_ptr<KjPromiseIdCompletion>& completion, uint32_t promiseId);
void completeKjPromiseIdFailure(const std::shared_ptr<KjPromiseIdCompletion>& completion, std::string message);

} // namespace capnp_lean_rpc
