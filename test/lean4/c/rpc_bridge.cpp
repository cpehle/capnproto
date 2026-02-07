#include <lean/lean.h>

#include <capnp/any.h>
#include <capnp/capability.h>
#include <capnp/rpc-twoparty.h>
#include <capnp/serialize.h>
#include <kj/async.h>
#include <kj/async-io.h>
#include <kj/io.h>
#include <kj/vector.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <variant>
#include <vector>

namespace {

lean_obj_res mkByteArrayCopy(const uint8_t* data, size_t size) {
  lean_object* out = lean_alloc_sarray(1, size, size);
  if (size != 0) {
    std::memcpy(lean_sarray_cptr(out), data, size);
  }
  lean_sarray_set_size(out, size);
  return out;
}

lean_obj_res mkIoUserError(const std::string& message) {
  lean_object* msg = lean_mk_string(message.c_str());
  lean_object* err = lean_mk_io_user_error(msg);
  return lean_io_result_mk_error(err);
}

void mkIoOkUnit(lean_obj_res& out) { out = lean_io_result_mk_ok(lean_box(0)); }

std::string describeKjException(const kj::Exception& e) {
  return std::string(e.getDescription().cStr());
}

uint32_t readUint32Le(const uint8_t* data) {
  return static_cast<uint32_t>(data[0]) |
         (static_cast<uint32_t>(data[1]) << 8) |
         (static_cast<uint32_t>(data[2]) << 16) |
         (static_cast<uint32_t>(data[3]) << 24);
}

void appendUint32Le(std::vector<uint8_t>& out, uint32_t value) {
  out.push_back(static_cast<uint8_t>(value & 0xff));
  out.push_back(static_cast<uint8_t>((value >> 8) & 0xff));
  out.push_back(static_cast<uint8_t>((value >> 16) & 0xff));
  out.push_back(static_cast<uint8_t>((value >> 24) & 0xff));
}

std::vector<uint32_t> decodeCapTable(const uint8_t* data, size_t size) {
  if ((size % 4) != 0) {
    throw std::runtime_error("RPC capability table payload must be a multiple of 4 bytes");
  }
  std::vector<uint32_t> caps;
  caps.reserve(size / 4);
  for (size_t i = 0; i < size; i += 4) {
    caps.push_back(readUint32Le(data + i));
  }
  return caps;
}

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

void completeSuccess(const std::shared_ptr<RawCallCompletion>& completion, RawCallResult result) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = true;
    completion->result = std::move(result);
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeFailure(const std::shared_ptr<RawCallCompletion>& completion, std::string message) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = false;
    completion->error = std::move(message);
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeRegisterSuccess(const std::shared_ptr<RegisterTargetCompletion>& completion,
                             uint32_t targetId) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = true;
    completion->targetId = targetId;
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeRegisterFailure(const std::shared_ptr<RegisterTargetCompletion>& completion,
                             std::string message) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = false;
    completion->error = std::move(message);
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeUnitSuccess(const std::shared_ptr<UnitCompletion>& completion) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = true;
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeUnitFailure(const std::shared_ptr<UnitCompletion>& completion, std::string message) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = false;
    completion->error = std::move(message);
    completion->done = true;
  }
  completion->cv.notify_one();
}

class EchoCapabilityServer final : public capnp::Capability::Server {
 public:
  DispatchCallResult dispatchCall(uint64_t interfaceId, uint16_t methodId,
                                  capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer>
                                      context) override {
    (void)interfaceId;
    (void)methodId;

    context.getResults().setAs<capnp::AnyPointer>(context.getParams().getAs<capnp::AnyPointer>());
    return {kj::READY_NOW, false};
  }
};

class RuntimeLoop {
 public:
  RuntimeLoop() : worker_(&RuntimeLoop::run, this) {
    std::unique_lock<std::mutex> lock(startupMutex_);
    startupCv_.wait(lock, [this]() { return startupComplete_; });
    if (!startupError_.empty()) {
      throw std::runtime_error(startupError_);
    }
  }

  ~RuntimeLoop() { shutdown(); }

  std::shared_ptr<RawCallCompletion> enqueueRawCall(
      uint32_t target, uint64_t interfaceId, uint16_t methodId, std::vector<uint8_t> request,
      std::vector<uint32_t> requestCaps) {
    auto completion = std::make_shared<RawCallCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedRawCall{
          target, interfaceId, methodId, std::move(request), std::move(requestCaps), completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueRegisterEchoTarget() {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedRegisterEchoTarget{completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueReleaseTarget(uint32_t target) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedReleaseTarget{target, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  void shutdown() {
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        return;
      }
      stopping_ = true;
    }
    queueCv_.notify_one();
    if (worker_.joinable()) {
      worker_.join();
    }
  }

 private:
  struct QueuedRawCall {
    uint32_t target;
    uint64_t interfaceId;
    uint16_t methodId;
    std::vector<uint8_t> request;
    std::vector<uint32_t> requestCaps;
    std::shared_ptr<RawCallCompletion> completion;
  };

  struct QueuedRegisterEchoTarget {
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedReleaseTarget {
    uint32_t target;
    std::shared_ptr<UnitCompletion> completion;
  };

  using QueuedOperation =
      std::variant<QueuedRawCall, QueuedRegisterEchoTarget, QueuedReleaseTarget>;

  struct LoopbackEchoPeer {
    kj::Own<kj::AsyncCapabilityStream> clientStream;
    kj::Own<capnp::TwoPartyServer> server;
    kj::Own<capnp::TwoPartyClient> client;
  };

  uint32_t addTarget(capnp::Capability::Client cap) {
    uint32_t targetId = nextTargetId_++;
    while (targets_.find(targetId) != targets_.end()) {
      targetId = nextTargetId_++;
    }
    targets_.emplace(targetId, kj::mv(cap));
    return targetId;
  }

  RawCallResult processRawCall(uint32_t target, uint64_t interfaceId, uint16_t methodId,
                               const std::vector<uint8_t>& request,
                               const std::vector<uint32_t>& requestCaps,
                               kj::WaitScope& waitScope) {
    auto targetIt = targets_.find(target);
    if (targetIt == targets_.end()) {
      throw std::runtime_error("unknown RPC target capability id: " + std::to_string(target));
    }

    auto requestBuilder = targetIt->second.typelessRequest(interfaceId, methodId, kj::none, {});
    if (!request.empty()) {
      kj::ArrayPtr<const kj::byte> reqBytes(reinterpret_cast<const kj::byte*>(request.data()),
                                            request.size());
      kj::ArrayInputStream input(reqBytes);
      capnp::ReaderOptions options;
      options.traversalLimitInWords = 1ull << 30;
      capnp::InputStreamMessageReader reader(input, options);
      auto requestRoot = reader.getRoot<capnp::AnyPointer>();
      if (requestCaps.empty()) {
        requestBuilder.setAs<capnp::AnyPointer>(requestRoot);
      } else {
        auto capTableBuilder =
            kj::heapArrayBuilder<kj::Maybe<kj::Own<capnp::ClientHook>>>(requestCaps.size());
        for (auto capId : requestCaps) {
          if (capId == 0) {
            capTableBuilder.add(kj::none);
            continue;
          }
          auto capIt = targets_.find(capId);
          if (capIt == targets_.end()) {
            throw std::runtime_error("unknown RPC request capability id: " +
                                     std::to_string(capId));
          }
          capnp::Capability::Client cap = capIt->second;
          capTableBuilder.add(capnp::ClientHook::from(kj::mv(cap)));
        }
        capnp::ReaderCapabilityTable requestCapTable(capTableBuilder.finish());
        requestBuilder.setAs<capnp::AnyPointer>(requestCapTable.imbue(requestRoot));
      }
    }

    auto response = requestBuilder.send().wait(waitScope);
    capnp::MallocMessageBuilder responseMessage;
    capnp::BuilderCapabilityTable responseCapTable;
    responseCapTable
        .imbue(responseMessage.getRoot<capnp::AnyPointer>())
        .setAs<capnp::AnyPointer>(response.getAs<capnp::AnyPointer>());

    auto responseWords = capnp::messageToFlatArray(responseMessage);
    auto responseBytes = responseWords.asBytes();
    std::vector<uint8_t> responseCopy(responseBytes.begin(), responseBytes.end());

    std::vector<uint8_t> responseCaps;
    auto responseCapTableEntries = responseCapTable.getTable();
    responseCaps.reserve(responseCapTableEntries.size() * 4);
    for (auto& maybeHook : responseCapTableEntries) {
      KJ_IF_SOME(hook, maybeHook) {
        auto cap = capnp::Capability::Client(hook->addRef());
        appendUint32Le(responseCaps, addTarget(kj::mv(cap)));
      } else {
        appendUint32Le(responseCaps, 0);
      }
    }

    return RawCallResult{std::move(responseCopy), std::move(responseCaps)};
  }

  void releaseTarget(uint32_t target) {
    auto erased = targets_.erase(target);
    loopbackEchoPeers_.erase(target);
    if (erased == 0) {
      throw std::runtime_error("unknown RPC target capability id: " + std::to_string(target));
    }
  }

  uint32_t registerEchoTarget(kj::AsyncIoProvider& ioProvider) {
    auto pipe = ioProvider.newCapabilityPipe();
    auto server = kj::heap<capnp::TwoPartyServer>(
        capnp::Capability::Client(kj::heap<EchoCapabilityServer>()));
    server->accept(kj::mv(pipe.ends[0]), 2);

    auto client = kj::heap<capnp::TwoPartyClient>(*pipe.ends[1], 2);
    auto cap = client->bootstrap();

    auto peer = kj::heap<LoopbackEchoPeer>();
    peer->clientStream = kj::mv(pipe.ends[1]);
    peer->server = kj::mv(server);
    peer->client = kj::mv(client);

    auto targetId = addTarget(kj::mv(cap));
    loopbackEchoPeers_.emplace(targetId, kj::mv(peer));
    return targetId;
  }

  void run() {
    try {
      auto io = kj::setupAsyncIo();
      {
        std::lock_guard<std::mutex> lock(startupMutex_);
        startupComplete_ = true;
      }
      startupCv_.notify_all();

      while (true) {
        QueuedOperation op;
        {
          std::unique_lock<std::mutex> lock(queueMutex_);
          queueCv_.wait(lock, [this]() { return stopping_ || !queue_.empty(); });
          if (stopping_ && queue_.empty()) {
            break;
          }
          op = std::move(queue_.front());
          queue_.pop_front();
        }

        if (std::holds_alternative<QueuedRawCall>(op)) {
          auto call = std::get<QueuedRawCall>(std::move(op));
          try {
            auto promise = kj::evalNow([&]() {
              return processRawCall(call.target, call.interfaceId, call.methodId, call.request,
                                    call.requestCaps, io.waitScope);
            });
            completeSuccess(call.completion, promise.wait(io.waitScope));
          } catch (const kj::Exception& e) {
            completeFailure(call.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeFailure(call.completion, e.what());
          } catch (...) {
            completeFailure(call.completion, "unknown exception in capnp_lean_rpc_raw_call");
          }
        } else if (std::holds_alternative<QueuedRegisterEchoTarget>(op)) {
          auto registration = std::get<QueuedRegisterEchoTarget>(std::move(op));
          try {
            completeRegisterSuccess(registration.completion, registerEchoTarget(*io.provider));
          } catch (const kj::Exception& e) {
            completeRegisterFailure(registration.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(registration.completion, e.what());
          } catch (...) {
            completeRegisterFailure(registration.completion,
                                    "unknown exception in capnp_lean_rpc_runtime_register_echo_target");
          }
        } else {
          auto release = std::get<QueuedReleaseTarget>(std::move(op));
          try {
            releaseTarget(release.target);
            completeUnitSuccess(release.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(release.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(release.completion, e.what());
          } catch (...) {
            completeUnitFailure(release.completion,
                                "unknown exception in capnp_lean_rpc_runtime_release_target");
          }
        }
      }

      // Tear down RPC clients/servers on the runtime thread while async I/O is still valid.
      targets_.clear();
      loopbackEchoPeers_.clear();

      failPendingCalls("Capnp.Rpc runtime shut down");
    } catch (const kj::Exception& e) {
      reportStartupFailure(describeKjException(e));
      failPendingCalls(describeKjException(e));
    } catch (const std::exception& e) {
      reportStartupFailure(e.what());
      failPendingCalls(e.what());
    } catch (...) {
      reportStartupFailure("unknown exception while starting Capnp.Rpc runtime");
      failPendingCalls("unknown exception while starting Capnp.Rpc runtime");
    }
  }

  void reportStartupFailure(std::string message) {
    {
      std::lock_guard<std::mutex> lock(startupMutex_);
      if (startupComplete_) {
        return;
      }
      startupError_ = std::move(message);
      startupComplete_ = true;
    }
    startupCv_.notify_all();
  }

  void failPendingCalls(const std::string& message) {
    std::deque<QueuedOperation> pending;
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      pending.swap(queue_);
    }
    for (auto& op : pending) {
      if (std::holds_alternative<QueuedRawCall>(op)) {
        completeFailure(std::get<QueuedRawCall>(op).completion, message);
      } else if (std::holds_alternative<QueuedRegisterEchoTarget>(op)) {
        completeRegisterFailure(std::get<QueuedRegisterEchoTarget>(op).completion, message);
      } else {
        completeUnitFailure(std::get<QueuedReleaseTarget>(op).completion, message);
      }
    }
  }

  std::thread worker_;

  std::mutex startupMutex_;
  std::condition_variable startupCv_;
  bool startupComplete_ = false;
  std::string startupError_;

  std::mutex queueMutex_;
  std::condition_variable queueCv_;
  bool stopping_ = false;
  std::deque<QueuedOperation> queue_;

  std::unordered_map<uint32_t, capnp::Capability::Client> targets_;
  std::unordered_map<uint32_t, kj::Own<LoopbackEchoPeer>> loopbackEchoPeers_;
  uint32_t nextTargetId_ = 1;
};

std::mutex gRuntimeRegistryMutex;
std::unordered_map<uint64_t, std::shared_ptr<RuntimeLoop>> gRuntimes;
std::atomic<uint64_t> gNextRuntimeId{1};

uint64_t allocateRuntimeIdLocked() {
  while (true) {
    uint64_t id = gNextRuntimeId.fetch_add(1, std::memory_order_relaxed);
    if (id == 0) {
      continue;
    }
    if (gRuntimes.find(id) == gRuntimes.end()) {
      return id;
    }
  }
}

std::shared_ptr<RuntimeLoop> getRuntime(uint64_t runtimeId) {
  std::lock_guard<std::mutex> lock(gRuntimeRegistryMutex);
  auto it = gRuntimes.find(runtimeId);
  if (it == gRuntimes.end()) {
    return nullptr;
  }
  return it->second;
}

bool isRuntimeAlive(uint64_t runtimeId) {
  std::lock_guard<std::mutex> lock(gRuntimeRegistryMutex);
  return gRuntimes.find(runtimeId) != gRuntimes.end();
}

std::shared_ptr<RuntimeLoop> unregisterRuntime(uint64_t runtimeId) {
  std::lock_guard<std::mutex> lock(gRuntimeRegistryMutex);
  auto it = gRuntimes.find(runtimeId);
  if (it == gRuntimes.end()) {
    return nullptr;
  }
  auto runtime = it->second;
  gRuntimes.erase(it);
  return runtime;
}

}  // namespace

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_raw_call_on_runtime(
    uint64_t runtimeId, uint32_t target, uint64_t interfaceId, uint16_t methodId,
    b_lean_obj_arg request) {
  auto runtime = getRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.Rpc runtime handle is invalid or already released");
  }

  try {
    const auto size = lean_sarray_size(request);
    const auto* reqData = lean_sarray_cptr(const_cast<lean_object*>(request));
    std::vector<uint8_t> requestCopy(size);
    if (size != 0) {
      std::memcpy(requestCopy.data(), reqData, size);
    }

    auto completion =
        runtime->enqueueRawCall(target, interfaceId, methodId, std::move(requestCopy), {});

    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
    }

    const auto responseSize = completion->result.response.size();
    const auto* responseData =
        responseSize == 0 ? nullptr : completion->result.response.data();
    return lean_io_result_mk_ok(mkByteArrayCopy(responseData, responseSize));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_rpc_raw_call_on_runtime");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_raw_call_with_caps_on_runtime(
    uint64_t runtimeId, uint32_t target, uint64_t interfaceId, uint16_t methodId,
    b_lean_obj_arg request, b_lean_obj_arg requestCaps) {
  auto runtime = getRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.Rpc runtime handle is invalid or already released");
  }

  try {
    const auto requestSize = lean_sarray_size(request);
    const auto* requestData = lean_sarray_cptr(const_cast<lean_object*>(request));
    std::vector<uint8_t> requestCopy(requestSize);
    if (requestSize != 0) {
      std::memcpy(requestCopy.data(), requestData, requestSize);
    }

    const auto requestCapsSize = lean_sarray_size(requestCaps);
    const auto* requestCapsData = lean_sarray_cptr(const_cast<lean_object*>(requestCaps));
    auto requestCapIds = decodeCapTable(requestCapsData, requestCapsSize);

    auto completion = runtime->enqueueRawCall(target, interfaceId, methodId, std::move(requestCopy),
                                              std::move(requestCapIds));
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
    }

    const auto responseSize = completion->result.response.size();
    const auto* responseData =
        responseSize == 0 ? nullptr : completion->result.response.data();
    const auto responseCapsSize = completion->result.responseCaps.size();
    const auto* responseCapsData =
        responseCapsSize == 0 ? nullptr : completion->result.responseCaps.data();

    lean_object* resultTuple = lean_alloc_ctor(0, 2, 0);
    lean_ctor_set(resultTuple, 0, mkByteArrayCopy(responseData, responseSize));
    lean_ctor_set(resultTuple, 1, mkByteArrayCopy(responseCapsData, responseCapsSize));
    return lean_io_result_mk_ok(resultTuple);
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_rpc_raw_call_with_caps_on_runtime");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_runtime_release_target(
    uint64_t runtimeId, uint32_t target) {
  auto runtime = getRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.Rpc runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueReleaseTarget(target);
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
    }

    lean_obj_res ok;
    mkIoOkUnit(ok);
    return ok;
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_rpc_runtime_release_target");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_runtime_new() {
  try {
    auto runtime = std::make_shared<RuntimeLoop>();

    uint64_t runtimeId;
    {
      std::lock_guard<std::mutex> lock(gRuntimeRegistryMutex);
      runtimeId = allocateRuntimeIdLocked();
      gRuntimes.emplace(runtimeId, runtime);
    }

    return lean_io_result_mk_ok(lean_box_uint64(runtimeId));
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_rpc_runtime_new");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_runtime_release(uint64_t runtimeId) {
  try {
    auto runtime = unregisterRuntime(runtimeId);
    if (runtime) {
      runtime->shutdown();
    }

    lean_obj_res ok;
    mkIoOkUnit(ok);
    return ok;
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_rpc_runtime_release");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_runtime_is_alive(uint64_t runtimeId) {
  return lean_io_result_mk_ok(lean_box(isRuntimeAlive(runtimeId) ? 1 : 0));
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_runtime_register_echo_target(
    uint64_t runtimeId) {
  auto runtime = getRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.Rpc runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueRegisterEchoTarget();
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      return lean_io_result_mk_ok(lean_box_uint32(completion->targetId));
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_rpc_runtime_register_echo_target");
  }
}
