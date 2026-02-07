#include <lean/lean.h>

#include <capnp/any.h>
#include <capnp/capability.h>
#include <capnp/rpc-twoparty.h>
#include <capnp/serialize.h>
#include <kj/async.h>
#include <kj/async-io.h>
#include <kj/io.h>
#include <kj/time.h>
#include <kj/vector.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
#include <memory>
#include <mutex>
#include <limits>
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

std::vector<uint8_t> copyByteArray(b_lean_obj_arg bytes) {
  const auto size = lean_sarray_size(bytes);
  const auto* data = reinterpret_cast<const uint8_t*>(lean_sarray_cptr(const_cast<lean_object*>(bytes)));
  return std::vector<uint8_t>(data, data + size);
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

struct UInt64Completion {
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  bool ok = false;
  std::string error;
  uint64_t value = 0;
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

void completeUInt64Success(const std::shared_ptr<UInt64Completion>& completion, uint64_t value) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = true;
    completion->value = value;
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeUInt64Failure(const std::shared_ptr<UInt64Completion>& completion,
                           std::string message) {
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

using TwoPartyRpcSystem = capnp::RpcSystem<capnp::rpc::twoparty::VatId>;

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

  std::shared_ptr<RegisterTargetCompletion> enqueueRegisterHandlerTarget(
      b_lean_obj_arg handler) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    auto* handlerObj = const_cast<lean_object*>(handler);
    lean_mark_mt(handlerObj);
    lean_inc(handlerObj);
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        lean_dec(handlerObj);
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedRegisterHandlerTarget{handlerObj, completion});
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

  std::shared_ptr<RegisterTargetCompletion> enqueueConnectTarget(std::string address,
                                                                 uint32_t portHint) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedConnectTarget{std::move(address), portHint, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueListenEcho(std::string address,
                                                              uint32_t portHint) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedListenEcho{std::move(address), portHint, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueAcceptEcho(uint32_t listenerId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedAcceptEcho{listenerId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueReleaseListener(uint32_t listenerId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedReleaseListener{listenerId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueNewClient(std::string address,
                                                             uint32_t portHint) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedNewClient{std::move(address), portHint, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueReleaseClient(uint32_t clientId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedReleaseClient{clientId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueClientBootstrap(uint32_t clientId) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedClientBootstrap{clientId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueClientOnDisconnect(uint32_t clientId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedClientOnDisconnect{clientId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueClientSetFlowLimit(uint32_t clientId, uint64_t words) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedClientSetFlowLimit{clientId, words, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueNewServer(uint32_t bootstrapTarget) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedNewServer{bootstrapTarget, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueReleaseServer(uint32_t serverId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedReleaseServer{serverId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueServerListen(uint32_t serverId,
                                                                std::string address,
                                                                uint32_t portHint) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedServerListen{serverId, std::move(address), portHint, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueServerAccept(uint32_t serverId, uint32_t listenerId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedServerAccept{serverId, listenerId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueServerDrain(uint32_t serverId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedServerDrain{serverId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UInt64Completion> enqueueClientQueueSize(uint32_t clientId) {
    auto completion = std::make_shared<UInt64Completion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUInt64Failure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedClientQueueSize{clientId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UInt64Completion> enqueueClientQueueCount(uint32_t clientId) {
    auto completion = std::make_shared<UInt64Completion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUInt64Failure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedClientQueueCount{clientId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UInt64Completion> enqueueClientOutgoingWaitNanos(uint32_t clientId) {
    auto completion = std::make_shared<UInt64Completion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUInt64Failure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedClientOutgoingWaitNanos{clientId, completion});
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

  struct QueuedRegisterHandlerTarget {
    lean_object* handler;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedReleaseTarget {
    uint32_t target;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedConnectTarget {
    std::string address;
    uint32_t portHint;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedListenEcho {
    std::string address;
    uint32_t portHint;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedAcceptEcho {
    uint32_t listenerId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedReleaseListener {
    uint32_t listenerId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedNewClient {
    std::string address;
    uint32_t portHint;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedReleaseClient {
    uint32_t clientId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedClientBootstrap {
    uint32_t clientId;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedClientOnDisconnect {
    uint32_t clientId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedClientSetFlowLimit {
    uint32_t clientId;
    uint64_t words;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedNewServer {
    uint32_t bootstrapTarget;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedReleaseServer {
    uint32_t serverId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedServerListen {
    uint32_t serverId;
    std::string address;
    uint32_t portHint;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedServerAccept {
    uint32_t serverId;
    uint32_t listenerId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedServerDrain {
    uint32_t serverId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedClientQueueSize {
    uint32_t clientId;
    std::shared_ptr<UInt64Completion> completion;
  };

  struct QueuedClientQueueCount {
    uint32_t clientId;
    std::shared_ptr<UInt64Completion> completion;
  };

  struct QueuedClientOutgoingWaitNanos {
    uint32_t clientId;
    std::shared_ptr<UInt64Completion> completion;
  };

  using QueuedOperation =
      std::variant<QueuedRawCall, QueuedRegisterEchoTarget, QueuedRegisterHandlerTarget,
                   QueuedReleaseTarget,
                   QueuedConnectTarget, QueuedListenEcho, QueuedAcceptEcho,
                   QueuedReleaseListener, QueuedNewClient, QueuedReleaseClient,
                   QueuedClientBootstrap, QueuedClientOnDisconnect, QueuedClientSetFlowLimit,
                   QueuedNewServer,
                   QueuedReleaseServer, QueuedServerListen, QueuedServerAccept,
                   QueuedServerDrain, QueuedClientQueueSize, QueuedClientQueueCount,
                   QueuedClientOutgoingWaitNanos>;

  struct LoopbackEchoPeer {
    kj::Own<kj::AsyncCapabilityStream> clientStream;
    kj::Own<capnp::TwoPartyServer> server;
    kj::Own<capnp::TwoPartyClient> client;
  };

  struct NetworkClientPeer {
    kj::Own<kj::AsyncIoStream> stream;
    kj::Own<capnp::TwoPartyVatNetwork> network;
    kj::Own<TwoPartyRpcSystem> rpcSystem;
  };

  struct NetworkServerPeer {
    kj::Own<capnp::TwoPartyServer> server;
  };

  struct RuntimeServer {
    explicit RuntimeServer(capnp::Capability::Client bootstrap)
        : bootstrap(kj::mv(bootstrap)) {}
    capnp::Capability::Client bootstrap;
    kj::Vector<kj::Own<NetworkServerPeer>> peers;
  };

  class LeanCapabilityServer final : public capnp::Capability::Server {
   public:
    LeanCapabilityServer(RuntimeLoop& runtime,
                         std::shared_ptr<std::atomic<uint32_t>> targetId,
                         lean_object* handler)
        : runtime_(runtime), targetId_(kj::mv(targetId)), handler_(handler) {
      lean_inc(handler_);
    }

    ~LeanCapabilityServer() { lean_dec(handler_); }

    DispatchCallResult dispatchCall(
        uint64_t interfaceId, uint16_t methodId,
        capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer> context) override {
      capnp::MallocMessageBuilder requestMessage;
      capnp::BuilderCapabilityTable requestCapTable;
      requestCapTable
          .imbue(requestMessage.getRoot<capnp::AnyPointer>())
          .setAs<capnp::AnyPointer>(context.getParams().getAs<capnp::AnyPointer>());

      auto requestWords = capnp::messageToFlatArray(requestMessage);
      auto requestBytes = requestWords.asBytes();
      auto requestObj = mkByteArrayCopy(reinterpret_cast<const uint8_t*>(requestBytes.begin()),
                                        requestBytes.size());

      std::vector<uint8_t> requestCaps;
      auto requestCapEntries = requestCapTable.getTable();
      requestCaps.reserve(requestCapEntries.size() * 4);
      for (auto& maybeHook : requestCapEntries) {
        KJ_IF_SOME(hook, maybeHook) {
          auto cap = capnp::Capability::Client(hook->addRef());
          appendUint32Le(requestCaps, runtime_.addTarget(kj::mv(cap)));
        } else {
          appendUint32Le(requestCaps, 0);
        }
      }
      auto requestCapsObj = mkByteArrayCopy(requestCaps.data(), requestCaps.size());

      auto targetId = targetId_->load(std::memory_order_relaxed);
      lean_inc(handler_);
      auto ioResult = lean_apply_6(handler_, lean_box_uint32(targetId), lean_box_uint64(interfaceId),
                                   lean_box(static_cast<size_t>(methodId)), requestObj,
                                   requestCapsObj, lean_box(0));
      if (lean_io_result_is_error(ioResult)) {
        lean_dec(ioResult);
        throw std::runtime_error("Lean RPC handler returned IO error");
      }

      auto resultPair = lean_io_result_take_value(ioResult);
      auto responseObj = lean_ctor_get(resultPair, 0);
      lean_inc(responseObj);
      auto responseCapsObj = lean_ctor_get(resultPair, 1);
      lean_inc(responseCapsObj);
      lean_dec(resultPair);

      auto responseBytesCopy = copyByteArray(responseObj);
      auto responseCapsCopy = copyByteArray(responseCapsObj);
      lean_dec(responseObj);
      lean_dec(responseCapsObj);

      kj::ArrayPtr<const kj::byte> responseBytes(
          reinterpret_cast<const kj::byte*>(responseBytesCopy.data()), responseBytesCopy.size());
      kj::ArrayInputStream input(responseBytes);
      capnp::ReaderOptions options;
      options.traversalLimitInWords = 1ull << 30;
      capnp::InputStreamMessageReader reader(input, options);
      auto responseRoot = reader.getRoot<capnp::AnyPointer>();

      auto responseCapIds = decodeCapTable(responseCapsCopy.data(), responseCapsCopy.size());
      if (responseCapIds.empty()) {
        context.getResults().setAs<capnp::AnyPointer>(responseRoot);
      } else {
        auto capTableBuilder =
            kj::heapArrayBuilder<kj::Maybe<kj::Own<capnp::ClientHook>>>(responseCapIds.size());
        for (auto capId : responseCapIds) {
          if (capId == 0) {
            capTableBuilder.add(kj::none);
            continue;
          }
          auto capIt = runtime_.targets_.find(capId);
          if (capIt == runtime_.targets_.end()) {
            throw std::runtime_error("unknown RPC response capability id from Lean handler: " +
                                     std::to_string(capId));
          }
          capnp::Capability::Client cap = capIt->second;
          capTableBuilder.add(capnp::ClientHook::from(kj::mv(cap)));
        }
        capnp::ReaderCapabilityTable responseCapTable(capTableBuilder.finish());
        context.getResults().setAs<capnp::AnyPointer>(responseCapTable.imbue(responseRoot));
      }

      return {kj::READY_NOW, false};
    }

   private:
    RuntimeLoop& runtime_;
    std::shared_ptr<std::atomic<uint32_t>> targetId_;
    lean_object* handler_;
  };

  uint32_t addTarget(capnp::Capability::Client cap) {
    uint32_t targetId = nextTargetId_++;
    while (targets_.find(targetId) != targets_.end()) {
      targetId = nextTargetId_++;
    }
    targets_.emplace(targetId, kj::mv(cap));
    return targetId;
  }

  uint32_t registerHandlerTarget(lean_object* handler) {
    auto targetIdRef = std::make_shared<std::atomic<uint32_t>>(0);
    auto server = kj::heap<LeanCapabilityServer>(*this, targetIdRef, handler);
    auto targetId = addTarget(capnp::Capability::Client(kj::mv(server)));
    targetIdRef->store(targetId, std::memory_order_relaxed);
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
    networkClientPeers_.erase(target);
    if (erased == 0) {
      throw std::runtime_error("unknown RPC target capability id: " + std::to_string(target));
    }
  }

  uint32_t addListener(kj::Own<kj::ConnectionReceiver>&& listener) {
    uint32_t listenerId = nextListenerId_++;
    while (listeners_.find(listenerId) != listeners_.end()) {
      listenerId = nextListenerId_++;
    }
    listeners_.emplace(listenerId, kj::mv(listener));
    return listenerId;
  }

  uint32_t addClient(kj::Own<NetworkClientPeer>&& client) {
    uint32_t clientId = nextClientId_++;
    while (clients_.find(clientId) != clients_.end()) {
      clientId = nextClientId_++;
    }
    clients_.emplace(clientId, kj::mv(client));
    return clientId;
  }

  uint32_t addServer(kj::Own<RuntimeServer>&& server) {
    uint32_t serverId = nextServerId_++;
    while (servers_.find(serverId) != servers_.end()) {
      serverId = nextServerId_++;
    }
    servers_.emplace(serverId, kj::mv(server));
    return serverId;
  }

  kj::Own<NetworkClientPeer> connectPeer(kj::AsyncIoProvider& ioProvider, kj::WaitScope& waitScope,
                                         const std::string& address, uint32_t portHint) {
    auto addr = ioProvider.getNetwork().parseAddress(address.c_str(), portHint).wait(waitScope);
    auto stream = addr->connect().wait(waitScope);
    auto network =
        kj::heap<capnp::TwoPartyVatNetwork>(*stream, capnp::rpc::twoparty::Side::CLIENT);
    auto rpcSystem = kj::heap<TwoPartyRpcSystem>(capnp::makeRpcClient(*network));

    auto peer = kj::heap<NetworkClientPeer>();
    peer->stream = kj::mv(stream);
    peer->network = kj::mv(network);
    peer->rpcSystem = kj::mv(rpcSystem);
    return peer;
  }

  uint32_t connectTarget(kj::AsyncIoProvider& ioProvider, kj::WaitScope& waitScope,
                         const std::string& address, uint32_t portHint) {
    auto peer = connectPeer(ioProvider, waitScope, address, portHint);
    capnp::word scratch[4];
    memset(&scratch, 0, sizeof(scratch));
    capnp::MallocMessageBuilder message(scratch);
    auto vatId = message.getRoot<capnp::rpc::twoparty::VatId>();
    vatId.setSide(peer->network->getSide() == capnp::rpc::twoparty::Side::CLIENT
                      ? capnp::rpc::twoparty::Side::SERVER
                      : capnp::rpc::twoparty::Side::CLIENT);
    auto cap = peer->rpcSystem->bootstrap(vatId);

    auto targetId = addTarget(kj::mv(cap));
    networkClientPeers_.emplace(targetId, kj::mv(peer));
    return targetId;
  }

  uint32_t newClient(kj::AsyncIoProvider& ioProvider, kj::WaitScope& waitScope,
                     const std::string& address, uint32_t portHint) {
    auto peer = connectPeer(ioProvider, waitScope, address, portHint);
    return addClient(kj::mv(peer));
  }

  void releaseClient(uint32_t clientId) {
    auto erased = clients_.erase(clientId);
    if (erased == 0) {
      throw std::runtime_error("unknown RPC client id: " + std::to_string(clientId));
    }
  }

  uint32_t clientBootstrap(uint32_t clientId) {
    auto clientIt = clients_.find(clientId);
    if (clientIt == clients_.end()) {
      throw std::runtime_error("unknown RPC client id: " + std::to_string(clientId));
    }
    capnp::word scratch[4];
    memset(&scratch, 0, sizeof(scratch));
    capnp::MallocMessageBuilder message(scratch);
    auto vatId = message.getRoot<capnp::rpc::twoparty::VatId>();
    vatId.setSide(clientIt->second->network->getSide() == capnp::rpc::twoparty::Side::CLIENT
                      ? capnp::rpc::twoparty::Side::SERVER
                      : capnp::rpc::twoparty::Side::CLIENT);
    return addTarget(clientIt->second->rpcSystem->bootstrap(vatId));
  }

  void clientOnDisconnect(kj::WaitScope& waitScope, uint32_t clientId) {
    auto clientIt = clients_.find(clientId);
    if (clientIt == clients_.end()) {
      throw std::runtime_error("unknown RPC client id: " + std::to_string(clientId));
    }
    clientIt->second->network->onDisconnect().wait(waitScope);
  }

  uint64_t clientQueueSize(uint32_t clientId) {
    auto clientIt = clients_.find(clientId);
    if (clientIt == clients_.end()) {
      throw std::runtime_error("unknown RPC client id: " + std::to_string(clientId));
    }
    return static_cast<uint64_t>(clientIt->second->network->getCurrentQueueSize());
  }

  uint64_t clientQueueCount(uint32_t clientId) {
    auto clientIt = clients_.find(clientId);
    if (clientIt == clients_.end()) {
      throw std::runtime_error("unknown RPC client id: " + std::to_string(clientId));
    }
    return static_cast<uint64_t>(clientIt->second->network->getCurrentQueueCount());
  }

  uint64_t clientOutgoingWaitNanos(uint32_t clientId) {
    auto clientIt = clients_.find(clientId);
    if (clientIt == clients_.end()) {
      throw std::runtime_error("unknown RPC client id: " + std::to_string(clientId));
    }
    auto wait = clientIt->second->network->getOutgoingMessageWaitTime();
    return static_cast<uint64_t>(wait / kj::NANOSECONDS);
  }

  void clientSetFlowLimit(uint32_t clientId, uint64_t words) {
    auto clientIt = clients_.find(clientId);
    if (clientIt == clients_.end()) {
      throw std::runtime_error("unknown RPC client id: " + std::to_string(clientId));
    }
    constexpr uint64_t maxWords = static_cast<uint64_t>(std::numeric_limits<size_t>::max());
    if (words > maxWords) {
      throw std::runtime_error("flow limit exceeds platform size_t range");
    }
    clientIt->second->rpcSystem->setFlowLimit(static_cast<size_t>(words));
  }

  uint32_t listenEcho(kj::AsyncIoProvider& ioProvider, kj::WaitScope& waitScope,
                      const std::string& address, uint32_t portHint) {
    auto addr = ioProvider.getNetwork().parseAddress(address.c_str(), portHint).wait(waitScope);
    auto listener = addr->listen();
    return addListener(kj::mv(listener));
  }

  void acceptEcho(kj::WaitScope& waitScope, uint32_t listenerId) {
    auto listenerIt = listeners_.find(listenerId);
    if (listenerIt == listeners_.end()) {
      throw std::runtime_error("unknown RPC listener id: " + std::to_string(listenerId));
    }
    auto connection = listenerIt->second->accept().wait(waitScope);
    auto server = kj::heap<capnp::TwoPartyServer>(
        capnp::Capability::Client(kj::heap<EchoCapabilityServer>()));
    server->accept(kj::mv(connection));

    auto peer = kj::heap<NetworkServerPeer>();
    peer->server = kj::mv(server);
    networkServerPeers_.add(kj::mv(peer));
  }

  void releaseListener(uint32_t listenerId) {
    auto erased = listeners_.erase(listenerId);
    if (erased == 0) {
      throw std::runtime_error("unknown RPC listener id: " + std::to_string(listenerId));
    }
  }

  uint32_t newServer(uint32_t bootstrapTarget) {
    auto targetIt = targets_.find(bootstrapTarget);
    if (targetIt == targets_.end()) {
      throw std::runtime_error("unknown RPC target capability id: " +
                               std::to_string(bootstrapTarget));
    }
    auto server = kj::heap<RuntimeServer>(targetIt->second);
    return addServer(kj::mv(server));
  }

  void releaseServer(uint32_t serverId) {
    auto erased = servers_.erase(serverId);
    if (erased == 0) {
      throw std::runtime_error("unknown RPC server id: " + std::to_string(serverId));
    }
  }

  uint32_t serverListen(kj::AsyncIoProvider& ioProvider, kj::WaitScope& waitScope,
                        uint32_t serverId, const std::string& address, uint32_t portHint) {
    auto serverIt = servers_.find(serverId);
    if (serverIt == servers_.end()) {
      throw std::runtime_error("unknown RPC server id: " + std::to_string(serverId));
    }
    auto addr = ioProvider.getNetwork().parseAddress(address.c_str(), portHint).wait(waitScope);
    auto listener = addr->listen();
    return addListener(kj::mv(listener));
  }

  void serverAccept(kj::WaitScope& waitScope, uint32_t serverId, uint32_t listenerId) {
    auto serverIt = servers_.find(serverId);
    if (serverIt == servers_.end()) {
      throw std::runtime_error("unknown RPC server id: " + std::to_string(serverId));
    }
    auto listenerIt = listeners_.find(listenerId);
    if (listenerIt == listeners_.end()) {
      throw std::runtime_error("unknown RPC listener id: " + std::to_string(listenerId));
    }

    auto connection = listenerIt->second->accept().wait(waitScope);
    auto peer = kj::heap<NetworkServerPeer>();
    peer->server = kj::heap<capnp::TwoPartyServer>(serverIt->second->bootstrap);
    peer->server->accept(kj::mv(connection));
    serverIt->second->peers.add(kj::mv(peer));
  }

  void serverDrain(kj::WaitScope& waitScope, uint32_t serverId) {
    auto serverIt = servers_.find(serverId);
    if (serverIt == servers_.end()) {
      throw std::runtime_error("unknown RPC server id: " + std::to_string(serverId));
    }
    for (auto& peer : serverIt->second->peers) {
      peer->server->drain().wait(waitScope);
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
        } else if (std::holds_alternative<QueuedRegisterHandlerTarget>(op)) {
          auto registration = std::get<QueuedRegisterHandlerTarget>(std::move(op));
          try {
            auto targetId = registerHandlerTarget(registration.handler);
            completeRegisterSuccess(registration.completion, targetId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(registration.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(registration.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                registration.completion,
                "unknown exception in capnp_lean_rpc_runtime_register_handler_target");
          }
          lean_dec(registration.handler);
        } else if (std::holds_alternative<QueuedReleaseTarget>(op)) {
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
        } else if (std::holds_alternative<QueuedConnectTarget>(op)) {
          auto connect = std::get<QueuedConnectTarget>(std::move(op));
          try {
            auto targetId =
                connectTarget(*io.provider, io.waitScope, connect.address, connect.portHint);
            completeRegisterSuccess(connect.completion, targetId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(connect.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(connect.completion, e.what());
          } catch (...) {
            completeRegisterFailure(connect.completion,
                                    "unknown exception in capnp_lean_rpc_runtime_connect");
          }
        } else if (std::holds_alternative<QueuedListenEcho>(op)) {
          auto listen = std::get<QueuedListenEcho>(std::move(op));
          try {
            auto listenerId = listenEcho(*io.provider, io.waitScope, listen.address, listen.portHint);
            completeRegisterSuccess(listen.completion, listenerId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(listen.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(listen.completion, e.what());
          } catch (...) {
            completeRegisterFailure(listen.completion,
                                    "unknown exception in capnp_lean_rpc_runtime_listen_echo");
          }
        } else if (std::holds_alternative<QueuedAcceptEcho>(op)) {
          auto accept = std::get<QueuedAcceptEcho>(std::move(op));
          try {
            acceptEcho(io.waitScope, accept.listenerId);
            completeUnitSuccess(accept.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(accept.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(accept.completion, e.what());
          } catch (...) {
            completeUnitFailure(accept.completion,
                                "unknown exception in capnp_lean_rpc_runtime_accept_echo");
          }
        } else if (std::holds_alternative<QueuedReleaseListener>(op)) {
          auto release = std::get<QueuedReleaseListener>(std::move(op));
          try {
            releaseListener(release.listenerId);
            completeUnitSuccess(release.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(release.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(release.completion, e.what());
          } catch (...) {
            completeUnitFailure(release.completion,
                                "unknown exception in capnp_lean_rpc_runtime_release_listener");
          }
        } else if (std::holds_alternative<QueuedNewClient>(op)) {
          auto newClientReq = std::get<QueuedNewClient>(std::move(op));
          try {
            auto clientId =
                newClient(*io.provider, io.waitScope, newClientReq.address, newClientReq.portHint);
            completeRegisterSuccess(newClientReq.completion, clientId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(newClientReq.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(newClientReq.completion, e.what());
          } catch (...) {
            completeRegisterFailure(newClientReq.completion,
                                    "unknown exception in capnp_lean_rpc_runtime_new_client");
          }
        } else if (std::holds_alternative<QueuedReleaseClient>(op)) {
          auto release = std::get<QueuedReleaseClient>(std::move(op));
          try {
            releaseClient(release.clientId);
            completeUnitSuccess(release.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(release.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(release.completion, e.what());
          } catch (...) {
            completeUnitFailure(release.completion,
                                "unknown exception in capnp_lean_rpc_runtime_release_client");
          }
        } else if (std::holds_alternative<QueuedClientBootstrap>(op)) {
          auto bootstrap = std::get<QueuedClientBootstrap>(std::move(op));
          try {
            auto targetId = clientBootstrap(bootstrap.clientId);
            completeRegisterSuccess(bootstrap.completion, targetId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(bootstrap.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(bootstrap.completion, e.what());
          } catch (...) {
            completeRegisterFailure(bootstrap.completion,
                                    "unknown exception in capnp_lean_rpc_runtime_client_bootstrap");
          }
        } else if (std::holds_alternative<QueuedClientOnDisconnect>(op)) {
          auto disconnect = std::get<QueuedClientOnDisconnect>(std::move(op));
          try {
            clientOnDisconnect(io.waitScope, disconnect.clientId);
            completeUnitSuccess(disconnect.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(disconnect.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(disconnect.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                disconnect.completion,
                "unknown exception in capnp_lean_rpc_runtime_client_on_disconnect");
          }
        } else if (std::holds_alternative<QueuedClientSetFlowLimit>(op)) {
          auto setLimit = std::get<QueuedClientSetFlowLimit>(std::move(op));
          try {
            clientSetFlowLimit(setLimit.clientId, setLimit.words);
            completeUnitSuccess(setLimit.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(setLimit.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(setLimit.completion, e.what());
          } catch (...) {
            completeUnitFailure(setLimit.completion,
                                "unknown exception in capnp_lean_rpc_runtime_client_set_flow_limit");
          }
        } else if (std::holds_alternative<QueuedClientQueueSize>(op)) {
          auto metric = std::get<QueuedClientQueueSize>(std::move(op));
          try {
            completeUInt64Success(metric.completion, clientQueueSize(metric.clientId));
          } catch (const kj::Exception& e) {
            completeUInt64Failure(metric.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUInt64Failure(metric.completion, e.what());
          } catch (...) {
            completeUInt64Failure(metric.completion,
                                  "unknown exception in capnp_lean_rpc_runtime_client_queue_size");
          }
        } else if (std::holds_alternative<QueuedClientQueueCount>(op)) {
          auto metric = std::get<QueuedClientQueueCount>(std::move(op));
          try {
            completeUInt64Success(metric.completion, clientQueueCount(metric.clientId));
          } catch (const kj::Exception& e) {
            completeUInt64Failure(metric.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUInt64Failure(metric.completion, e.what());
          } catch (...) {
            completeUInt64Failure(metric.completion,
                                  "unknown exception in capnp_lean_rpc_runtime_client_queue_count");
          }
        } else if (std::holds_alternative<QueuedClientOutgoingWaitNanos>(op)) {
          auto metric = std::get<QueuedClientOutgoingWaitNanos>(std::move(op));
          try {
            completeUInt64Success(metric.completion, clientOutgoingWaitNanos(metric.clientId));
          } catch (const kj::Exception& e) {
            completeUInt64Failure(metric.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUInt64Failure(metric.completion, e.what());
          } catch (...) {
            completeUInt64Failure(
                metric.completion,
                "unknown exception in capnp_lean_rpc_runtime_client_outgoing_wait_nanos");
          }
        } else if (std::holds_alternative<QueuedNewServer>(op)) {
          auto newServerReq = std::get<QueuedNewServer>(std::move(op));
          try {
            auto serverId = newServer(newServerReq.bootstrapTarget);
            completeRegisterSuccess(newServerReq.completion, serverId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(newServerReq.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(newServerReq.completion, e.what());
          } catch (...) {
            completeRegisterFailure(newServerReq.completion,
                                    "unknown exception in capnp_lean_rpc_runtime_new_server");
          }
        } else if (std::holds_alternative<QueuedReleaseServer>(op)) {
          auto release = std::get<QueuedReleaseServer>(std::move(op));
          try {
            releaseServer(release.serverId);
            completeUnitSuccess(release.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(release.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(release.completion, e.what());
          } catch (...) {
            completeUnitFailure(release.completion,
                                "unknown exception in capnp_lean_rpc_runtime_release_server");
          }
        } else if (std::holds_alternative<QueuedServerListen>(op)) {
          auto listen = std::get<QueuedServerListen>(std::move(op));
          try {
            auto listenerId = serverListen(*io.provider, io.waitScope, listen.serverId, listen.address,
                                           listen.portHint);
            completeRegisterSuccess(listen.completion, listenerId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(listen.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(listen.completion, e.what());
          } catch (...) {
            completeRegisterFailure(listen.completion,
                                    "unknown exception in capnp_lean_rpc_runtime_server_listen");
          }
        } else if (std::holds_alternative<QueuedServerAccept>(op)) {
          auto accept = std::get<QueuedServerAccept>(std::move(op));
          try {
            serverAccept(io.waitScope, accept.serverId, accept.listenerId);
            completeUnitSuccess(accept.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(accept.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(accept.completion, e.what());
          } catch (...) {
            completeUnitFailure(accept.completion,
                                "unknown exception in capnp_lean_rpc_runtime_server_accept");
          }
        } else {
          auto drain = std::get<QueuedServerDrain>(std::move(op));
          try {
            serverDrain(io.waitScope, drain.serverId);
            completeUnitSuccess(drain.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(drain.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(drain.completion, e.what());
          } catch (...) {
            completeUnitFailure(drain.completion,
                                "unknown exception in capnp_lean_rpc_runtime_server_drain");
          }
        }
      }

      // Tear down RPC clients/servers on the runtime thread while async I/O is still valid.
      targets_.clear();
      listeners_.clear();
      loopbackEchoPeers_.clear();
      networkClientPeers_.clear();
      networkServerPeers_.clear();
      clients_.clear();
      servers_.clear();

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
      } else if (std::holds_alternative<QueuedRegisterHandlerTarget>(op)) {
        auto& registration = std::get<QueuedRegisterHandlerTarget>(op);
        lean_dec(registration.handler);
        completeRegisterFailure(registration.completion, message);
      } else if (std::holds_alternative<QueuedReleaseTarget>(op)) {
        completeUnitFailure(std::get<QueuedReleaseTarget>(op).completion, message);
      } else if (std::holds_alternative<QueuedConnectTarget>(op)) {
        completeRegisterFailure(std::get<QueuedConnectTarget>(op).completion, message);
      } else if (std::holds_alternative<QueuedListenEcho>(op)) {
        completeRegisterFailure(std::get<QueuedListenEcho>(op).completion, message);
      } else if (std::holds_alternative<QueuedAcceptEcho>(op)) {
        completeUnitFailure(std::get<QueuedAcceptEcho>(op).completion, message);
      } else if (std::holds_alternative<QueuedReleaseListener>(op)) {
        completeUnitFailure(std::get<QueuedReleaseListener>(op).completion, message);
      } else if (std::holds_alternative<QueuedNewClient>(op)) {
        completeRegisterFailure(std::get<QueuedNewClient>(op).completion, message);
      } else if (std::holds_alternative<QueuedReleaseClient>(op)) {
        completeUnitFailure(std::get<QueuedReleaseClient>(op).completion, message);
      } else if (std::holds_alternative<QueuedClientBootstrap>(op)) {
        completeRegisterFailure(std::get<QueuedClientBootstrap>(op).completion, message);
      } else if (std::holds_alternative<QueuedClientOnDisconnect>(op)) {
        completeUnitFailure(std::get<QueuedClientOnDisconnect>(op).completion, message);
      } else if (std::holds_alternative<QueuedClientSetFlowLimit>(op)) {
        completeUnitFailure(std::get<QueuedClientSetFlowLimit>(op).completion, message);
      } else if (std::holds_alternative<QueuedNewServer>(op)) {
        completeRegisterFailure(std::get<QueuedNewServer>(op).completion, message);
      } else if (std::holds_alternative<QueuedReleaseServer>(op)) {
        completeUnitFailure(std::get<QueuedReleaseServer>(op).completion, message);
      } else if (std::holds_alternative<QueuedServerListen>(op)) {
        completeRegisterFailure(std::get<QueuedServerListen>(op).completion, message);
      } else if (std::holds_alternative<QueuedServerAccept>(op)) {
        completeUnitFailure(std::get<QueuedServerAccept>(op).completion, message);
      } else if (std::holds_alternative<QueuedClientQueueSize>(op)) {
        completeUInt64Failure(std::get<QueuedClientQueueSize>(op).completion, message);
      } else if (std::holds_alternative<QueuedClientQueueCount>(op)) {
        completeUInt64Failure(std::get<QueuedClientQueueCount>(op).completion, message);
      } else if (std::holds_alternative<QueuedClientOutgoingWaitNanos>(op)) {
        completeUInt64Failure(std::get<QueuedClientOutgoingWaitNanos>(op).completion, message);
      } else {
        completeUnitFailure(std::get<QueuedServerDrain>(op).completion, message);
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
  std::unordered_map<uint32_t, kj::Own<kj::ConnectionReceiver>> listeners_;
  std::unordered_map<uint32_t, kj::Own<LoopbackEchoPeer>> loopbackEchoPeers_;
  std::unordered_map<uint32_t, kj::Own<NetworkClientPeer>> networkClientPeers_;
  kj::Vector<kj::Own<NetworkServerPeer>> networkServerPeers_;
  std::unordered_map<uint32_t, kj::Own<NetworkClientPeer>> clients_;
  std::unordered_map<uint32_t, kj::Own<RuntimeServer>> servers_;
  uint32_t nextTargetId_ = 1;
  uint32_t nextListenerId_ = 1;
  uint32_t nextClientId_ = 1;
  uint32_t nextServerId_ = 1;
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

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_runtime_connect(
    uint64_t runtimeId, b_lean_obj_arg address, uint32_t portHint) {
  auto runtime = getRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.Rpc runtime handle is invalid or already released");
  }

  try {
    std::string addressCopy = lean_string_cstr(address);
    auto completion = runtime->enqueueConnectTarget(std::move(addressCopy), portHint);
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
    return mkIoUserError("unknown exception in capnp_lean_rpc_runtime_connect");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_runtime_listen_echo(
    uint64_t runtimeId, b_lean_obj_arg address, uint32_t portHint) {
  auto runtime = getRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.Rpc runtime handle is invalid or already released");
  }

  try {
    std::string addressCopy = lean_string_cstr(address);
    auto completion = runtime->enqueueListenEcho(std::move(addressCopy), portHint);
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
    return mkIoUserError("unknown exception in capnp_lean_rpc_runtime_listen_echo");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_runtime_accept_echo(
    uint64_t runtimeId, uint32_t listenerId) {
  auto runtime = getRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.Rpc runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueAcceptEcho(listenerId);
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
    return mkIoUserError("unknown exception in capnp_lean_rpc_runtime_accept_echo");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_runtime_release_listener(
    uint64_t runtimeId, uint32_t listenerId) {
  auto runtime = getRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.Rpc runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueReleaseListener(listenerId);
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
    return mkIoUserError("unknown exception in capnp_lean_rpc_runtime_release_listener");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_runtime_new_client(
    uint64_t runtimeId, b_lean_obj_arg address, uint32_t portHint) {
  auto runtime = getRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.Rpc runtime handle is invalid or already released");
  }

  try {
    std::string addressCopy = lean_string_cstr(address);
    auto completion = runtime->enqueueNewClient(std::move(addressCopy), portHint);
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
    return mkIoUserError("unknown exception in capnp_lean_rpc_runtime_new_client");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_runtime_release_client(
    uint64_t runtimeId, uint32_t clientId) {
  auto runtime = getRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.Rpc runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueReleaseClient(clientId);
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
    return mkIoUserError("unknown exception in capnp_lean_rpc_runtime_release_client");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_runtime_client_bootstrap(
    uint64_t runtimeId, uint32_t clientId) {
  auto runtime = getRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.Rpc runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueClientBootstrap(clientId);
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
    return mkIoUserError("unknown exception in capnp_lean_rpc_runtime_client_bootstrap");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_runtime_client_on_disconnect(
    uint64_t runtimeId, uint32_t clientId) {
  auto runtime = getRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.Rpc runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueClientOnDisconnect(clientId);
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
    return mkIoUserError("unknown exception in capnp_lean_rpc_runtime_client_on_disconnect");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_runtime_client_set_flow_limit(
    uint64_t runtimeId, uint32_t clientId, uint64_t words) {
  auto runtime = getRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.Rpc runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueClientSetFlowLimit(clientId, words);
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
    return mkIoUserError("unknown exception in capnp_lean_rpc_runtime_client_set_flow_limit");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_runtime_client_queue_size(
    uint64_t runtimeId, uint32_t clientId) {
  auto runtime = getRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.Rpc runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueClientQueueSize(clientId);
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      return lean_io_result_mk_ok(lean_box_uint64(completion->value));
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_rpc_runtime_client_queue_size");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_runtime_client_queue_count(
    uint64_t runtimeId, uint32_t clientId) {
  auto runtime = getRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.Rpc runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueClientQueueCount(clientId);
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      return lean_io_result_mk_ok(lean_box_uint64(completion->value));
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_rpc_runtime_client_queue_count");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_runtime_client_outgoing_wait_nanos(
    uint64_t runtimeId, uint32_t clientId) {
  auto runtime = getRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.Rpc runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueClientOutgoingWaitNanos(clientId);
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      return lean_io_result_mk_ok(lean_box_uint64(completion->value));
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_rpc_runtime_client_outgoing_wait_nanos");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_runtime_new_server(
    uint64_t runtimeId, uint32_t bootstrapTarget) {
  auto runtime = getRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.Rpc runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueNewServer(bootstrapTarget);
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
    return mkIoUserError("unknown exception in capnp_lean_rpc_runtime_new_server");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_runtime_release_server(
    uint64_t runtimeId, uint32_t serverId) {
  auto runtime = getRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.Rpc runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueReleaseServer(serverId);
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
    return mkIoUserError("unknown exception in capnp_lean_rpc_runtime_release_server");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_runtime_server_listen(
    uint64_t runtimeId, uint32_t serverId, b_lean_obj_arg address, uint32_t portHint) {
  auto runtime = getRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.Rpc runtime handle is invalid or already released");
  }

  try {
    std::string addressCopy = lean_string_cstr(address);
    auto completion = runtime->enqueueServerListen(serverId, std::move(addressCopy), portHint);
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
    return mkIoUserError("unknown exception in capnp_lean_rpc_runtime_server_listen");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_runtime_server_accept(
    uint64_t runtimeId, uint32_t serverId, uint32_t listenerId) {
  auto runtime = getRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.Rpc runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueServerAccept(serverId, listenerId);
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
    return mkIoUserError("unknown exception in capnp_lean_rpc_runtime_server_accept");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_runtime_server_drain(
    uint64_t runtimeId, uint32_t serverId) {
  auto runtime = getRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.Rpc runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueServerDrain(serverId);
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
    return mkIoUserError("unknown exception in capnp_lean_rpc_runtime_server_drain");
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

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_runtime_register_handler_target(
    uint64_t runtimeId, b_lean_obj_arg handler) {
  auto runtime = getRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.Rpc runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueRegisterHandlerTarget(handler);
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
    return mkIoUserError("unknown exception in capnp_lean_rpc_runtime_register_handler_target");
  }
}
