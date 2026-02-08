#include <lean/lean.h>

#include <kj/async.h>
#include <kj/async-io.h>
#include <kj/time.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <variant>
#include <vector>

namespace {

lean_obj_res mkIoUserError(const std::string& message) {
  lean_object* msg = lean_mk_string(message.c_str());
  lean_object* err = lean_mk_io_user_error(msg);
  return lean_io_result_mk_error(err);
}

void mkIoOkUnit(lean_obj_res& out) { out = lean_io_result_mk_ok(lean_box(0)); }

lean_obj_res mkByteArrayCopy(const uint8_t* data, size_t size) {
  lean_object* out = lean_alloc_sarray(1, size, size);
  if (size != 0) {
    std::memcpy(lean_sarray_cptr(out), data, size);
  }
  lean_sarray_set_size(out, size);
  return out;
}

std::vector<uint8_t> copyByteArray(b_lean_obj_arg bytes) {
  const auto size = lean_sarray_size(bytes);
  const auto* data =
      reinterpret_cast<const uint8_t*>(lean_sarray_cptr(const_cast<lean_object*>(bytes)));
  std::vector<uint8_t> out(size);
  if (size != 0) {
    std::memcpy(out.data(), data, size);
  }
  return out;
}

uint32_t readUint32Le(const uint8_t* data) {
  return static_cast<uint32_t>(data[0]) |
         (static_cast<uint32_t>(data[1]) << 8) |
         (static_cast<uint32_t>(data[2]) << 16) |
         (static_cast<uint32_t>(data[3]) << 24);
}

std::vector<uint32_t> decodeUint32Array(b_lean_obj_arg bytes) {
  const auto size = lean_sarray_size(bytes);
  const auto* data =
      reinterpret_cast<const uint8_t*>(lean_sarray_cptr(const_cast<lean_object*>(bytes)));
  if ((size % 4) != 0) {
    throw std::runtime_error("uint32 array payload must be a multiple of 4 bytes");
  }
  std::vector<uint32_t> out;
  out.reserve(size / 4);
  for (size_t i = 0; i < size; i += 4) {
    out.push_back(readUint32Le(data + i));
  }
  return out;
}

std::string describeKjException(const kj::Exception& e) {
  std::string message(e.getDescription().cStr());
  KJ_IF_SOME(detail, e.getDetail(0)) {
    message += "\ndetail: ";
    message.append(reinterpret_cast<const char*>(detail.begin()), detail.size());
  }
  return message;
}

struct UnitCompletion {
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  bool ok = false;
  std::string error;
};

struct PromiseIdCompletion {
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  bool ok = false;
  uint32_t promiseId = 0;
  std::string error;
};

struct HandleCompletion {
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  bool ok = false;
  uint32_t handle = 0;
  std::string error;
};

struct BytesCompletion {
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  bool ok = false;
  std::vector<uint8_t> bytes;
  std::string error;
};

struct HandlePairCompletion {
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  bool ok = false;
  uint32_t first = 0;
  uint32_t second = 0;
  std::string error;
};

struct BoolCompletion {
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  bool ok = false;
  bool value = false;
  std::string error;
};

struct UInt32Completion {
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  bool ok = false;
  uint32_t value = 0;
  std::string error;
};

struct OptionalStringCompletion {
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  bool ok = false;
  bool hasValue = false;
  std::string value;
  std::string error;
};

struct DatagramReceiveCompletion {
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  bool ok = false;
  std::string sourceAddress;
  std::vector<uint8_t> bytes;
  std::string error;
};

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

void completePromiseIdSuccess(const std::shared_ptr<PromiseIdCompletion>& completion,
                              uint32_t promiseId) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = true;
    completion->promiseId = promiseId;
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completePromiseIdFailure(const std::shared_ptr<PromiseIdCompletion>& completion,
                              std::string message) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = false;
    completion->error = std::move(message);
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeHandleSuccess(const std::shared_ptr<HandleCompletion>& completion, uint32_t handle) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = true;
    completion->handle = handle;
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeHandleFailure(const std::shared_ptr<HandleCompletion>& completion,
                           std::string message) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = false;
    completion->error = std::move(message);
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeBytesSuccess(const std::shared_ptr<BytesCompletion>& completion,
                          std::vector<uint8_t> bytes) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = true;
    completion->bytes = std::move(bytes);
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeBytesFailure(const std::shared_ptr<BytesCompletion>& completion, std::string message) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = false;
    completion->error = std::move(message);
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeHandlePairSuccess(const std::shared_ptr<HandlePairCompletion>& completion,
                               uint32_t first, uint32_t second) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = true;
    completion->first = first;
    completion->second = second;
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeHandlePairFailure(const std::shared_ptr<HandlePairCompletion>& completion,
                               std::string message) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = false;
    completion->error = std::move(message);
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeBoolSuccess(const std::shared_ptr<BoolCompletion>& completion, bool value) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = true;
    completion->value = value;
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeBoolFailure(const std::shared_ptr<BoolCompletion>& completion, std::string message) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = false;
    completion->error = std::move(message);
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeUInt32Success(const std::shared_ptr<UInt32Completion>& completion, uint32_t value) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = true;
    completion->value = value;
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeUInt32Failure(const std::shared_ptr<UInt32Completion>& completion,
                           std::string message) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = false;
    completion->error = std::move(message);
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeOptionalStringSuccess(const std::shared_ptr<OptionalStringCompletion>& completion,
                                   kj::Maybe<std::string> value) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = true;
    KJ_IF_SOME(v, value) {
      completion->hasValue = true;
      completion->value = kj::mv(v);
    } else {
      completion->hasValue = false;
      completion->value.clear();
    }
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeOptionalStringFailure(const std::shared_ptr<OptionalStringCompletion>& completion,
                                   std::string message) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = false;
    completion->error = std::move(message);
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeDatagramReceiveSuccess(const std::shared_ptr<DatagramReceiveCompletion>& completion,
                                    std::string sourceAddress, std::vector<uint8_t> bytes) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = true;
    completion->sourceAddress = std::move(sourceAddress);
    completion->bytes = std::move(bytes);
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeDatagramReceiveFailure(const std::shared_ptr<DatagramReceiveCompletion>& completion,
                                    std::string message) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = false;
    completion->error = std::move(message);
    completion->done = true;
  }
  completion->cv.notify_one();
}

class KjAsyncRuntimeLoop {
 public:
  KjAsyncRuntimeLoop() : worker_(&KjAsyncRuntimeLoop::run, this) {
    std::unique_lock<std::mutex> lock(startupMutex_);
    startupCv_.wait(lock, [this]() { return startupComplete_; });
    if (!startupError_.empty()) {
      throw std::runtime_error(startupError_);
    }
  }

  ~KjAsyncRuntimeLoop() { shutdown(); }

  bool isAlive() const { return alive_.load(std::memory_order_acquire); }

  void shutdown() {
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        // Another thread already initiated shutdown.
      } else {
        stopping_ = true;
      }
    }
    queueCv_.notify_one();
    if (worker_.joinable()) {
      worker_.join();
    }
  }

  std::shared_ptr<PromiseIdCompletion> enqueueSleepNanos(uint64_t delayNanos) {
    auto completion = std::make_shared<PromiseIdCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completePromiseIdFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedSleepNanos{delayNanos, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueAwaitPromise(uint32_t promiseId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedAwaitPromise{promiseId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueCancelPromise(uint32_t promiseId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedCancelPromise{promiseId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueReleasePromise(uint32_t promiseId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedReleasePromise{promiseId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<HandleCompletion> enqueueListen(std::string address, uint32_t portHint) {
    auto completion = std::make_shared<HandleCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeHandleFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedListen{std::move(address), portHint, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueReleaseListener(uint32_t listenerId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedReleaseListener{listenerId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<HandleCompletion> enqueueAccept(uint32_t listenerId) {
    auto completion = std::make_shared<HandleCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeHandleFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedAccept{listenerId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<PromiseIdCompletion> enqueueAcceptStart(uint32_t listenerId) {
    auto completion = std::make_shared<PromiseIdCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completePromiseIdFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedAcceptStart{listenerId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<HandleCompletion> enqueueConnect(std::string address, uint32_t portHint) {
    auto completion = std::make_shared<HandleCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeHandleFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedConnect{std::move(address), portHint, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<PromiseIdCompletion> enqueueConnectStart(std::string address, uint32_t portHint) {
    auto completion = std::make_shared<PromiseIdCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completePromiseIdFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedConnectStart{std::move(address), portHint, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<HandleCompletion> enqueueAwaitConnectionPromise(uint32_t promiseId) {
    auto completion = std::make_shared<HandleCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeHandleFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedAwaitConnectionPromise{promiseId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueCancelConnectionPromise(uint32_t promiseId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedCancelConnectionPromise{promiseId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueReleaseConnectionPromise(uint32_t promiseId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedReleaseConnectionPromise{promiseId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueReleaseConnection(uint32_t connectionId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedReleaseConnection{connectionId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueConnectionWrite(uint32_t connectionId,
                                                         std::vector<uint8_t> bytes) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedConnectionWrite{connectionId, std::move(bytes), completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<BytesCompletion> enqueueConnectionRead(uint32_t connectionId, uint32_t minBytes,
                                                         uint32_t maxBytes) {
    auto completion = std::make_shared<BytesCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeBytesFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedConnectionRead{connectionId, minBytes, maxBytes, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueConnectionShutdownWrite(uint32_t connectionId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedConnectionShutdownWrite{connectionId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<PromiseIdCompletion> enqueuePromiseAllStart(std::vector<uint32_t> promiseIds) {
    auto completion = std::make_shared<PromiseIdCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completePromiseIdFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedPromiseAllStart{std::move(promiseIds), completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<PromiseIdCompletion> enqueuePromiseRaceStart(std::vector<uint32_t> promiseIds) {
    auto completion = std::make_shared<PromiseIdCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completePromiseIdFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedPromiseRaceStart{std::move(promiseIds), completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<HandleCompletion> enqueueTaskSetNew() {
    auto completion = std::make_shared<HandleCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeHandleFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedTaskSetNew{completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueTaskSetRelease(uint32_t taskSetId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedTaskSetRelease{taskSetId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueTaskSetAddPromise(uint32_t taskSetId, uint32_t promiseId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedTaskSetAddPromise{taskSetId, promiseId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueTaskSetClear(uint32_t taskSetId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedTaskSetClear{taskSetId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<BoolCompletion> enqueueTaskSetIsEmpty(uint32_t taskSetId) {
    auto completion = std::make_shared<BoolCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeBoolFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedTaskSetIsEmpty{taskSetId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<PromiseIdCompletion> enqueueTaskSetOnEmptyStart(uint32_t taskSetId) {
    auto completion = std::make_shared<PromiseIdCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completePromiseIdFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedTaskSetOnEmptyStart{taskSetId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UInt32Completion> enqueueTaskSetErrorCount(uint32_t taskSetId) {
    auto completion = std::make_shared<UInt32Completion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUInt32Failure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedTaskSetErrorCount{taskSetId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<OptionalStringCompletion> enqueueTaskSetTakeLastError(uint32_t taskSetId) {
    auto completion = std::make_shared<OptionalStringCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeOptionalStringFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedTaskSetTakeLastError{taskSetId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<PromiseIdCompletion> enqueueConnectionWhenWriteDisconnectedStart(
      uint32_t connectionId) {
    auto completion = std::make_shared<PromiseIdCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completePromiseIdFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedConnectionWhenWriteDisconnectedStart{connectionId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueConnectionAbortRead(uint32_t connectionId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedConnectionAbortRead{connectionId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueConnectionAbortWrite(uint32_t connectionId,
                                                              std::string reason) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedConnectionAbortWrite{connectionId, std::move(reason), completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<HandlePairCompletion> enqueueNewTwoWayPipe() {
    auto completion = std::make_shared<HandlePairCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeHandlePairFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedNewTwoWayPipe{completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<HandleCompletion> enqueueDatagramBind(std::string address, uint32_t portHint) {
    auto completion = std::make_shared<HandleCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeHandleFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedDatagramBind{std::move(address), portHint, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueDatagramReleasePort(uint32_t portId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedDatagramReleasePort{portId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<HandleCompletion> enqueueDatagramGetPort(uint32_t portId) {
    auto completion = std::make_shared<HandleCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeHandleFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedDatagramGetPort{portId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UInt32Completion> enqueueDatagramSend(uint32_t portId, std::string address,
                                                        uint32_t portHint,
                                                        std::vector<uint8_t> bytes) {
    auto completion = std::make_shared<UInt32Completion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUInt32Failure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(
          QueuedDatagramSend{portId, std::move(address), portHint, std::move(bytes), completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<DatagramReceiveCompletion> enqueueDatagramReceive(uint32_t portId,
                                                                    uint32_t maxBytes) {
    auto completion = std::make_shared<DatagramReceiveCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeDatagramReceiveFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedDatagramReceive{portId, maxBytes, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

 private:
  struct PendingPromise {
    PendingPromise(kj::Promise<void>&& promise, kj::Own<kj::Canceler>&& canceler)
        : promise(kj::mv(promise)), canceler(kj::mv(canceler)) {}

    PendingPromise(PendingPromise&&) = default;
    PendingPromise& operator=(PendingPromise&&) = default;
    PendingPromise(const PendingPromise&) = delete;
    PendingPromise& operator=(const PendingPromise&) = delete;

    kj::Promise<void> promise;
    kj::Own<kj::Canceler> canceler;
  };

  struct PendingConnectionPromise {
    PendingConnectionPromise(kj::Promise<kj::Own<kj::AsyncIoStream>>&& promise,
                             kj::Own<kj::Canceler>&& canceler)
        : promise(kj::mv(promise)), canceler(kj::mv(canceler)) {}

    PendingConnectionPromise(PendingConnectionPromise&&) = default;
    PendingConnectionPromise& operator=(PendingConnectionPromise&&) = default;
    PendingConnectionPromise(const PendingConnectionPromise&) = delete;
    PendingConnectionPromise& operator=(const PendingConnectionPromise&) = delete;

    kj::Promise<kj::Own<kj::AsyncIoStream>> promise;
    kj::Own<kj::Canceler> canceler;
  };

  struct RuntimeTaskSet {
    class ErrorHandler final : public kj::TaskSet::ErrorHandler {
     public:
      explicit ErrorHandler(RuntimeTaskSet& state) : state_(state) {}

      void taskFailed(kj::Exception&& exception) override {
        std::lock_guard<std::mutex> lock(state_.mutex);
        state_.errorCount += 1;
        state_.lastError = describeKjException(exception);
      }

     private:
      RuntimeTaskSet& state_;
    };

    RuntimeTaskSet() : errorHandler(*this), tasks(errorHandler) {}

    std::mutex mutex;
    uint32_t errorCount = 0;
    std::string lastError;
    ErrorHandler errorHandler;
    kj::TaskSet tasks;
  };

  struct QueuedSleepNanos {
    uint64_t delayNanos;
    std::shared_ptr<PromiseIdCompletion> completion;
  };

  struct QueuedAwaitPromise {
    uint32_t promiseId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedCancelPromise {
    uint32_t promiseId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedReleasePromise {
    uint32_t promiseId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedListen {
    std::string address;
    uint32_t portHint;
    std::shared_ptr<HandleCompletion> completion;
  };

  struct QueuedReleaseListener {
    uint32_t listenerId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedAccept {
    uint32_t listenerId;
    std::shared_ptr<HandleCompletion> completion;
  };

  struct QueuedAcceptStart {
    uint32_t listenerId;
    std::shared_ptr<PromiseIdCompletion> completion;
  };

  struct QueuedConnect {
    std::string address;
    uint32_t portHint;
    std::shared_ptr<HandleCompletion> completion;
  };

  struct QueuedConnectStart {
    std::string address;
    uint32_t portHint;
    std::shared_ptr<PromiseIdCompletion> completion;
  };

  struct QueuedAwaitConnectionPromise {
    uint32_t promiseId;
    std::shared_ptr<HandleCompletion> completion;
  };

  struct QueuedCancelConnectionPromise {
    uint32_t promiseId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedReleaseConnectionPromise {
    uint32_t promiseId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedReleaseConnection {
    uint32_t connectionId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedConnectionWrite {
    uint32_t connectionId;
    std::vector<uint8_t> bytes;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedConnectionRead {
    uint32_t connectionId;
    uint32_t minBytes;
    uint32_t maxBytes;
    std::shared_ptr<BytesCompletion> completion;
  };

  struct QueuedConnectionShutdownWrite {
    uint32_t connectionId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedPromiseAllStart {
    std::vector<uint32_t> promiseIds;
    std::shared_ptr<PromiseIdCompletion> completion;
  };

  struct QueuedPromiseRaceStart {
    std::vector<uint32_t> promiseIds;
    std::shared_ptr<PromiseIdCompletion> completion;
  };

  struct QueuedTaskSetNew {
    std::shared_ptr<HandleCompletion> completion;
  };

  struct QueuedTaskSetRelease {
    uint32_t taskSetId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedTaskSetAddPromise {
    uint32_t taskSetId;
    uint32_t promiseId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedTaskSetClear {
    uint32_t taskSetId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedTaskSetIsEmpty {
    uint32_t taskSetId;
    std::shared_ptr<BoolCompletion> completion;
  };

  struct QueuedTaskSetOnEmptyStart {
    uint32_t taskSetId;
    std::shared_ptr<PromiseIdCompletion> completion;
  };

  struct QueuedTaskSetErrorCount {
    uint32_t taskSetId;
    std::shared_ptr<UInt32Completion> completion;
  };

  struct QueuedTaskSetTakeLastError {
    uint32_t taskSetId;
    std::shared_ptr<OptionalStringCompletion> completion;
  };

  struct QueuedConnectionWhenWriteDisconnectedStart {
    uint32_t connectionId;
    std::shared_ptr<PromiseIdCompletion> completion;
  };

  struct QueuedConnectionAbortRead {
    uint32_t connectionId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedConnectionAbortWrite {
    uint32_t connectionId;
    std::string reason;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedNewTwoWayPipe {
    std::shared_ptr<HandlePairCompletion> completion;
  };

  struct QueuedDatagramBind {
    std::string address;
    uint32_t portHint;
    std::shared_ptr<HandleCompletion> completion;
  };

  struct QueuedDatagramReleasePort {
    uint32_t portId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedDatagramGetPort {
    uint32_t portId;
    std::shared_ptr<HandleCompletion> completion;
  };

  struct QueuedDatagramSend {
    uint32_t portId;
    std::string address;
    uint32_t portHint;
    std::vector<uint8_t> bytes;
    std::shared_ptr<UInt32Completion> completion;
  };

  struct QueuedDatagramReceive {
    uint32_t portId;
    uint32_t maxBytes;
    std::shared_ptr<DatagramReceiveCompletion> completion;
  };

  using QueuedOperation =
      std::variant<QueuedSleepNanos, QueuedAwaitPromise, QueuedCancelPromise,
                   QueuedReleasePromise, QueuedListen, QueuedReleaseListener, QueuedAccept,
                   QueuedAcceptStart, QueuedConnect, QueuedConnectStart,
                   QueuedAwaitConnectionPromise, QueuedCancelConnectionPromise,
                   QueuedReleaseConnectionPromise, QueuedReleaseConnection,
                   QueuedConnectionWrite, QueuedConnectionRead, QueuedConnectionShutdownWrite,
                   QueuedPromiseAllStart, QueuedPromiseRaceStart, QueuedTaskSetNew,
                   QueuedTaskSetRelease, QueuedTaskSetAddPromise, QueuedTaskSetClear,
                   QueuedTaskSetIsEmpty, QueuedTaskSetOnEmptyStart, QueuedTaskSetErrorCount,
                   QueuedTaskSetTakeLastError, QueuedConnectionWhenWriteDisconnectedStart,
                   QueuedConnectionAbortRead, QueuedConnectionAbortWrite, QueuedNewTwoWayPipe,
                   QueuedDatagramBind, QueuedDatagramReleasePort, QueuedDatagramGetPort,
                   QueuedDatagramSend, QueuedDatagramReceive>;

  uint32_t addPromise(PendingPromise&& promise) {
    uint32_t promiseId = nextPromiseId_++;
    while (promises_.find(promiseId) != promises_.end()) {
      promiseId = nextPromiseId_++;
    }
    promises_.emplace(promiseId, std::move(promise));
    return promiseId;
  }

  uint32_t addConnectionPromise(PendingConnectionPromise&& promise) {
    uint32_t promiseId = nextConnectionPromiseId_++;
    while (connectionPromises_.find(promiseId) != connectionPromises_.end()) {
      promiseId = nextConnectionPromiseId_++;
    }
    connectionPromises_.emplace(promiseId, std::move(promise));
    return promiseId;
  }

  uint32_t addListener(kj::Own<kj::ConnectionReceiver>&& listener) {
    uint32_t listenerId = nextListenerId_++;
    while (listeners_.find(listenerId) != listeners_.end()) {
      listenerId = nextListenerId_++;
    }
    listeners_.emplace(listenerId, kj::mv(listener));
    return listenerId;
  }

  uint32_t addConnection(kj::Own<kj::AsyncIoStream>&& stream) {
    uint32_t connectionId = nextConnectionId_++;
    while (connections_.find(connectionId) != connections_.end()) {
      connectionId = nextConnectionId_++;
    }
    connections_.emplace(connectionId, kj::mv(stream));
    return connectionId;
  }

  uint32_t addTaskSet(kj::Own<RuntimeTaskSet>&& taskSet) {
    uint32_t taskSetId = nextTaskSetId_++;
    while (taskSets_.find(taskSetId) != taskSets_.end()) {
      taskSetId = nextTaskSetId_++;
    }
    taskSets_.emplace(taskSetId, kj::mv(taskSet));
    return taskSetId;
  }

  uint32_t addDatagramPort(kj::Own<kj::DatagramPort>&& port) {
    uint32_t portId = nextDatagramPortId_++;
    while (datagramPorts_.find(portId) != datagramPorts_.end()) {
      portId = nextDatagramPortId_++;
    }
    datagramPorts_.emplace(portId, kj::mv(port));
    return portId;
  }

  uint32_t promiseAllStart(std::vector<uint32_t> promiseIds) {
    auto promises = kj::heapArrayBuilder<kj::Promise<void>>(promiseIds.size());
    for (auto promiseId : promiseIds) {
      auto it = promises_.find(promiseId);
      if (it == promises_.end()) {
        throw std::runtime_error("unknown KJ promise id: " + std::to_string(promiseId));
      }
      auto pending = kj::mv(it->second);
      promises_.erase(it);
      pending.canceler->release();
      promises.add(kj::mv(pending.promise));
    }
    auto canceler = kj::heap<kj::Canceler>();
    auto promise = canceler->wrap(kj::joinPromises(promises.finish()));
    return addPromise(PendingPromise(kj::mv(promise), kj::mv(canceler)));
  }

  uint32_t promiseRaceStart(std::vector<uint32_t> promiseIds) {
    if (promiseIds.empty()) {
      throw std::runtime_error("promiseRaceStart requires at least one promise id");
    }

    auto firstIt = promises_.find(promiseIds[0]);
    if (firstIt == promises_.end()) {
      throw std::runtime_error("unknown KJ promise id: " + std::to_string(promiseIds[0]));
    }
    auto firstPending = kj::mv(firstIt->second);
    promises_.erase(firstIt);
    firstPending.canceler->release();
    auto raced = kj::mv(firstPending.promise);

    for (size_t i = 1; i < promiseIds.size(); ++i) {
      auto promiseId = promiseIds[i];
      auto it = promises_.find(promiseId);
      if (it == promises_.end()) {
        throw std::runtime_error("unknown KJ promise id: " + std::to_string(promiseId));
      }
      auto pending = kj::mv(it->second);
      promises_.erase(it);
      pending.canceler->release();
      raced = kj::mv(raced).exclusiveJoin(kj::mv(pending.promise));
    }

    auto canceler = kj::heap<kj::Canceler>();
    auto promise = canceler->wrap(kj::mv(raced));
    return addPromise(PendingPromise(kj::mv(promise), kj::mv(canceler)));
  }

  uint32_t taskSetNew() { return addTaskSet(kj::heap<RuntimeTaskSet>()); }

  void taskSetRelease(uint32_t taskSetId) {
    auto erased = taskSets_.erase(taskSetId);
    if (erased == 0) {
      throw std::runtime_error("unknown KJ task set id: " + std::to_string(taskSetId));
    }
  }

  void taskSetAddPromise(uint32_t taskSetId, uint32_t promiseId) {
    auto taskSetIt = taskSets_.find(taskSetId);
    if (taskSetIt == taskSets_.end()) {
      throw std::runtime_error("unknown KJ task set id: " + std::to_string(taskSetId));
    }
    auto promiseIt = promises_.find(promiseId);
    if (promiseIt == promises_.end()) {
      throw std::runtime_error("unknown KJ promise id: " + std::to_string(promiseId));
    }
    auto pending = kj::mv(promiseIt->second);
    promises_.erase(promiseIt);
    pending.canceler->release();
    taskSetIt->second->tasks.add(kj::mv(pending.promise));
  }

  void taskSetClear(uint32_t taskSetId) {
    auto taskSetIt = taskSets_.find(taskSetId);
    if (taskSetIt == taskSets_.end()) {
      throw std::runtime_error("unknown KJ task set id: " + std::to_string(taskSetId));
    }
    taskSetIt->second->tasks.clear();
  }

  bool taskSetIsEmpty(uint32_t taskSetId) {
    auto taskSetIt = taskSets_.find(taskSetId);
    if (taskSetIt == taskSets_.end()) {
      throw std::runtime_error("unknown KJ task set id: " + std::to_string(taskSetId));
    }
    return taskSetIt->second->tasks.isEmpty();
  }

  uint32_t taskSetOnEmptyStart(uint32_t taskSetId) {
    auto taskSetIt = taskSets_.find(taskSetId);
    if (taskSetIt == taskSets_.end()) {
      throw std::runtime_error("unknown KJ task set id: " + std::to_string(taskSetId));
    }
    auto canceler = kj::heap<kj::Canceler>();
    auto promise = canceler->wrap(taskSetIt->second->tasks.onEmpty());
    return addPromise(PendingPromise(kj::mv(promise), kj::mv(canceler)));
  }

  uint32_t taskSetErrorCount(uint32_t taskSetId) {
    auto taskSetIt = taskSets_.find(taskSetId);
    if (taskSetIt == taskSets_.end()) {
      throw std::runtime_error("unknown KJ task set id: " + std::to_string(taskSetId));
    }
    std::lock_guard<std::mutex> lock(taskSetIt->second->mutex);
    return taskSetIt->second->errorCount;
  }

  kj::Maybe<std::string> taskSetTakeLastError(uint32_t taskSetId) {
    auto taskSetIt = taskSets_.find(taskSetId);
    if (taskSetIt == taskSets_.end()) {
      throw std::runtime_error("unknown KJ task set id: " + std::to_string(taskSetId));
    }
    std::lock_guard<std::mutex> lock(taskSetIt->second->mutex);
    if (taskSetIt->second->lastError.empty()) {
      return kj::none;
    }
    auto out = taskSetIt->second->lastError;
    taskSetIt->second->lastError.clear();
    return out;
  }

  void awaitPromise(kj::WaitScope& waitScope, uint32_t promiseId) {
    auto it = promises_.find(promiseId);
    if (it == promises_.end()) {
      throw std::runtime_error("unknown KJ promise id: " + std::to_string(promiseId));
    }

    auto pending = kj::mv(it->second);
    promises_.erase(it);
    kj::mv(pending.promise).wait(waitScope);
  }

  void cancelPromise(uint32_t promiseId) {
    auto it = promises_.find(promiseId);
    if (it == promises_.end()) {
      throw std::runtime_error("unknown KJ promise id: " + std::to_string(promiseId));
    }
    it->second.canceler->cancel("Capnp.KjAsync promise canceled from Lean");
  }

  void releasePromise(uint32_t promiseId) {
    auto it = promises_.find(promiseId);
    if (it == promises_.end()) {
      throw std::runtime_error("unknown KJ promise id: " + std::to_string(promiseId));
    }
    promises_.erase(it);
  }

  uint32_t awaitConnectionPromise(kj::WaitScope& waitScope, uint32_t promiseId) {
    auto it = connectionPromises_.find(promiseId);
    if (it == connectionPromises_.end()) {
      throw std::runtime_error("unknown KJ connection promise id: " + std::to_string(promiseId));
    }

    auto pending = kj::mv(it->second);
    connectionPromises_.erase(it);
    auto stream = kj::mv(pending.promise).wait(waitScope);
    return addConnection(kj::mv(stream));
  }

  void cancelConnectionPromise(uint32_t promiseId) {
    auto it = connectionPromises_.find(promiseId);
    if (it == connectionPromises_.end()) {
      throw std::runtime_error("unknown KJ connection promise id: " + std::to_string(promiseId));
    }
    it->second.canceler->cancel("Capnp.KjAsync connection promise canceled from Lean");
  }

  void releaseConnectionPromise(uint32_t promiseId) {
    auto it = connectionPromises_.find(promiseId);
    if (it == connectionPromises_.end()) {
      throw std::runtime_error("unknown KJ connection promise id: " + std::to_string(promiseId));
    }
    connectionPromises_.erase(it);
  }

  uint32_t listen(kj::AsyncIoProvider& ioProvider, kj::WaitScope& waitScope,
                  const std::string& address, uint32_t portHint) {
    auto addr = ioProvider.getNetwork().parseAddress(address.c_str(), portHint).wait(waitScope);
    return addListener(addr->listen());
  }

  void releaseListener(uint32_t listenerId) {
    auto it = listeners_.find(listenerId);
    if (it == listeners_.end()) {
      throw std::runtime_error("unknown KJ listener id: " + std::to_string(listenerId));
    }
    listeners_.erase(it);
  }

  uint32_t accept(kj::WaitScope& waitScope, uint32_t listenerId) {
    auto it = listeners_.find(listenerId);
    if (it == listeners_.end()) {
      throw std::runtime_error("unknown KJ listener id: " + std::to_string(listenerId));
    }
    auto stream = it->second->accept().wait(waitScope).downcast<kj::AsyncIoStream>();
    return addConnection(kj::mv(stream));
  }

  uint32_t acceptStart(uint32_t listenerId) {
    auto it = listeners_.find(listenerId);
    if (it == listeners_.end()) {
      throw std::runtime_error("unknown KJ listener id: " + std::to_string(listenerId));
    }

    auto canceler = kj::heap<kj::Canceler>();
    auto promise = canceler->wrap(it->second->accept());
    return addConnectionPromise(PendingConnectionPromise(kj::mv(promise), kj::mv(canceler)));
  }

  uint32_t connect(kj::AsyncIoProvider& ioProvider, kj::WaitScope& waitScope,
                   const std::string& address, uint32_t portHint) {
    auto addr = ioProvider.getNetwork().parseAddress(address.c_str(), portHint).wait(waitScope);
    auto stream = addr->connect().wait(waitScope).downcast<kj::AsyncIoStream>();
    return addConnection(kj::mv(stream));
  }

  uint32_t connectStart(kj::AsyncIoProvider& ioProvider, const std::string& address,
                        uint32_t portHint) {
    auto canceler = kj::heap<kj::Canceler>();
    auto promise = canceler->wrap(
        ioProvider.getNetwork()
            .parseAddress(address.c_str(), portHint)
            .then([](kj::Own<kj::NetworkAddress>&& addr) { return addr->connect(); }));
    return addConnectionPromise(PendingConnectionPromise(kj::mv(promise), kj::mv(canceler)));
  }

  void releaseConnection(uint32_t connectionId) {
    auto it = connections_.find(connectionId);
    if (it == connections_.end()) {
      throw std::runtime_error("unknown KJ connection id: " + std::to_string(connectionId));
    }
    connections_.erase(it);
  }

  void connectionWrite(kj::WaitScope& waitScope, uint32_t connectionId,
                       const std::vector<uint8_t>& bytes) {
    auto it = connections_.find(connectionId);
    if (it == connections_.end()) {
      throw std::runtime_error("unknown KJ connection id: " + std::to_string(connectionId));
    }
    if (bytes.empty()) {
      return;
    }
    auto ptr = kj::ArrayPtr<const kj::byte>(reinterpret_cast<const kj::byte*>(bytes.data()),
                                            bytes.size());
    it->second->write(ptr).wait(waitScope);
  }

  std::vector<uint8_t> connectionRead(kj::WaitScope& waitScope, uint32_t connectionId,
                                      uint32_t minBytes, uint32_t maxBytes) {
    if (minBytes > maxBytes) {
      throw std::runtime_error("connection read requires minBytes <= maxBytes");
    }
    if (maxBytes == 0) {
      return {};
    }

    auto it = connections_.find(connectionId);
    if (it == connections_.end()) {
      throw std::runtime_error("unknown KJ connection id: " + std::to_string(connectionId));
    }

    std::vector<uint8_t> bytes(maxBytes);
    auto readCount = it->second
                         ->tryRead(bytes.data(), static_cast<size_t>(minBytes),
                                   static_cast<size_t>(maxBytes))
                         .wait(waitScope);
    bytes.resize(readCount);
    return bytes;
  }

  void connectionShutdownWrite(uint32_t connectionId) {
    auto it = connections_.find(connectionId);
    if (it == connections_.end()) {
      throw std::runtime_error("unknown KJ connection id: " + std::to_string(connectionId));
    }
    it->second->shutdownWrite();
  }

  uint32_t connectionWhenWriteDisconnectedStart(uint32_t connectionId) {
    auto it = connections_.find(connectionId);
    if (it == connections_.end()) {
      throw std::runtime_error("unknown KJ connection id: " + std::to_string(connectionId));
    }
    auto canceler = kj::heap<kj::Canceler>();
    auto promise = canceler->wrap(it->second->whenWriteDisconnected());
    return addPromise(PendingPromise(kj::mv(promise), kj::mv(canceler)));
  }

  void connectionAbortRead(uint32_t connectionId) {
    auto it = connections_.find(connectionId);
    if (it == connections_.end()) {
      throw std::runtime_error("unknown KJ connection id: " + std::to_string(connectionId));
    }
    it->second->abortRead();
  }

  void connectionAbortWrite(uint32_t connectionId, std::string reason) {
    auto it = connections_.find(connectionId);
    if (it == connections_.end()) {
      throw std::runtime_error("unknown KJ connection id: " + std::to_string(connectionId));
    }
    it->second->abortWrite(kj::Exception(
        kj::Exception::Type::FAILED, __FILE__, __LINE__, kj::str(reason.c_str())));
  }

  std::pair<uint32_t, uint32_t> newTwoWayPipe(kj::AsyncIoProvider& ioProvider) {
    auto pipe = ioProvider.newTwoWayPipe();
    auto first = addConnection(kj::mv(pipe.ends[0]));
    auto second = addConnection(kj::mv(pipe.ends[1]));
    return {first, second};
  }

  uint32_t datagramBind(kj::AsyncIoProvider& ioProvider, kj::WaitScope& waitScope,
                        const std::string& address, uint32_t portHint) {
    auto addr = ioProvider.getNetwork().parseAddress(address.c_str(), portHint).wait(waitScope);
    auto port = addr->bindDatagramPort();
    return addDatagramPort(kj::mv(port));
  }

  void datagramReleasePort(uint32_t portId) {
    auto erased = datagramPorts_.erase(portId);
    if (erased == 0) {
      throw std::runtime_error("unknown KJ datagram port id: " + std::to_string(portId));
    }
  }

  uint32_t datagramGetPort(uint32_t portId) {
    auto it = datagramPorts_.find(portId);
    if (it == datagramPorts_.end()) {
      throw std::runtime_error("unknown KJ datagram port id: " + std::to_string(portId));
    }
    return it->second->getPort();
  }

  uint32_t datagramSend(kj::AsyncIoProvider& ioProvider, kj::WaitScope& waitScope, uint32_t portId,
                        const std::string& address, uint32_t portHint,
                        const std::vector<uint8_t>& bytes) {
    auto it = datagramPorts_.find(portId);
    if (it == datagramPorts_.end()) {
      throw std::runtime_error("unknown KJ datagram port id: " + std::to_string(portId));
    }
    auto addr = ioProvider.getNetwork().parseAddress(address.c_str(), portHint).wait(waitScope);
    auto ptr = kj::ArrayPtr<const kj::byte>(reinterpret_cast<const kj::byte*>(bytes.data()),
                                            bytes.size());
    auto sent = it->second->send(ptr, *addr).wait(waitScope);
    if (sent > static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
      throw std::runtime_error("datagram send byte count exceeds UInt32 range");
    }
    return static_cast<uint32_t>(sent);
  }

  std::pair<std::string, std::vector<uint8_t>> datagramReceive(kj::WaitScope& waitScope,
                                                                uint32_t portId,
                                                                uint32_t maxBytes) {
    if (maxBytes == 0) {
      throw std::runtime_error("datagram receive requires maxBytes > 0");
    }
    auto it = datagramPorts_.find(portId);
    if (it == datagramPorts_.end()) {
      throw std::runtime_error("unknown KJ datagram port id: " + std::to_string(portId));
    }

    kj::DatagramReceiver::Capacity capacity;
    capacity.content = maxBytes;
    capacity.ancillary = 0;
    auto receiver = it->second->makeReceiver(capacity);
    receiver->receive().wait(waitScope);
    auto content = receiver->getContent();
    auto contentPtr = content.value;
    std::vector<uint8_t> bytes(contentPtr.size());
    if (!bytes.empty()) {
      std::memcpy(bytes.data(), contentPtr.begin(), bytes.size());
    }

    auto source = receiver->getSource().toString();
    std::string sourceAddress(source.cStr());
    return {std::move(sourceAddress), std::move(bytes)};
  }

  void failOutstanding(const std::string& message) {
    std::deque<QueuedOperation> queued;
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      queued.swap(queue_);
    }

    for (auto& op : queued) {
      if (std::holds_alternative<QueuedSleepNanos>(op)) {
        completePromiseIdFailure(std::get<QueuedSleepNanos>(op).completion, message);
      } else if (std::holds_alternative<QueuedAwaitPromise>(op)) {
        completeUnitFailure(std::get<QueuedAwaitPromise>(op).completion, message);
      } else if (std::holds_alternative<QueuedCancelPromise>(op)) {
        completeUnitFailure(std::get<QueuedCancelPromise>(op).completion, message);
      } else if (std::holds_alternative<QueuedReleasePromise>(op)) {
        completeUnitFailure(std::get<QueuedReleasePromise>(op).completion, message);
      } else if (std::holds_alternative<QueuedListen>(op)) {
        completeHandleFailure(std::get<QueuedListen>(op).completion, message);
      } else if (std::holds_alternative<QueuedReleaseListener>(op)) {
        completeUnitFailure(std::get<QueuedReleaseListener>(op).completion, message);
      } else if (std::holds_alternative<QueuedAccept>(op)) {
        completeHandleFailure(std::get<QueuedAccept>(op).completion, message);
      } else if (std::holds_alternative<QueuedAcceptStart>(op)) {
        completePromiseIdFailure(std::get<QueuedAcceptStart>(op).completion, message);
      } else if (std::holds_alternative<QueuedConnect>(op)) {
        completeHandleFailure(std::get<QueuedConnect>(op).completion, message);
      } else if (std::holds_alternative<QueuedConnectStart>(op)) {
        completePromiseIdFailure(std::get<QueuedConnectStart>(op).completion, message);
      } else if (std::holds_alternative<QueuedAwaitConnectionPromise>(op)) {
        completeHandleFailure(std::get<QueuedAwaitConnectionPromise>(op).completion, message);
      } else if (std::holds_alternative<QueuedCancelConnectionPromise>(op)) {
        completeUnitFailure(std::get<QueuedCancelConnectionPromise>(op).completion, message);
      } else if (std::holds_alternative<QueuedReleaseConnectionPromise>(op)) {
        completeUnitFailure(std::get<QueuedReleaseConnectionPromise>(op).completion, message);
      } else if (std::holds_alternative<QueuedReleaseConnection>(op)) {
        completeUnitFailure(std::get<QueuedReleaseConnection>(op).completion, message);
      } else if (std::holds_alternative<QueuedConnectionWrite>(op)) {
        completeUnitFailure(std::get<QueuedConnectionWrite>(op).completion, message);
      } else if (std::holds_alternative<QueuedConnectionRead>(op)) {
        completeBytesFailure(std::get<QueuedConnectionRead>(op).completion, message);
      } else if (std::holds_alternative<QueuedConnectionShutdownWrite>(op)) {
        completeUnitFailure(std::get<QueuedConnectionShutdownWrite>(op).completion, message);
      } else if (std::holds_alternative<QueuedPromiseAllStart>(op)) {
        completePromiseIdFailure(std::get<QueuedPromiseAllStart>(op).completion, message);
      } else if (std::holds_alternative<QueuedPromiseRaceStart>(op)) {
        completePromiseIdFailure(std::get<QueuedPromiseRaceStart>(op).completion, message);
      } else if (std::holds_alternative<QueuedTaskSetNew>(op)) {
        completeHandleFailure(std::get<QueuedTaskSetNew>(op).completion, message);
      } else if (std::holds_alternative<QueuedTaskSetRelease>(op)) {
        completeUnitFailure(std::get<QueuedTaskSetRelease>(op).completion, message);
      } else if (std::holds_alternative<QueuedTaskSetAddPromise>(op)) {
        completeUnitFailure(std::get<QueuedTaskSetAddPromise>(op).completion, message);
      } else if (std::holds_alternative<QueuedTaskSetClear>(op)) {
        completeUnitFailure(std::get<QueuedTaskSetClear>(op).completion, message);
      } else if (std::holds_alternative<QueuedTaskSetIsEmpty>(op)) {
        completeBoolFailure(std::get<QueuedTaskSetIsEmpty>(op).completion, message);
      } else if (std::holds_alternative<QueuedTaskSetOnEmptyStart>(op)) {
        completePromiseIdFailure(std::get<QueuedTaskSetOnEmptyStart>(op).completion, message);
      } else if (std::holds_alternative<QueuedTaskSetErrorCount>(op)) {
        completeUInt32Failure(std::get<QueuedTaskSetErrorCount>(op).completion, message);
      } else if (std::holds_alternative<QueuedTaskSetTakeLastError>(op)) {
        completeOptionalStringFailure(std::get<QueuedTaskSetTakeLastError>(op).completion, message);
      } else if (std::holds_alternative<QueuedConnectionWhenWriteDisconnectedStart>(op)) {
        completePromiseIdFailure(
            std::get<QueuedConnectionWhenWriteDisconnectedStart>(op).completion, message);
      } else if (std::holds_alternative<QueuedConnectionAbortRead>(op)) {
        completeUnitFailure(std::get<QueuedConnectionAbortRead>(op).completion, message);
      } else if (std::holds_alternative<QueuedConnectionAbortWrite>(op)) {
        completeUnitFailure(std::get<QueuedConnectionAbortWrite>(op).completion, message);
      } else if (std::holds_alternative<QueuedNewTwoWayPipe>(op)) {
        completeHandlePairFailure(std::get<QueuedNewTwoWayPipe>(op).completion, message);
      } else if (std::holds_alternative<QueuedDatagramBind>(op)) {
        completeHandleFailure(std::get<QueuedDatagramBind>(op).completion, message);
      } else if (std::holds_alternative<QueuedDatagramReleasePort>(op)) {
        completeUnitFailure(std::get<QueuedDatagramReleasePort>(op).completion, message);
      } else if (std::holds_alternative<QueuedDatagramGetPort>(op)) {
        completeHandleFailure(std::get<QueuedDatagramGetPort>(op).completion, message);
      } else if (std::holds_alternative<QueuedDatagramSend>(op)) {
        completeUInt32Failure(std::get<QueuedDatagramSend>(op).completion, message);
      } else if (std::holds_alternative<QueuedDatagramReceive>(op)) {
        completeDatagramReceiveFailure(std::get<QueuedDatagramReceive>(op).completion, message);
      }
    }
  }

  void run() {
    try {
      auto io = kj::setupAsyncIo();
      {
        std::lock_guard<std::mutex> lock(startupMutex_);
        startupComplete_ = true;
      }
      alive_.store(true, std::memory_order_release);
      startupCv_.notify_one();

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

        if (std::holds_alternative<QueuedSleepNanos>(op)) {
          auto sleep = std::get<QueuedSleepNanos>(std::move(op));
          try {
            uint64_t clamped = std::min<uint64_t>(
                sleep.delayNanos, static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
            auto delay = static_cast<int64_t>(clamped) * kj::NANOSECONDS;
            auto canceler = kj::heap<kj::Canceler>();
            auto promise = canceler->wrap(io.provider->getTimer().afterDelay(delay));
            auto promiseId = addPromise(PendingPromise(kj::mv(promise), kj::mv(canceler)));
            completePromiseIdSuccess(sleep.completion, promiseId);
          } catch (const kj::Exception& e) {
            completePromiseIdFailure(sleep.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completePromiseIdFailure(sleep.completion, e.what());
          } catch (...) {
            completePromiseIdFailure(sleep.completion,
                                     "unknown exception in Capnp.KjAsync sleep");
          }
        } else if (std::holds_alternative<QueuedAwaitPromise>(op)) {
          auto await = std::get<QueuedAwaitPromise>(std::move(op));
          try {
            awaitPromise(io.waitScope, await.promiseId);
            completeUnitSuccess(await.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(await.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(await.completion, e.what());
          } catch (...) {
            completeUnitFailure(await.completion,
                                "unknown exception in Capnp.KjAsync promise await");
          }
        } else if (std::holds_alternative<QueuedCancelPromise>(op)) {
          auto cancel = std::get<QueuedCancelPromise>(std::move(op));
          try {
            cancelPromise(cancel.promiseId);
            completeUnitSuccess(cancel.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(cancel.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(cancel.completion, e.what());
          } catch (...) {
            completeUnitFailure(cancel.completion,
                                "unknown exception in Capnp.KjAsync promise cancel");
          }
        } else if (std::holds_alternative<QueuedReleasePromise>(op)) {
          auto release = std::get<QueuedReleasePromise>(std::move(op));
          try {
            releasePromise(release.promiseId);
            completeUnitSuccess(release.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(release.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(release.completion, e.what());
          } catch (...) {
            completeUnitFailure(release.completion,
                                "unknown exception in Capnp.KjAsync promise release");
          }
        } else if (std::holds_alternative<QueuedListen>(op)) {
          auto listenReq = std::get<QueuedListen>(std::move(op));
          try {
            auto listenerId = listen(*io.provider, io.waitScope, listenReq.address,
                                     listenReq.portHint);
            completeHandleSuccess(listenReq.completion, listenerId);
          } catch (const kj::Exception& e) {
            completeHandleFailure(listenReq.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeHandleFailure(listenReq.completion, e.what());
          } catch (...) {
            completeHandleFailure(listenReq.completion,
                                  "unknown exception in Capnp.KjAsync listen");
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
                                "unknown exception in Capnp.KjAsync release listener");
          }
        } else if (std::holds_alternative<QueuedAccept>(op)) {
          auto acceptReq = std::get<QueuedAccept>(std::move(op));
          try {
            auto connectionId = accept(io.waitScope, acceptReq.listenerId);
            completeHandleSuccess(acceptReq.completion, connectionId);
          } catch (const kj::Exception& e) {
            completeHandleFailure(acceptReq.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeHandleFailure(acceptReq.completion, e.what());
          } catch (...) {
            completeHandleFailure(acceptReq.completion,
                                  "unknown exception in Capnp.KjAsync accept");
          }
        } else if (std::holds_alternative<QueuedAcceptStart>(op)) {
          auto acceptReq = std::get<QueuedAcceptStart>(std::move(op));
          try {
            auto promiseId = acceptStart(acceptReq.listenerId);
            completePromiseIdSuccess(acceptReq.completion, promiseId);
          } catch (const kj::Exception& e) {
            completePromiseIdFailure(acceptReq.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completePromiseIdFailure(acceptReq.completion, e.what());
          } catch (...) {
            completePromiseIdFailure(acceptReq.completion,
                                     "unknown exception in Capnp.KjAsync acceptStart");
          }
        } else if (std::holds_alternative<QueuedConnect>(op)) {
          auto connectReq = std::get<QueuedConnect>(std::move(op));
          try {
            auto connectionId =
                connect(*io.provider, io.waitScope, connectReq.address, connectReq.portHint);
            completeHandleSuccess(connectReq.completion, connectionId);
          } catch (const kj::Exception& e) {
            completeHandleFailure(connectReq.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeHandleFailure(connectReq.completion, e.what());
          } catch (...) {
            completeHandleFailure(connectReq.completion,
                                  "unknown exception in Capnp.KjAsync connect");
          }
        } else if (std::holds_alternative<QueuedConnectStart>(op)) {
          auto connectReq = std::get<QueuedConnectStart>(std::move(op));
          try {
            auto promiseId = connectStart(*io.provider, connectReq.address, connectReq.portHint);
            completePromiseIdSuccess(connectReq.completion, promiseId);
          } catch (const kj::Exception& e) {
            completePromiseIdFailure(connectReq.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completePromiseIdFailure(connectReq.completion, e.what());
          } catch (...) {
            completePromiseIdFailure(connectReq.completion,
                                     "unknown exception in Capnp.KjAsync connectStart");
          }
        } else if (std::holds_alternative<QueuedAwaitConnectionPromise>(op)) {
          auto await = std::get<QueuedAwaitConnectionPromise>(std::move(op));
          try {
            auto connectionId = awaitConnectionPromise(io.waitScope, await.promiseId);
            completeHandleSuccess(await.completion, connectionId);
          } catch (const kj::Exception& e) {
            completeHandleFailure(await.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeHandleFailure(await.completion, e.what());
          } catch (...) {
            completeHandleFailure(
                await.completion,
                "unknown exception in Capnp.KjAsync connection promise await");
          }
        } else if (std::holds_alternative<QueuedCancelConnectionPromise>(op)) {
          auto cancel = std::get<QueuedCancelConnectionPromise>(std::move(op));
          try {
            cancelConnectionPromise(cancel.promiseId);
            completeUnitSuccess(cancel.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(cancel.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(cancel.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                cancel.completion,
                "unknown exception in Capnp.KjAsync connection promise cancel");
          }
        } else if (std::holds_alternative<QueuedReleaseConnectionPromise>(op)) {
          auto release = std::get<QueuedReleaseConnectionPromise>(std::move(op));
          try {
            releaseConnectionPromise(release.promiseId);
            completeUnitSuccess(release.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(release.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(release.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                release.completion,
                "unknown exception in Capnp.KjAsync connection promise release");
          }
        } else if (std::holds_alternative<QueuedReleaseConnection>(op)) {
          auto release = std::get<QueuedReleaseConnection>(std::move(op));
          try {
            releaseConnection(release.connectionId);
            completeUnitSuccess(release.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(release.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(release.completion, e.what());
          } catch (...) {
            completeUnitFailure(release.completion,
                                "unknown exception in Capnp.KjAsync release connection");
          }
        } else if (std::holds_alternative<QueuedConnectionWrite>(op)) {
          auto writeReq = std::get<QueuedConnectionWrite>(std::move(op));
          try {
            connectionWrite(io.waitScope, writeReq.connectionId, writeReq.bytes);
            completeUnitSuccess(writeReq.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(writeReq.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(writeReq.completion, e.what());
          } catch (...) {
            completeUnitFailure(writeReq.completion,
                                "unknown exception in Capnp.KjAsync connection write");
          }
        } else if (std::holds_alternative<QueuedConnectionRead>(op)) {
          auto readReq = std::get<QueuedConnectionRead>(std::move(op));
          try {
            auto bytes = connectionRead(io.waitScope, readReq.connectionId, readReq.minBytes,
                                        readReq.maxBytes);
            completeBytesSuccess(readReq.completion, std::move(bytes));
          } catch (const kj::Exception& e) {
            completeBytesFailure(readReq.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeBytesFailure(readReq.completion, e.what());
          } catch (...) {
            completeBytesFailure(readReq.completion,
                                 "unknown exception in Capnp.KjAsync connection read");
          }
        } else if (std::holds_alternative<QueuedConnectionShutdownWrite>(op)) {
          auto shutdown = std::get<QueuedConnectionShutdownWrite>(std::move(op));
          try {
            connectionShutdownWrite(shutdown.connectionId);
            completeUnitSuccess(shutdown.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(shutdown.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(shutdown.completion, e.what());
          } catch (...) {
            completeUnitFailure(shutdown.completion,
                                "unknown exception in Capnp.KjAsync connection shutdownWrite");
          }
        } else if (std::holds_alternative<QueuedPromiseAllStart>(op)) {
          auto compose = std::get<QueuedPromiseAllStart>(std::move(op));
          try {
            auto promiseId = promiseAllStart(std::move(compose.promiseIds));
            completePromiseIdSuccess(compose.completion, promiseId);
          } catch (const kj::Exception& e) {
            completePromiseIdFailure(compose.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completePromiseIdFailure(compose.completion, e.what());
          } catch (...) {
            completePromiseIdFailure(compose.completion,
                                     "unknown exception in Capnp.KjAsync promiseAllStart");
          }
        } else if (std::holds_alternative<QueuedPromiseRaceStart>(op)) {
          auto compose = std::get<QueuedPromiseRaceStart>(std::move(op));
          try {
            auto promiseId = promiseRaceStart(std::move(compose.promiseIds));
            completePromiseIdSuccess(compose.completion, promiseId);
          } catch (const kj::Exception& e) {
            completePromiseIdFailure(compose.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completePromiseIdFailure(compose.completion, e.what());
          } catch (...) {
            completePromiseIdFailure(compose.completion,
                                     "unknown exception in Capnp.KjAsync promiseRaceStart");
          }
        } else if (std::holds_alternative<QueuedTaskSetNew>(op)) {
          auto create = std::get<QueuedTaskSetNew>(std::move(op));
          try {
            auto taskSetId = taskSetNew();
            completeHandleSuccess(create.completion, taskSetId);
          } catch (const kj::Exception& e) {
            completeHandleFailure(create.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeHandleFailure(create.completion, e.what());
          } catch (...) {
            completeHandleFailure(create.completion,
                                  "unknown exception in Capnp.KjAsync taskSetNew");
          }
        } else if (std::holds_alternative<QueuedTaskSetRelease>(op)) {
          auto release = std::get<QueuedTaskSetRelease>(std::move(op));
          try {
            taskSetRelease(release.taskSetId);
            completeUnitSuccess(release.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(release.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(release.completion, e.what());
          } catch (...) {
            completeUnitFailure(release.completion,
                                "unknown exception in Capnp.KjAsync taskSetRelease");
          }
        } else if (std::holds_alternative<QueuedTaskSetAddPromise>(op)) {
          auto add = std::get<QueuedTaskSetAddPromise>(std::move(op));
          try {
            taskSetAddPromise(add.taskSetId, add.promiseId);
            completeUnitSuccess(add.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(add.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(add.completion, e.what());
          } catch (...) {
            completeUnitFailure(add.completion,
                                "unknown exception in Capnp.KjAsync taskSetAddPromise");
          }
        } else if (std::holds_alternative<QueuedTaskSetClear>(op)) {
          auto clear = std::get<QueuedTaskSetClear>(std::move(op));
          try {
            taskSetClear(clear.taskSetId);
            completeUnitSuccess(clear.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(clear.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(clear.completion, e.what());
          } catch (...) {
            completeUnitFailure(clear.completion,
                                "unknown exception in Capnp.KjAsync taskSetClear");
          }
        } else if (std::holds_alternative<QueuedTaskSetIsEmpty>(op)) {
          auto query = std::get<QueuedTaskSetIsEmpty>(std::move(op));
          try {
            completeBoolSuccess(query.completion, taskSetIsEmpty(query.taskSetId));
          } catch (const kj::Exception& e) {
            completeBoolFailure(query.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeBoolFailure(query.completion, e.what());
          } catch (...) {
            completeBoolFailure(query.completion,
                                "unknown exception in Capnp.KjAsync taskSetIsEmpty");
          }
        } else if (std::holds_alternative<QueuedTaskSetOnEmptyStart>(op)) {
          auto start = std::get<QueuedTaskSetOnEmptyStart>(std::move(op));
          try {
            auto promiseId = taskSetOnEmptyStart(start.taskSetId);
            completePromiseIdSuccess(start.completion, promiseId);
          } catch (const kj::Exception& e) {
            completePromiseIdFailure(start.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completePromiseIdFailure(start.completion, e.what());
          } catch (...) {
            completePromiseIdFailure(start.completion,
                                     "unknown exception in Capnp.KjAsync taskSetOnEmptyStart");
          }
        } else if (std::holds_alternative<QueuedTaskSetErrorCount>(op)) {
          auto query = std::get<QueuedTaskSetErrorCount>(std::move(op));
          try {
            completeUInt32Success(query.completion, taskSetErrorCount(query.taskSetId));
          } catch (const kj::Exception& e) {
            completeUInt32Failure(query.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUInt32Failure(query.completion, e.what());
          } catch (...) {
            completeUInt32Failure(query.completion,
                                  "unknown exception in Capnp.KjAsync taskSetErrorCount");
          }
        } else if (std::holds_alternative<QueuedTaskSetTakeLastError>(op)) {
          auto query = std::get<QueuedTaskSetTakeLastError>(std::move(op));
          try {
            completeOptionalStringSuccess(query.completion, taskSetTakeLastError(query.taskSetId));
          } catch (const kj::Exception& e) {
            completeOptionalStringFailure(query.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeOptionalStringFailure(query.completion, e.what());
          } catch (...) {
            completeOptionalStringFailure(
                query.completion, "unknown exception in Capnp.KjAsync taskSetTakeLastError");
          }
        } else if (std::holds_alternative<QueuedConnectionWhenWriteDisconnectedStart>(op)) {
          auto start = std::get<QueuedConnectionWhenWriteDisconnectedStart>(std::move(op));
          try {
            auto promiseId = connectionWhenWriteDisconnectedStart(start.connectionId);
            completePromiseIdSuccess(start.completion, promiseId);
          } catch (const kj::Exception& e) {
            completePromiseIdFailure(start.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completePromiseIdFailure(start.completion, e.what());
          } catch (...) {
            completePromiseIdFailure(
                start.completion,
                "unknown exception in Capnp.KjAsync connection whenWriteDisconnected start");
          }
        } else if (std::holds_alternative<QueuedConnectionAbortRead>(op)) {
          auto abort = std::get<QueuedConnectionAbortRead>(std::move(op));
          try {
            connectionAbortRead(abort.connectionId);
            completeUnitSuccess(abort.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(abort.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(abort.completion, e.what());
          } catch (...) {
            completeUnitFailure(abort.completion,
                                "unknown exception in Capnp.KjAsync connection abortRead");
          }
        } else if (std::holds_alternative<QueuedConnectionAbortWrite>(op)) {
          auto abort = std::get<QueuedConnectionAbortWrite>(std::move(op));
          try {
            connectionAbortWrite(abort.connectionId, std::move(abort.reason));
            completeUnitSuccess(abort.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(abort.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(abort.completion, e.what());
          } catch (...) {
            completeUnitFailure(abort.completion,
                                "unknown exception in Capnp.KjAsync connection abortWrite");
          }
        } else if (std::holds_alternative<QueuedNewTwoWayPipe>(op)) {
          auto create = std::get<QueuedNewTwoWayPipe>(std::move(op));
          try {
            auto pair = newTwoWayPipe(*io.provider);
            completeHandlePairSuccess(create.completion, pair.first, pair.second);
          } catch (const kj::Exception& e) {
            completeHandlePairFailure(create.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeHandlePairFailure(create.completion, e.what());
          } catch (...) {
            completeHandlePairFailure(create.completion,
                                      "unknown exception in Capnp.KjAsync newTwoWayPipe");
          }
        } else if (std::holds_alternative<QueuedDatagramBind>(op)) {
          auto bind = std::get<QueuedDatagramBind>(std::move(op));
          try {
            auto portId = datagramBind(*io.provider, io.waitScope, bind.address, bind.portHint);
            completeHandleSuccess(bind.completion, portId);
          } catch (const kj::Exception& e) {
            completeHandleFailure(bind.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeHandleFailure(bind.completion, e.what());
          } catch (...) {
            completeHandleFailure(bind.completion,
                                  "unknown exception in Capnp.KjAsync datagramBind");
          }
        } else if (std::holds_alternative<QueuedDatagramReleasePort>(op)) {
          auto release = std::get<QueuedDatagramReleasePort>(std::move(op));
          try {
            datagramReleasePort(release.portId);
            completeUnitSuccess(release.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(release.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(release.completion, e.what());
          } catch (...) {
            completeUnitFailure(release.completion,
                                "unknown exception in Capnp.KjAsync datagramReleasePort");
          }
        } else if (std::holds_alternative<QueuedDatagramGetPort>(op)) {
          auto query = std::get<QueuedDatagramGetPort>(std::move(op));
          try {
            completeHandleSuccess(query.completion, datagramGetPort(query.portId));
          } catch (const kj::Exception& e) {
            completeHandleFailure(query.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeHandleFailure(query.completion, e.what());
          } catch (...) {
            completeHandleFailure(query.completion,
                                  "unknown exception in Capnp.KjAsync datagramGetPort");
          }
        } else if (std::holds_alternative<QueuedDatagramSend>(op)) {
          auto send = std::get<QueuedDatagramSend>(std::move(op));
          try {
            auto sent = datagramSend(*io.provider, io.waitScope, send.portId, send.address,
                                     send.portHint, send.bytes);
            completeUInt32Success(send.completion, sent);
          } catch (const kj::Exception& e) {
            completeUInt32Failure(send.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUInt32Failure(send.completion, e.what());
          } catch (...) {
            completeUInt32Failure(send.completion,
                                  "unknown exception in Capnp.KjAsync datagramSend");
          }
        } else if (std::holds_alternative<QueuedDatagramReceive>(op)) {
          auto recv = std::get<QueuedDatagramReceive>(std::move(op));
          try {
            auto result = datagramReceive(io.waitScope, recv.portId, recv.maxBytes);
            completeDatagramReceiveSuccess(
                recv.completion, std::move(result.first), std::move(result.second));
          } catch (const kj::Exception& e) {
            completeDatagramReceiveFailure(recv.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeDatagramReceiveFailure(recv.completion, e.what());
          } catch (...) {
            completeDatagramReceiveFailure(
                recv.completion, "unknown exception in Capnp.KjAsync datagramReceive");
          }
        }
      }

      promises_.clear();
      connectionPromises_.clear();
      listeners_.clear();
      connections_.clear();
      taskSets_.clear();
      datagramPorts_.clear();
    } catch (const kj::Exception& e) {
      {
        std::lock_guard<std::mutex> lock(startupMutex_);
        startupError_ = describeKjException(e);
        startupComplete_ = true;
      }
      startupCv_.notify_one();
      failOutstanding(startupError_);
    } catch (const std::exception& e) {
      {
        std::lock_guard<std::mutex> lock(startupMutex_);
        startupError_ = e.what();
        startupComplete_ = true;
      }
      startupCv_.notify_one();
      failOutstanding(startupError_);
    } catch (...) {
      {
        std::lock_guard<std::mutex> lock(startupMutex_);
        startupError_ = "unknown exception in Capnp.KjAsync runtime thread";
        startupComplete_ = true;
      }
      startupCv_.notify_one();
      failOutstanding(startupError_);
    }

    alive_.store(false, std::memory_order_release);
  }

  std::thread worker_;
  std::mutex startupMutex_;
  std::condition_variable startupCv_;
  bool startupComplete_ = false;
  std::string startupError_;

  std::mutex queueMutex_;
  std::condition_variable queueCv_;
  std::deque<QueuedOperation> queue_;
  bool stopping_ = false;

  std::atomic<bool> alive_{false};
  uint32_t nextPromiseId_ = 1;
  std::unordered_map<uint32_t, PendingPromise> promises_;
  uint32_t nextConnectionPromiseId_ = 1;
  std::unordered_map<uint32_t, PendingConnectionPromise> connectionPromises_;
  uint32_t nextListenerId_ = 1;
  std::unordered_map<uint32_t, kj::Own<kj::ConnectionReceiver>> listeners_;
  uint32_t nextConnectionId_ = 1;
  std::unordered_map<uint32_t, kj::Own<kj::AsyncIoStream>> connections_;
  uint32_t nextTaskSetId_ = 1;
  std::unordered_map<uint32_t, kj::Own<RuntimeTaskSet>> taskSets_;
  uint32_t nextDatagramPortId_ = 1;
  std::unordered_map<uint32_t, kj::Own<kj::DatagramPort>> datagramPorts_;
};

std::mutex gKjAsyncRuntimeRegistryMutex;
std::unordered_map<uint64_t, std::shared_ptr<KjAsyncRuntimeLoop>> gKjAsyncRuntimes;
uint64_t gNextKjAsyncRuntimeId = 1;

uint64_t allocateKjAsyncRuntimeIdLocked() {
  while (true) {
    uint64_t id = gNextKjAsyncRuntimeId++;
    if (id == 0) {
      continue;
    }
    if (gKjAsyncRuntimes.find(id) == gKjAsyncRuntimes.end()) {
      return id;
    }
  }
}

std::shared_ptr<KjAsyncRuntimeLoop> getKjAsyncRuntime(uint64_t runtimeId) {
  std::lock_guard<std::mutex> lock(gKjAsyncRuntimeRegistryMutex);
  auto it = gKjAsyncRuntimes.find(runtimeId);
  if (it == gKjAsyncRuntimes.end()) {
    return nullptr;
  }
  return it->second;
}

std::shared_ptr<KjAsyncRuntimeLoop> unregisterKjAsyncRuntime(uint64_t runtimeId) {
  std::lock_guard<std::mutex> lock(gKjAsyncRuntimeRegistryMutex);
  auto it = gKjAsyncRuntimes.find(runtimeId);
  if (it == gKjAsyncRuntimes.end()) {
    return nullptr;
  }
  auto result = std::move(it->second);
  gKjAsyncRuntimes.erase(runtimeId);
  return result;
}

bool isKjAsyncRuntimeAlive(uint64_t runtimeId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  return runtime && runtime->isAlive();
}

}  // namespace

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_new() {
  try {
    auto runtime = std::make_shared<KjAsyncRuntimeLoop>();

    uint64_t runtimeId;
    {
      std::lock_guard<std::mutex> lock(gKjAsyncRuntimeRegistryMutex);
      runtimeId = allocateKjAsyncRuntimeIdLocked();
      gKjAsyncRuntimes.emplace(runtimeId, runtime);
    }

    return lean_io_result_mk_ok(lean_box_uint64(runtimeId));
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_new");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_release(uint64_t runtimeId) {
  try {
    auto runtime = unregisterKjAsyncRuntime(runtimeId);
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_release");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_is_alive(uint64_t runtimeId) {
  return lean_io_result_mk_ok(lean_box(isKjAsyncRuntimeAlive(runtimeId) ? 1 : 0));
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_sleep_nanos_start(
    uint64_t runtimeId, uint64_t delayNanos) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueSleepNanos(delayNanos);
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      return lean_io_result_mk_ok(lean_box_uint32(completion->promiseId));
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_sleep_nanos_start");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_promise_await(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueAwaitPromise(promiseId);
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_promise_await");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_promise_cancel(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueCancelPromise(promiseId);
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_promise_cancel");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_promise_release(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueReleasePromise(promiseId);
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_promise_release");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_listen(
    uint64_t runtimeId, b_lean_obj_arg address, uint32_t portHint) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueListen(std::string(lean_string_cstr(address)), portHint);
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      return lean_io_result_mk_ok(lean_box_uint32(completion->handle));
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_listen");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_release_listener(
    uint64_t runtimeId, uint32_t listenerId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_release_listener");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_listener_accept(
    uint64_t runtimeId, uint32_t listenerId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueAccept(listenerId);
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      return lean_io_result_mk_ok(lean_box_uint32(completion->handle));
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_listener_accept");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_listener_accept_start(
    uint64_t runtimeId, uint32_t listenerId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueAcceptStart(listenerId);
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      return lean_io_result_mk_ok(lean_box_uint32(completion->promiseId));
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError(
        "unknown exception in capnp_lean_kj_async_runtime_listener_accept_start");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_connect(
    uint64_t runtimeId, b_lean_obj_arg address, uint32_t portHint) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueConnect(std::string(lean_string_cstr(address)), portHint);
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      return lean_io_result_mk_ok(lean_box_uint32(completion->handle));
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_connect");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_connect_start(
    uint64_t runtimeId, b_lean_obj_arg address, uint32_t portHint) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueConnectStart(std::string(lean_string_cstr(address)), portHint);
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      return lean_io_result_mk_ok(lean_box_uint32(completion->promiseId));
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_connect_start");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_connection_promise_await(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueAwaitConnectionPromise(promiseId);
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      return lean_io_result_mk_ok(lean_box_uint32(completion->handle));
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError(
        "unknown exception in capnp_lean_kj_async_runtime_connection_promise_await");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_connection_promise_cancel(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueCancelConnectionPromise(promiseId);
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
    return mkIoUserError(
        "unknown exception in capnp_lean_kj_async_runtime_connection_promise_cancel");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_connection_promise_release(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueReleaseConnectionPromise(promiseId);
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
    return mkIoUserError(
        "unknown exception in capnp_lean_kj_async_runtime_connection_promise_release");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_release_connection(
    uint64_t runtimeId, uint32_t connectionId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueReleaseConnection(connectionId);
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_release_connection");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_connection_write(
    uint64_t runtimeId, uint32_t connectionId, b_lean_obj_arg bytes) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueConnectionWrite(connectionId, copyByteArray(bytes));
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_connection_write");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_connection_read(
    uint64_t runtimeId, uint32_t connectionId, uint32_t minBytes, uint32_t maxBytes) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueConnectionRead(connectionId, minBytes, maxBytes);
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      return lean_io_result_mk_ok(
          mkByteArrayCopy(completion->bytes.data(), completion->bytes.size()));
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_connection_read");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_connection_shutdown_write(
    uint64_t runtimeId, uint32_t connectionId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueConnectionShutdownWrite(connectionId);
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
    return mkIoUserError(
        "unknown exception in capnp_lean_kj_async_runtime_connection_shutdown_write");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_promise_all_start(
    uint64_t runtimeId, b_lean_obj_arg promiseIds) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueuePromiseAllStart(decodeUint32Array(promiseIds));
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      return lean_io_result_mk_ok(lean_box_uint32(completion->promiseId));
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_promise_all_start");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_promise_race_start(
    uint64_t runtimeId, b_lean_obj_arg promiseIds) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueuePromiseRaceStart(decodeUint32Array(promiseIds));
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      return lean_io_result_mk_ok(lean_box_uint32(completion->promiseId));
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_promise_race_start");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_task_set_new(uint64_t runtimeId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueTaskSetNew();
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      return lean_io_result_mk_ok(lean_box_uint32(completion->handle));
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_task_set_new");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_task_set_release(
    uint64_t runtimeId, uint32_t taskSetId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueTaskSetRelease(taskSetId);
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_task_set_release");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_task_set_add_promise(
    uint64_t runtimeId, uint32_t taskSetId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueTaskSetAddPromise(taskSetId, promiseId);
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
    return mkIoUserError(
        "unknown exception in capnp_lean_kj_async_runtime_task_set_add_promise");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_task_set_clear(
    uint64_t runtimeId, uint32_t taskSetId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueTaskSetClear(taskSetId);
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_task_set_clear");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_task_set_is_empty(
    uint64_t runtimeId, uint32_t taskSetId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueTaskSetIsEmpty(taskSetId);
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      return lean_io_result_mk_ok(lean_box(completion->value ? 1 : 0));
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_task_set_is_empty");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_task_set_on_empty_start(
    uint64_t runtimeId, uint32_t taskSetId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueTaskSetOnEmptyStart(taskSetId);
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      return lean_io_result_mk_ok(lean_box_uint32(completion->promiseId));
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError(
        "unknown exception in capnp_lean_kj_async_runtime_task_set_on_empty_start");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_task_set_error_count(
    uint64_t runtimeId, uint32_t taskSetId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueTaskSetErrorCount(taskSetId);
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      return lean_io_result_mk_ok(lean_box_uint32(completion->value));
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError(
        "unknown exception in capnp_lean_kj_async_runtime_task_set_error_count");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_task_set_take_last_error(
    uint64_t runtimeId, uint32_t taskSetId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueTaskSetTakeLastError(taskSetId);
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      auto pair = lean_alloc_ctor(0, 2, 0);
      lean_ctor_set(pair, 0, lean_box(completion->hasValue ? 1 : 0));
      lean_ctor_set(pair, 1, lean_mk_string(completion->value.c_str()));
      return lean_io_result_mk_ok(pair);
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError(
        "unknown exception in capnp_lean_kj_async_runtime_task_set_take_last_error");
  }
}

extern "C" LEAN_EXPORT lean_obj_res
capnp_lean_kj_async_runtime_connection_when_write_disconnected_start(
    uint64_t runtimeId, uint32_t connectionId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueConnectionWhenWriteDisconnectedStart(connectionId);
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      return lean_io_result_mk_ok(lean_box_uint32(completion->promiseId));
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError(
        "unknown exception in capnp_lean_kj_async_runtime_connection_when_write_disconnected_start");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_connection_abort_read(
    uint64_t runtimeId, uint32_t connectionId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueConnectionAbortRead(connectionId);
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_connection_abort_read");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_connection_abort_write(
    uint64_t runtimeId, uint32_t connectionId, b_lean_obj_arg reason) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion =
        runtime->enqueueConnectionAbortWrite(connectionId, std::string(lean_string_cstr(reason)));
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
    return mkIoUserError(
        "unknown exception in capnp_lean_kj_async_runtime_connection_abort_write");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_new_two_way_pipe(
    uint64_t runtimeId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueNewTwoWayPipe();
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      auto pair = lean_alloc_ctor(0, 2, 0);
      lean_ctor_set(pair, 0, lean_box_uint32(completion->first));
      lean_ctor_set(pair, 1, lean_box_uint32(completion->second));
      return lean_io_result_mk_ok(pair);
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_new_two_way_pipe");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_datagram_bind(
    uint64_t runtimeId, b_lean_obj_arg address, uint32_t portHint) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueDatagramBind(std::string(lean_string_cstr(address)), portHint);
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      return lean_io_result_mk_ok(lean_box_uint32(completion->handle));
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_datagram_bind");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_datagram_release_port(
    uint64_t runtimeId, uint32_t portId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueDatagramReleasePort(portId);
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_datagram_release_port");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_datagram_get_port(
    uint64_t runtimeId, uint32_t portId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueDatagramGetPort(portId);
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      return lean_io_result_mk_ok(lean_box_uint32(completion->handle));
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_datagram_get_port");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_datagram_send(
    uint64_t runtimeId, uint32_t portId, b_lean_obj_arg address, uint32_t portHint,
    b_lean_obj_arg bytes) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueDatagramSend(
        portId, std::string(lean_string_cstr(address)), portHint, copyByteArray(bytes));
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      return lean_io_result_mk_ok(lean_box_uint32(completion->value));
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_datagram_send");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_datagram_receive(
    uint64_t runtimeId, uint32_t portId, uint32_t maxBytes) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueDatagramReceive(portId, maxBytes);
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      auto pair = lean_alloc_ctor(0, 2, 0);
      lean_ctor_set(pair, 0, lean_mk_string(completion->sourceAddress.c_str()));
      lean_ctor_set(pair, 1, mkByteArrayCopy(completion->bytes.data(), completion->bytes.size()));
      return lean_io_result_mk_ok(pair);
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_datagram_receive");
  }
}
