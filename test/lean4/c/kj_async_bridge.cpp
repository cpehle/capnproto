#include <lean/lean.h>

#include <kj/async.h>
#include <kj/async-io.h>
#include <kj/compat/http.h>
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
#include <random>
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

kj::ArrayPtr<const kj::byte> viewAsKjBytes(const std::vector<uint8_t>& bytes) {
  return kj::ArrayPtr<const kj::byte>(reinterpret_cast<const kj::byte*>(bytes.data()), bytes.size());
}

kj::ArrayPtr<kj::byte> viewAsMutableKjBytes(std::vector<uint8_t>& bytes) {
  return kj::ArrayPtr<kj::byte>(reinterpret_cast<kj::byte*>(bytes.data()), bytes.size());
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

struct HttpResponseCompletion {
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  bool ok = false;
  uint32_t statusCode = 0;
  std::vector<uint8_t> body;
  std::string error;
};

struct WebSocketMessageCompletion {
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  bool ok = false;
  uint8_t tag = 0;
  uint16_t closeCode = 0;
  std::string text;
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

void completeHttpResponseSuccess(const std::shared_ptr<HttpResponseCompletion>& completion,
                                 uint32_t statusCode, std::vector<uint8_t> body) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = true;
    completion->statusCode = statusCode;
    completion->body = std::move(body);
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeHttpResponseFailure(const std::shared_ptr<HttpResponseCompletion>& completion,
                                 std::string message) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = false;
    completion->error = std::move(message);
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeWebSocketMessageSuccess(const std::shared_ptr<WebSocketMessageCompletion>& completion,
                                     uint8_t tag, uint16_t closeCode, std::string text,
                                     std::vector<uint8_t> bytes) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = true;
    completion->tag = tag;
    completion->closeCode = closeCode;
    completion->text = std::move(text);
    completion->bytes = std::move(bytes);
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeWebSocketMessageFailure(const std::shared_ptr<WebSocketMessageCompletion>& completion,
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

  std::shared_ptr<PromiseIdCompletion> enqueueConnectionWriteStart(uint32_t connectionId,
                                                                   std::vector<uint8_t> bytes) {
    auto completion = std::make_shared<PromiseIdCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completePromiseIdFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(
          QueuedConnectionWriteStart{connectionId, std::move(bytes), completion});
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

  std::shared_ptr<PromiseIdCompletion> enqueueConnectionReadStart(uint32_t connectionId,
                                                                  uint32_t minBytes,
                                                                  uint32_t maxBytes) {
    auto completion = std::make_shared<PromiseIdCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completePromiseIdFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(
          QueuedConnectionReadStart{connectionId, minBytes, maxBytes, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<BytesCompletion> enqueueBytesPromiseAwait(uint32_t promiseId) {
    auto completion = std::make_shared<BytesCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeBytesFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedBytesPromiseAwait{promiseId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueBytesPromiseCancel(uint32_t promiseId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedBytesPromiseCancel{promiseId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueBytesPromiseRelease(uint32_t promiseId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedBytesPromiseRelease{promiseId, completion});
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

  std::shared_ptr<PromiseIdCompletion> enqueueConnectionShutdownWriteStart(uint32_t connectionId) {
    auto completion = std::make_shared<PromiseIdCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completePromiseIdFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedConnectionShutdownWriteStart{connectionId, completion});
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

  std::shared_ptr<PromiseIdCompletion> enqueueDatagramSendStart(uint32_t portId, std::string address,
                                                                uint32_t portHint,
                                                                std::vector<uint8_t> bytes) {
    auto completion = std::make_shared<PromiseIdCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completePromiseIdFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedDatagramSendStart{portId, std::move(address), portHint,
                                                  std::move(bytes), completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UInt32Completion> enqueueUInt32PromiseAwait(uint32_t promiseId) {
    auto completion = std::make_shared<UInt32Completion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUInt32Failure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedUInt32PromiseAwait{promiseId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueUInt32PromiseCancel(uint32_t promiseId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedUInt32PromiseCancel{promiseId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueUInt32PromiseRelease(uint32_t promiseId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedUInt32PromiseRelease{promiseId, completion});
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

  std::shared_ptr<PromiseIdCompletion> enqueueDatagramReceiveStart(uint32_t portId,
                                                                   uint32_t maxBytes) {
    auto completion = std::make_shared<PromiseIdCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completePromiseIdFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedDatagramReceiveStart{portId, maxBytes, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<DatagramReceiveCompletion> enqueueDatagramReceivePromiseAwait(uint32_t promiseId) {
    auto completion = std::make_shared<DatagramReceiveCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeDatagramReceiveFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedDatagramReceivePromiseAwait{promiseId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueDatagramReceivePromiseCancel(uint32_t promiseId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedDatagramReceivePromiseCancel{promiseId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueDatagramReceivePromiseRelease(uint32_t promiseId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedDatagramReceivePromiseRelease{promiseId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<HttpResponseCompletion> enqueueHttpRequest(uint32_t method, std::string address,
                                                             uint32_t portHint, std::string path,
                                                             std::vector<uint8_t> body) {
    auto completion = std::make_shared<HttpResponseCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeHttpResponseFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedHttpRequest{
          method, std::move(address), portHint, std::move(path), std::move(body), completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<PromiseIdCompletion> enqueueHttpRequestStart(uint32_t method,
                                                               std::string address,
                                                               uint32_t portHint,
                                                               std::string path,
                                                               std::vector<uint8_t> body) {
    auto completion = std::make_shared<PromiseIdCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completePromiseIdFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedHttpRequestStart{
          method, std::move(address), portHint, std::move(path), std::move(body), completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<HttpResponseCompletion> enqueueHttpResponsePromiseAwait(uint32_t promiseId) {
    auto completion = std::make_shared<HttpResponseCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeHttpResponseFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedHttpResponsePromiseAwait{promiseId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueHttpResponsePromiseCancel(uint32_t promiseId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedHttpResponsePromiseCancel{promiseId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueHttpResponsePromiseRelease(uint32_t promiseId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedHttpResponsePromiseRelease{promiseId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<HandleCompletion> enqueueWebSocketConnect(std::string address, uint32_t portHint,
                                                            std::string path) {
    auto completion = std::make_shared<HandleCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeHandleFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(
          QueuedWebSocketConnect{std::move(address), portHint, std::move(path), completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<PromiseIdCompletion> enqueueWebSocketConnectStart(std::string address,
                                                                     uint32_t portHint,
                                                                     std::string path) {
    auto completion = std::make_shared<PromiseIdCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completePromiseIdFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(
          QueuedWebSocketConnectStart{std::move(address), portHint, std::move(path), completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<HandleCompletion> enqueueWebSocketPromiseAwait(uint32_t promiseId) {
    auto completion = std::make_shared<HandleCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeHandleFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedWebSocketPromiseAwait{promiseId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueWebSocketPromiseCancel(uint32_t promiseId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedWebSocketPromiseCancel{promiseId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueWebSocketPromiseRelease(uint32_t promiseId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedWebSocketPromiseRelease{promiseId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueWebSocketRelease(uint32_t webSocketId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedWebSocketRelease{webSocketId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<PromiseIdCompletion> enqueueWebSocketSendTextStart(uint32_t webSocketId,
                                                                      std::string text) {
    auto completion = std::make_shared<PromiseIdCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completePromiseIdFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedWebSocketSendTextStart{webSocketId, std::move(text), completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueWebSocketSendText(uint32_t webSocketId, std::string text) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedWebSocketSendText{webSocketId, std::move(text), completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<PromiseIdCompletion> enqueueWebSocketSendBinaryStart(uint32_t webSocketId,
                                                                        std::vector<uint8_t> bytes) {
    auto completion = std::make_shared<PromiseIdCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completePromiseIdFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(
          QueuedWebSocketSendBinaryStart{webSocketId, std::move(bytes), completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueWebSocketSendBinary(uint32_t webSocketId,
                                                             std::vector<uint8_t> bytes) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedWebSocketSendBinary{webSocketId, std::move(bytes), completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<PromiseIdCompletion> enqueueWebSocketReceiveStart(uint32_t webSocketId) {
    auto completion = std::make_shared<PromiseIdCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completePromiseIdFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedWebSocketReceiveStart{webSocketId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<WebSocketMessageCompletion> enqueueWebSocketMessagePromiseAwait(
      uint32_t promiseId) {
    auto completion = std::make_shared<WebSocketMessageCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeWebSocketMessageFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedWebSocketMessagePromiseAwait{promiseId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueWebSocketMessagePromiseCancel(uint32_t promiseId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedWebSocketMessagePromiseCancel{promiseId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueWebSocketMessagePromiseRelease(uint32_t promiseId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedWebSocketMessagePromiseRelease{promiseId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<WebSocketMessageCompletion> enqueueWebSocketReceive(uint32_t webSocketId) {
    auto completion = std::make_shared<WebSocketMessageCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeWebSocketMessageFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedWebSocketReceive{webSocketId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<PromiseIdCompletion> enqueueWebSocketCloseStart(uint32_t webSocketId,
                                                                   uint16_t closeCode,
                                                                   std::string reason) {
    auto completion = std::make_shared<PromiseIdCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completePromiseIdFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(
          QueuedWebSocketCloseStart{webSocketId, closeCode, std::move(reason), completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueWebSocketClose(uint32_t webSocketId, uint16_t closeCode,
                                                        std::string reason) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(
          QueuedWebSocketClose{webSocketId, closeCode, std::move(reason), completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueWebSocketDisconnect(uint32_t webSocketId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedWebSocketDisconnect{webSocketId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueWebSocketAbort(uint32_t webSocketId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedWebSocketAbort{webSocketId, completion});
    }
    queueCv_.notify_one();
    return completion;
  }

  std::shared_ptr<HandlePairCompletion> enqueueNewWebSocketPipe() {
    auto completion = std::make_shared<HandlePairCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeHandlePairFailure(completion, "Capnp.KjAsync runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedNewWebSocketPipe{completion});
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

  struct PendingBytesPromise {
    PendingBytesPromise(kj::Promise<std::vector<uint8_t>>&& promise,
                        kj::Own<kj::Canceler>&& canceler)
        : promise(kj::mv(promise)), canceler(kj::mv(canceler)) {}

    PendingBytesPromise(PendingBytesPromise&&) = default;
    PendingBytesPromise& operator=(PendingBytesPromise&&) = default;
    PendingBytesPromise(const PendingBytesPromise&) = delete;
    PendingBytesPromise& operator=(const PendingBytesPromise&) = delete;

    kj::Promise<std::vector<uint8_t>> promise;
    kj::Own<kj::Canceler> canceler;
  };

  struct PendingUInt32Promise {
    PendingUInt32Promise(kj::Promise<uint32_t>&& promise, kj::Own<kj::Canceler>&& canceler)
        : promise(kj::mv(promise)), canceler(kj::mv(canceler)) {}

    PendingUInt32Promise(PendingUInt32Promise&&) = default;
    PendingUInt32Promise& operator=(PendingUInt32Promise&&) = default;
    PendingUInt32Promise(const PendingUInt32Promise&) = delete;
    PendingUInt32Promise& operator=(const PendingUInt32Promise&) = delete;

    kj::Promise<uint32_t> promise;
    kj::Own<kj::Canceler> canceler;
  };

  struct DatagramReceiveResult {
    std::string sourceAddress;
    std::vector<uint8_t> bytes;
  };

  struct PendingDatagramReceivePromise {
    PendingDatagramReceivePromise(kj::Promise<DatagramReceiveResult>&& promise,
                                  kj::Own<kj::Canceler>&& canceler)
        : promise(kj::mv(promise)), canceler(kj::mv(canceler)) {}

    PendingDatagramReceivePromise(PendingDatagramReceivePromise&&) = default;
    PendingDatagramReceivePromise& operator=(PendingDatagramReceivePromise&&) = default;
    PendingDatagramReceivePromise(const PendingDatagramReceivePromise&) = delete;
    PendingDatagramReceivePromise& operator=(const PendingDatagramReceivePromise&) = delete;

    kj::Promise<DatagramReceiveResult> promise;
    kj::Own<kj::Canceler> canceler;
  };

  struct HttpResponseResult {
    uint32_t statusCode = 0;
    std::vector<uint8_t> body;
  };

  class RuntimeEntropySource final : public kj::EntropySource {
   public:
    void generate(kj::ArrayPtr<kj::byte> buffer) override {
      for (auto& value : buffer) {
        value = static_cast<kj::byte>(random_());
      }
    }

   private:
    std::random_device random_;
  };

  struct RuntimeWebSocketOwner {
    kj::Own<kj::HttpHeaderTable> headerTable;
    kj::Own<RuntimeEntropySource> entropySource;
    kj::Own<kj::HttpClient> client;
  };

  struct RuntimeWebSocket {
    kj::Maybe<kj::Own<RuntimeWebSocketOwner>> owner;
    kj::Own<kj::WebSocket> socket;
  };

  struct WebSocketMessageResult;

  struct PendingHttpResponsePromise {
    PendingHttpResponsePromise(kj::Promise<HttpResponseResult>&& promise,
                               kj::Own<kj::Canceler>&& canceler)
        : promise(kj::mv(promise)), canceler(kj::mv(canceler)) {}

    PendingHttpResponsePromise(PendingHttpResponsePromise&&) = default;
    PendingHttpResponsePromise& operator=(PendingHttpResponsePromise&&) = default;
    PendingHttpResponsePromise(const PendingHttpResponsePromise&) = delete;
    PendingHttpResponsePromise& operator=(const PendingHttpResponsePromise&) = delete;

    kj::Promise<HttpResponseResult> promise;
    kj::Own<kj::Canceler> canceler;
  };

  struct PendingWebSocketPromise {
    PendingWebSocketPromise(kj::Promise<RuntimeWebSocket>&& promise,
                            kj::Own<kj::Canceler>&& canceler)
        : promise(kj::mv(promise)), canceler(kj::mv(canceler)) {}

    PendingWebSocketPromise(PendingWebSocketPromise&&) = default;
    PendingWebSocketPromise& operator=(PendingWebSocketPromise&&) = default;
    PendingWebSocketPromise(const PendingWebSocketPromise&) = delete;
    PendingWebSocketPromise& operator=(const PendingWebSocketPromise&) = delete;

    kj::Promise<RuntimeWebSocket> promise;
    kj::Own<kj::Canceler> canceler;
  };

  struct PendingWebSocketMessagePromise {
    PendingWebSocketMessagePromise(kj::Promise<WebSocketMessageResult>&& promise,
                                   kj::Own<kj::Canceler>&& canceler)
        : promise(kj::mv(promise)), canceler(kj::mv(canceler)) {}

    PendingWebSocketMessagePromise(PendingWebSocketMessagePromise&&) = default;
    PendingWebSocketMessagePromise& operator=(PendingWebSocketMessagePromise&&) = default;
    PendingWebSocketMessagePromise(const PendingWebSocketMessagePromise&) = delete;
    PendingWebSocketMessagePromise& operator=(const PendingWebSocketMessagePromise&) = delete;

    kj::Promise<WebSocketMessageResult> promise;
    kj::Own<kj::Canceler> canceler;
  };

  struct WebSocketMessageResult {
    uint8_t tag = 0;
    uint16_t closeCode = 0;
    std::string text;
    std::vector<uint8_t> bytes;
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

  struct QueuedConnectionWriteStart {
    uint32_t connectionId;
    std::vector<uint8_t> bytes;
    std::shared_ptr<PromiseIdCompletion> completion;
  };

  struct QueuedConnectionRead {
    uint32_t connectionId;
    uint32_t minBytes;
    uint32_t maxBytes;
    std::shared_ptr<BytesCompletion> completion;
  };

  struct QueuedConnectionReadStart {
    uint32_t connectionId;
    uint32_t minBytes;
    uint32_t maxBytes;
    std::shared_ptr<PromiseIdCompletion> completion;
  };

  struct QueuedBytesPromiseAwait {
    uint32_t promiseId;
    std::shared_ptr<BytesCompletion> completion;
  };

  struct QueuedBytesPromiseCancel {
    uint32_t promiseId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedBytesPromiseRelease {
    uint32_t promiseId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedConnectionShutdownWrite {
    uint32_t connectionId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedConnectionShutdownWriteStart {
    uint32_t connectionId;
    std::shared_ptr<PromiseIdCompletion> completion;
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

  struct QueuedDatagramSendStart {
    uint32_t portId;
    std::string address;
    uint32_t portHint;
    std::vector<uint8_t> bytes;
    std::shared_ptr<PromiseIdCompletion> completion;
  };

  struct QueuedUInt32PromiseAwait {
    uint32_t promiseId;
    std::shared_ptr<UInt32Completion> completion;
  };

  struct QueuedUInt32PromiseCancel {
    uint32_t promiseId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedUInt32PromiseRelease {
    uint32_t promiseId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedDatagramReceive {
    uint32_t portId;
    uint32_t maxBytes;
    std::shared_ptr<DatagramReceiveCompletion> completion;
  };

  struct QueuedDatagramReceiveStart {
    uint32_t portId;
    uint32_t maxBytes;
    std::shared_ptr<PromiseIdCompletion> completion;
  };

  struct QueuedDatagramReceivePromiseAwait {
    uint32_t promiseId;
    std::shared_ptr<DatagramReceiveCompletion> completion;
  };

  struct QueuedDatagramReceivePromiseCancel {
    uint32_t promiseId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedDatagramReceivePromiseRelease {
    uint32_t promiseId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedHttpRequest {
    uint32_t method;
    std::string address;
    uint32_t portHint;
    std::string path;
    std::vector<uint8_t> body;
    std::shared_ptr<HttpResponseCompletion> completion;
  };

  struct QueuedHttpRequestStart {
    uint32_t method;
    std::string address;
    uint32_t portHint;
    std::string path;
    std::vector<uint8_t> body;
    std::shared_ptr<PromiseIdCompletion> completion;
  };

  struct QueuedHttpResponsePromiseAwait {
    uint32_t promiseId;
    std::shared_ptr<HttpResponseCompletion> completion;
  };

  struct QueuedHttpResponsePromiseCancel {
    uint32_t promiseId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedHttpResponsePromiseRelease {
    uint32_t promiseId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedWebSocketConnect {
    std::string address;
    uint32_t portHint;
    std::string path;
    std::shared_ptr<HandleCompletion> completion;
  };

  struct QueuedWebSocketConnectStart {
    std::string address;
    uint32_t portHint;
    std::string path;
    std::shared_ptr<PromiseIdCompletion> completion;
  };

  struct QueuedWebSocketPromiseAwait {
    uint32_t promiseId;
    std::shared_ptr<HandleCompletion> completion;
  };

  struct QueuedWebSocketPromiseCancel {
    uint32_t promiseId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedWebSocketPromiseRelease {
    uint32_t promiseId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedWebSocketRelease {
    uint32_t webSocketId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedWebSocketSendTextStart {
    uint32_t webSocketId;
    std::string text;
    std::shared_ptr<PromiseIdCompletion> completion;
  };

  struct QueuedWebSocketSendText {
    uint32_t webSocketId;
    std::string text;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedWebSocketSendBinaryStart {
    uint32_t webSocketId;
    std::vector<uint8_t> bytes;
    std::shared_ptr<PromiseIdCompletion> completion;
  };

  struct QueuedWebSocketSendBinary {
    uint32_t webSocketId;
    std::vector<uint8_t> bytes;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedWebSocketReceiveStart {
    uint32_t webSocketId;
    std::shared_ptr<PromiseIdCompletion> completion;
  };

  struct QueuedWebSocketMessagePromiseAwait {
    uint32_t promiseId;
    std::shared_ptr<WebSocketMessageCompletion> completion;
  };

  struct QueuedWebSocketMessagePromiseCancel {
    uint32_t promiseId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedWebSocketMessagePromiseRelease {
    uint32_t promiseId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedWebSocketReceive {
    uint32_t webSocketId;
    std::shared_ptr<WebSocketMessageCompletion> completion;
  };

  struct QueuedWebSocketCloseStart {
    uint32_t webSocketId;
    uint16_t closeCode;
    std::string reason;
    std::shared_ptr<PromiseIdCompletion> completion;
  };

  struct QueuedWebSocketClose {
    uint32_t webSocketId;
    uint16_t closeCode;
    std::string reason;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedWebSocketDisconnect {
    uint32_t webSocketId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedWebSocketAbort {
    uint32_t webSocketId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedNewWebSocketPipe {
    std::shared_ptr<HandlePairCompletion> completion;
  };

  using QueuedOperation =
      std::variant<QueuedSleepNanos, QueuedAwaitPromise, QueuedCancelPromise,
                   QueuedReleasePromise, QueuedListen, QueuedReleaseListener, QueuedAccept,
                   QueuedAcceptStart, QueuedConnect, QueuedConnectStart,
                   QueuedAwaitConnectionPromise, QueuedCancelConnectionPromise,
                   QueuedReleaseConnectionPromise, QueuedReleaseConnection,
                   QueuedConnectionWrite, QueuedConnectionWriteStart, QueuedConnectionRead,
                   QueuedConnectionReadStart, QueuedBytesPromiseAwait,
                   QueuedBytesPromiseCancel, QueuedBytesPromiseRelease,
                   QueuedConnectionShutdownWrite, QueuedConnectionShutdownWriteStart,
                   QueuedPromiseAllStart, QueuedPromiseRaceStart, QueuedTaskSetNew,
                   QueuedTaskSetRelease, QueuedTaskSetAddPromise, QueuedTaskSetClear,
                   QueuedTaskSetIsEmpty, QueuedTaskSetOnEmptyStart, QueuedTaskSetErrorCount,
                   QueuedTaskSetTakeLastError, QueuedConnectionWhenWriteDisconnectedStart,
                   QueuedConnectionAbortRead, QueuedConnectionAbortWrite, QueuedNewTwoWayPipe,
                   QueuedDatagramBind, QueuedDatagramReleasePort, QueuedDatagramGetPort,
                   QueuedDatagramSend, QueuedDatagramSendStart, QueuedUInt32PromiseAwait,
                   QueuedUInt32PromiseCancel, QueuedUInt32PromiseRelease, QueuedDatagramReceive,
                   QueuedDatagramReceiveStart, QueuedDatagramReceivePromiseAwait,
                   QueuedDatagramReceivePromiseCancel, QueuedDatagramReceivePromiseRelease,
                   QueuedHttpRequest,
                   QueuedHttpRequestStart, QueuedHttpResponsePromiseAwait,
                   QueuedHttpResponsePromiseCancel, QueuedHttpResponsePromiseRelease,
                   QueuedWebSocketConnect, QueuedWebSocketConnectStart,
                   QueuedWebSocketPromiseAwait, QueuedWebSocketPromiseCancel,
                   QueuedWebSocketPromiseRelease, QueuedWebSocketRelease,
                   QueuedWebSocketSendTextStart, QueuedWebSocketSendText,
                   QueuedWebSocketSendBinaryStart, QueuedWebSocketSendBinary,
                   QueuedWebSocketReceiveStart, QueuedWebSocketMessagePromiseAwait,
                   QueuedWebSocketMessagePromiseCancel, QueuedWebSocketMessagePromiseRelease,
                   QueuedWebSocketReceive, QueuedWebSocketCloseStart, QueuedWebSocketClose,
                   QueuedWebSocketDisconnect, QueuedWebSocketAbort, QueuedNewWebSocketPipe>;

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

  uint32_t addBytesPromise(PendingBytesPromise&& promise) {
    uint32_t promiseId = nextBytesPromiseId_++;
    while (bytesPromises_.find(promiseId) != bytesPromises_.end()) {
      promiseId = nextBytesPromiseId_++;
    }
    bytesPromises_.emplace(promiseId, std::move(promise));
    return promiseId;
  }

  uint32_t addUInt32Promise(PendingUInt32Promise&& promise) {
    uint32_t promiseId = nextUInt32PromiseId_++;
    while (uint32Promises_.find(promiseId) != uint32Promises_.end()) {
      promiseId = nextUInt32PromiseId_++;
    }
    uint32Promises_.emplace(promiseId, std::move(promise));
    return promiseId;
  }

  uint32_t addDatagramReceivePromise(PendingDatagramReceivePromise&& promise) {
    uint32_t promiseId = nextDatagramReceivePromiseId_++;
    while (datagramReceivePromises_.find(promiseId) != datagramReceivePromises_.end()) {
      promiseId = nextDatagramReceivePromiseId_++;
    }
    datagramReceivePromises_.emplace(promiseId, std::move(promise));
    return promiseId;
  }

  uint32_t addHttpResponsePromise(PendingHttpResponsePromise&& promise) {
    uint32_t promiseId = nextHttpResponsePromiseId_++;
    while (httpResponsePromises_.find(promiseId) != httpResponsePromises_.end()) {
      promiseId = nextHttpResponsePromiseId_++;
    }
    httpResponsePromises_.emplace(promiseId, std::move(promise));
    return promiseId;
  }

  uint32_t addWebSocketPromise(PendingWebSocketPromise&& promise) {
    uint32_t promiseId = nextWebSocketPromiseId_++;
    while (webSocketPromises_.find(promiseId) != webSocketPromises_.end()) {
      promiseId = nextWebSocketPromiseId_++;
    }
    webSocketPromises_.emplace(promiseId, std::move(promise));
    return promiseId;
  }

  uint32_t addWebSocketMessagePromise(PendingWebSocketMessagePromise&& promise) {
    uint32_t promiseId = nextWebSocketMessagePromiseId_++;
    while (webSocketMessagePromises_.find(promiseId) != webSocketMessagePromises_.end()) {
      promiseId = nextWebSocketMessagePromiseId_++;
    }
    webSocketMessagePromises_.emplace(promiseId, std::move(promise));
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

  uint32_t addWebSocket(kj::Own<kj::WebSocket>&& socket,
                        kj::Maybe<kj::Own<RuntimeWebSocketOwner>> owner = kj::none) {
    uint32_t webSocketId = nextWebSocketId_++;
    while (webSockets_.find(webSocketId) != webSockets_.end()) {
      webSocketId = nextWebSocketId_++;
    }
    webSockets_.emplace(webSocketId, RuntimeWebSocket{kj::mv(owner), kj::mv(socket)});
    return webSocketId;
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

  std::vector<uint8_t> awaitBytesPromise(kj::WaitScope& waitScope, uint32_t promiseId) {
    auto it = bytesPromises_.find(promiseId);
    if (it == bytesPromises_.end()) {
      throw std::runtime_error("unknown KJ bytes promise id: " + std::to_string(promiseId));
    }
    auto pending = kj::mv(it->second);
    bytesPromises_.erase(it);
    return kj::mv(pending.promise).wait(waitScope);
  }

  void cancelBytesPromise(uint32_t promiseId) {
    auto it = bytesPromises_.find(promiseId);
    if (it == bytesPromises_.end()) {
      throw std::runtime_error("unknown KJ bytes promise id: " + std::to_string(promiseId));
    }
    it->second.canceler->cancel("Capnp.KjAsync bytes promise canceled from Lean");
  }

  void releaseBytesPromise(uint32_t promiseId) {
    auto it = bytesPromises_.find(promiseId);
    if (it == bytesPromises_.end()) {
      throw std::runtime_error("unknown KJ bytes promise id: " + std::to_string(promiseId));
    }
    bytesPromises_.erase(it);
  }

  uint32_t awaitUInt32Promise(kj::WaitScope& waitScope, uint32_t promiseId) {
    auto it = uint32Promises_.find(promiseId);
    if (it == uint32Promises_.end()) {
      throw std::runtime_error("unknown KJ UInt32 promise id: " + std::to_string(promiseId));
    }
    auto pending = kj::mv(it->second);
    uint32Promises_.erase(it);
    return kj::mv(pending.promise).wait(waitScope);
  }

  void cancelUInt32Promise(uint32_t promiseId) {
    auto it = uint32Promises_.find(promiseId);
    if (it == uint32Promises_.end()) {
      throw std::runtime_error("unknown KJ UInt32 promise id: " + std::to_string(promiseId));
    }
    it->second.canceler->cancel("Capnp.KjAsync UInt32 promise canceled from Lean");
  }

  void releaseUInt32Promise(uint32_t promiseId) {
    auto it = uint32Promises_.find(promiseId);
    if (it == uint32Promises_.end()) {
      throw std::runtime_error("unknown KJ UInt32 promise id: " + std::to_string(promiseId));
    }
    uint32Promises_.erase(it);
  }

  DatagramReceiveResult awaitDatagramReceivePromise(kj::WaitScope& waitScope, uint32_t promiseId) {
    auto it = datagramReceivePromises_.find(promiseId);
    if (it == datagramReceivePromises_.end()) {
      throw std::runtime_error("unknown KJ datagram receive promise id: " +
                               std::to_string(promiseId));
    }
    auto pending = kj::mv(it->second);
    datagramReceivePromises_.erase(it);
    return kj::mv(pending.promise).wait(waitScope);
  }

  void cancelDatagramReceivePromise(uint32_t promiseId) {
    auto it = datagramReceivePromises_.find(promiseId);
    if (it == datagramReceivePromises_.end()) {
      throw std::runtime_error("unknown KJ datagram receive promise id: " +
                               std::to_string(promiseId));
    }
    it->second.canceler->cancel("Capnp.KjAsync datagram receive promise canceled from Lean");
  }

  void releaseDatagramReceivePromise(uint32_t promiseId) {
    auto it = datagramReceivePromises_.find(promiseId);
    if (it == datagramReceivePromises_.end()) {
      throw std::runtime_error("unknown KJ datagram receive promise id: " +
                               std::to_string(promiseId));
    }
    datagramReceivePromises_.erase(it);
  }

  HttpResponseResult awaitHttpResponsePromise(kj::WaitScope& waitScope, uint32_t promiseId) {
    auto it = httpResponsePromises_.find(promiseId);
    if (it == httpResponsePromises_.end()) {
      throw std::runtime_error("unknown KJ HTTP response promise id: " +
                               std::to_string(promiseId));
    }
    auto pending = kj::mv(it->second);
    httpResponsePromises_.erase(it);
    return kj::mv(pending.promise).wait(waitScope);
  }

  void cancelHttpResponsePromise(uint32_t promiseId) {
    auto it = httpResponsePromises_.find(promiseId);
    if (it == httpResponsePromises_.end()) {
      throw std::runtime_error("unknown KJ HTTP response promise id: " +
                               std::to_string(promiseId));
    }
    it->second.canceler->cancel("Capnp.KjAsync HTTP response promise canceled from Lean");
  }

  void releaseHttpResponsePromise(uint32_t promiseId) {
    auto it = httpResponsePromises_.find(promiseId);
    if (it == httpResponsePromises_.end()) {
      throw std::runtime_error("unknown KJ HTTP response promise id: " +
                               std::to_string(promiseId));
    }
    httpResponsePromises_.erase(it);
  }

  uint32_t awaitWebSocketPromise(kj::WaitScope& waitScope, uint32_t promiseId) {
    auto it = webSocketPromises_.find(promiseId);
    if (it == webSocketPromises_.end()) {
      throw std::runtime_error("unknown KJ websocket promise id: " + std::to_string(promiseId));
    }
    auto pending = kj::mv(it->second);
    webSocketPromises_.erase(it);
    auto runtimeWebSocket = kj::mv(pending.promise).wait(waitScope);
    return addWebSocket(kj::mv(runtimeWebSocket.socket), kj::mv(runtimeWebSocket.owner));
  }

  void cancelWebSocketPromise(uint32_t promiseId) {
    auto it = webSocketPromises_.find(promiseId);
    if (it == webSocketPromises_.end()) {
      throw std::runtime_error("unknown KJ websocket promise id: " + std::to_string(promiseId));
    }
    it->second.canceler->cancel("Capnp.KjAsync websocket promise canceled from Lean");
  }

  void releaseWebSocketPromise(uint32_t promiseId) {
    auto it = webSocketPromises_.find(promiseId);
    if (it == webSocketPromises_.end()) {
      throw std::runtime_error("unknown KJ websocket promise id: " + std::to_string(promiseId));
    }
    webSocketPromises_.erase(it);
  }

  WebSocketMessageResult awaitWebSocketMessagePromise(kj::WaitScope& waitScope,
                                                      uint32_t promiseId) {
    auto it = webSocketMessagePromises_.find(promiseId);
    if (it == webSocketMessagePromises_.end()) {
      throw std::runtime_error("unknown KJ websocket message promise id: " +
                               std::to_string(promiseId));
    }
    auto pending = kj::mv(it->second);
    webSocketMessagePromises_.erase(it);
    return kj::mv(pending.promise).wait(waitScope);
  }

  void cancelWebSocketMessagePromise(uint32_t promiseId) {
    auto it = webSocketMessagePromises_.find(promiseId);
    if (it == webSocketMessagePromises_.end()) {
      throw std::runtime_error("unknown KJ websocket message promise id: " +
                               std::to_string(promiseId));
    }
    it->second.canceler->cancel("Capnp.KjAsync websocket message promise canceled from Lean");
  }

  void releaseWebSocketMessagePromise(uint32_t promiseId) {
    auto it = webSocketMessagePromises_.find(promiseId);
    if (it == webSocketMessagePromises_.end()) {
      throw std::runtime_error("unknown KJ websocket message promise id: " +
                               std::to_string(promiseId));
    }
    webSocketMessagePromises_.erase(it);
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

  uint32_t connectionWriteStart(uint32_t connectionId, const std::vector<uint8_t>& bytes) {
    auto it = connections_.find(connectionId);
    if (it == connections_.end()) {
      throw std::runtime_error("unknown KJ connection id: " + std::to_string(connectionId));
    }

    auto canceler = kj::heap<kj::Canceler>();
    if (bytes.empty()) {
      kj::Promise<void> ready = kj::READY_NOW;
      auto wrapped = canceler->wrap(kj::mv(ready));
      return addPromise(PendingPromise(kj::mv(wrapped), kj::mv(canceler)));
    }
    auto bytesCopy = kj::heapArray<kj::byte>(bytes.size());
    std::memcpy(bytesCopy.begin(), bytes.data(), bytes.size());
    auto promise =
        canceler->wrap(it->second->write(bytesCopy.asPtr()).attach(kj::mv(bytesCopy)));
    return addPromise(PendingPromise(kj::mv(promise), kj::mv(canceler)));
  }

  void connectionWrite(kj::WaitScope& waitScope, uint32_t connectionId,
                       const std::vector<uint8_t>& bytes) {
    auto promiseId = connectionWriteStart(connectionId, bytes);
    awaitPromise(waitScope, promiseId);
  }

  uint32_t connectionReadStart(uint32_t connectionId, uint32_t minBytes, uint32_t maxBytes) {
    if (minBytes > maxBytes) {
      throw std::runtime_error("connection read requires minBytes <= maxBytes");
    }

    auto it = connections_.find(connectionId);
    if (it == connections_.end()) {
      throw std::runtime_error("unknown KJ connection id: " + std::to_string(connectionId));
    }

    auto canceler = kj::heap<kj::Canceler>();
    if (maxBytes == 0) {
      kj::Promise<void> ready = kj::READY_NOW;
      auto wrapped = canceler->wrap(kj::mv(ready).then([]() { return std::vector<uint8_t>(); }));
      return addBytesPromise(PendingBytesPromise(kj::mv(wrapped), kj::mv(canceler)));
    }

    auto buffer = kj::heapArray<kj::byte>(maxBytes);
    auto promise =
        canceler->wrap(it->second
                           ->tryRead(buffer.begin(), static_cast<size_t>(minBytes),
                                     static_cast<size_t>(maxBytes))
                           .then([buffer = kj::mv(buffer)](size_t readCount) mutable {
                             std::vector<uint8_t> bytes(readCount);
                             if (readCount != 0) {
                               std::memcpy(bytes.data(), buffer.begin(), readCount);
                             }
                             return bytes;
                           }));
    return addBytesPromise(PendingBytesPromise(kj::mv(promise), kj::mv(canceler)));
  }

  std::vector<uint8_t> connectionRead(kj::WaitScope& waitScope, uint32_t connectionId,
                                      uint32_t minBytes, uint32_t maxBytes) {
    auto promiseId = connectionReadStart(connectionId, minBytes, maxBytes);
    return awaitBytesPromise(waitScope, promiseId);
  }

  uint32_t connectionShutdownWriteStart(uint32_t connectionId) {
    auto it = connections_.find(connectionId);
    if (it == connections_.end()) {
      throw std::runtime_error("unknown KJ connection id: " + std::to_string(connectionId));
    }
    it->second->shutdownWrite();

    auto canceler = kj::heap<kj::Canceler>();
    kj::Promise<void> ready = kj::READY_NOW;
    auto wrapped = canceler->wrap(kj::mv(ready));
    return addPromise(PendingPromise(kj::mv(wrapped), kj::mv(canceler)));
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

  uint32_t datagramSendStart(kj::AsyncIoProvider& ioProvider, uint32_t portId,
                             const std::string& address, uint32_t portHint,
                             std::vector<uint8_t> bytes) {
    auto it = datagramPorts_.find(portId);
    if (it == datagramPorts_.end()) {
      throw std::runtime_error("unknown KJ datagram port id: " + std::to_string(portId));
    }

    auto canceler = kj::heap<kj::Canceler>();
    auto promise = canceler->wrap(
        ioProvider.getNetwork()
            .parseAddress(address.c_str(), portHint)
            .then([this, portId, bytes = std::move(bytes)](
                      kj::Own<kj::NetworkAddress>&& addr) mutable {
              auto portIt = datagramPorts_.find(portId);
              if (portIt == datagramPorts_.end()) {
                throw std::runtime_error("unknown KJ datagram port id: " + std::to_string(portId));
              }
              auto bytesCopy = kj::heapArray<kj::byte>(bytes.size());
              if (!bytes.empty()) {
                std::memcpy(bytesCopy.begin(), bytes.data(), bytes.size());
              }
              return portIt->second->send(bytesCopy.asPtr(), *addr)
                  .attach(kj::mv(bytesCopy))
                  .then([](size_t sent) {
                    if (sent > static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
                      throw std::runtime_error("datagram send byte count exceeds UInt32 range");
                    }
                    return static_cast<uint32_t>(sent);
                  });
            }));
    return addUInt32Promise(PendingUInt32Promise(kj::mv(promise), kj::mv(canceler)));
  }

  uint32_t datagramSend(kj::AsyncIoProvider& ioProvider, kj::WaitScope& waitScope, uint32_t portId,
                        const std::string& address, uint32_t portHint,
                        std::vector<uint8_t> bytes) {
    auto promiseId = datagramSendStart(ioProvider, portId, address, portHint, std::move(bytes));
    return awaitUInt32Promise(waitScope, promiseId);
  }

  uint32_t datagramReceiveStart(uint32_t portId, uint32_t maxBytes) {
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
    auto canceler = kj::heap<kj::Canceler>();
    auto promise = canceler->wrap(receiver->receive().then([receiver = kj::mv(receiver)]() mutable {
      DatagramReceiveResult result;
      auto content = receiver->getContent();
      auto contentPtr = content.value;
      result.bytes.resize(contentPtr.size());
      if (!result.bytes.empty()) {
        std::memcpy(result.bytes.data(), contentPtr.begin(), result.bytes.size());
      }
      auto source = receiver->getSource().toString();
      result.sourceAddress = source.cStr();
      return result;
    }));
    return addDatagramReceivePromise(
        PendingDatagramReceivePromise(kj::mv(promise), kj::mv(canceler)));
  }

  std::pair<std::string, std::vector<uint8_t>> datagramReceive(kj::WaitScope& waitScope,
                                                                uint32_t portId,
                                                                uint32_t maxBytes) {
    auto promiseId = datagramReceiveStart(portId, maxBytes);
    auto result = awaitDatagramReceivePromise(waitScope, promiseId);
    return {std::move(result.sourceAddress), std::move(result.bytes)};
  }

  static kj::HttpMethod decodeHttpMethod(uint32_t method) {
    switch (method) {
      case 0:
        return kj::HttpMethod::GET;
      case 1:
        return kj::HttpMethod::HEAD;
      case 2:
        return kj::HttpMethod::POST;
      case 3:
        return kj::HttpMethod::PUT;
      case 4:
        return kj::HttpMethod::DELETE;
      case 5:
        return kj::HttpMethod::PATCH;
      case 6:
        return kj::HttpMethod::OPTIONS;
      case 7:
        return kj::HttpMethod::TRACE;
      default:
        throw std::runtime_error("unknown HTTP method tag: " + std::to_string(method));
    }
  }

  static kj::Promise<HttpResponseResult> readHttpResponse(kj::HttpClient::Response&& response) {
    if (response.statusCode > std::numeric_limits<uint32_t>::max()) {
      throw std::runtime_error("HTTP status code exceeds UInt32 range");
    }
    const uint32_t statusCode = static_cast<uint32_t>(response.statusCode);
    return response.body->readAllBytes().then(
        [statusCode](kj::Array<kj::byte>&& responseBody) mutable {
          HttpResponseResult result;
          result.statusCode = statusCode;
          result.body.resize(responseBody.size());
          if (!result.body.empty()) {
            std::memcpy(result.body.data(), responseBody.begin(), result.body.size());
          }
          return result;
        });
  }

  uint32_t httpRequestStart(kj::AsyncIoProvider& ioProvider, uint32_t method,
                            const std::string& address, uint32_t portHint,
                            const std::string& path, std::vector<uint8_t> body) {
    const auto decodedMethod = decodeHttpMethod(method);
    auto canceler = kj::heap<kj::Canceler>();
    auto promise =
        canceler->wrap(ioProvider.getNetwork()
                           .parseAddress(address.c_str(), portHint)
                           .then([&ioProvider, decodedMethod, path, body = std::move(body)](
                                     kj::Own<kj::NetworkAddress>&& networkAddress) mutable
                                     -> kj::Promise<HttpResponseResult> {
                             auto headerTable = kj::heap<kj::HttpHeaderTable>();
                             auto client = kj::newHttpClient(
                                 ioProvider.getTimer(), *headerTable, *networkAddress);
                             auto headers = kj::HttpHeaders(*headerTable);
                             auto request = client->request(
                                 decodedMethod, path.c_str(), headers,
                                 body.empty()
                                     ? kj::none
                                     : kj::Maybe<uint64_t>(static_cast<uint64_t>(body.size())));

                             auto responsePromise = kj::mv(request.response);
                             if (request.body.get() == nullptr) {
                               if (!body.empty()) {
                                 throw std::runtime_error(
                                     "HTTP method does not accept a request body");
                               }
                               return kj::mv(responsePromise)
                                   .attach(kj::mv(client), kj::mv(headerTable))
                                   .then([](kj::HttpClient::Response&& response) {
                                     return readHttpResponse(kj::mv(response));
                                   });
                             }

                             kj::Promise<void> writePromise = kj::READY_NOW;
                             if (!body.empty()) {
                               auto bodyCopy = kj::heapArray<kj::byte>(body.size());
                               std::memcpy(bodyCopy.begin(), body.data(), body.size());
                               writePromise =
                                   request.body->write(bodyCopy.asPtr()).attach(kj::mv(bodyCopy));
                             }

                             return kj::mv(writePromise).then(
                                 [requestBody = kj::mv(request.body),
                                  responsePromise = kj::mv(responsePromise),
                                  client = kj::mv(client),
                                  headerTable = kj::mv(headerTable)]() mutable
                                     -> kj::Promise<HttpResponseResult> {
                                   requestBody = nullptr;
                                   return kj::mv(responsePromise)
                                       .attach(kj::mv(client), kj::mv(headerTable))
                                       .then([](kj::HttpClient::Response&& response) {
                                         return readHttpResponse(kj::mv(response));
                                       });
                                 });
                           }));
    return addHttpResponsePromise(PendingHttpResponsePromise(kj::mv(promise), kj::mv(canceler)));
  }

  std::pair<uint32_t, std::vector<uint8_t>> httpRequest(kj::AsyncIoProvider& ioProvider,
                                                         kj::WaitScope& waitScope, uint32_t method,
                                                         const std::string& address,
                                                         uint32_t portHint,
                                                         const std::string& path,
                                                         const std::vector<uint8_t>& body) {
    auto promiseId = httpRequestStart(ioProvider, method, address, portHint, path, body);
    auto result = awaitHttpResponsePromise(waitScope, promiseId);
    return {result.statusCode, std::move(result.body)};
  }

  static WebSocketMessageResult decodeWebSocketMessage(kj::WebSocket::Message&& message) {
    WebSocketMessageResult result;
    KJ_SWITCH_ONEOF(message) {
      KJ_CASE_ONEOF(text, kj::String) {
        result.tag = 0;
        result.text = text.cStr();
      }
      KJ_CASE_ONEOF(binary, kj::Array<kj::byte>) {
        result.tag = 1;
        result.bytes.resize(binary.size());
        if (!result.bytes.empty()) {
          std::memcpy(result.bytes.data(), binary.begin(), result.bytes.size());
        }
      }
      KJ_CASE_ONEOF(close, kj::WebSocket::Close) {
        result.tag = 2;
        result.closeCode = close.code;
        result.text = close.reason.cStr();
      }
    }
    return result;
  }

  static kj::Promise<RuntimeWebSocket> decodeWebSocketResponse(
      kj::HttpClient::WebSocketResponse&& response, kj::Own<RuntimeWebSocketOwner>&& owner) {
    const auto statusCode = response.statusCode;
    if (statusCode != 101) {
      if (response.webSocketOrBody.is<kj::Own<kj::AsyncInputStream>>()) {
        auto stream = kj::mv(response.webSocketOrBody.get<kj::Own<kj::AsyncInputStream>>());
        return stream->readAllText().then([statusCode](kj::String&& body) -> RuntimeWebSocket {
          throw std::runtime_error("websocket connect failed with status " +
                                   std::to_string(statusCode) + ": " +
                                   std::string(body.cStr()));
        });
      }
      throw std::runtime_error("websocket connect failed with status " +
                               std::to_string(statusCode));
    }
    if (!response.webSocketOrBody.is<kj::Own<kj::WebSocket>>()) {
      throw std::runtime_error("websocket connect returned HTTP body instead of websocket upgrade");
    }

    RuntimeWebSocket out;
    out.owner = kj::mv(owner);
    out.socket = kj::mv(response.webSocketOrBody.get<kj::Own<kj::WebSocket>>());
    return out;
  }

  uint32_t webSocketConnectStart(kj::AsyncIoProvider& ioProvider, const std::string& address,
                                 uint32_t portHint, const std::string& path) {
    auto canceler = kj::heap<kj::Canceler>();
    auto promise =
        canceler->wrap(ioProvider.getNetwork()
                           .parseAddress(address.c_str(), portHint)
                           .then([&ioProvider, path](kj::Own<kj::NetworkAddress>&& networkAddress)
                                     -> kj::Promise<RuntimeWebSocket> {
                             auto owner = kj::heap<RuntimeWebSocketOwner>();
                             owner->headerTable = kj::heap<kj::HttpHeaderTable>();
                             owner->entropySource = kj::heap<RuntimeEntropySource>();
                             kj::HttpClientSettings settings;
                             settings.entropySource = *owner->entropySource;
                             owner->client = kj::newHttpClient(
                                 ioProvider.getTimer(), *owner->headerTable, *networkAddress,
                                 settings);
                             auto headers = kj::HttpHeaders(*owner->headerTable);
                             return owner->client->openWebSocket(path.c_str(), headers).then(
                                 [owner = kj::mv(owner)](
                                     kj::HttpClient::WebSocketResponse&& response) mutable {
                                   return decodeWebSocketResponse(kj::mv(response), kj::mv(owner));
                                 });
                           }));
    return addWebSocketPromise(PendingWebSocketPromise(kj::mv(promise), kj::mv(canceler)));
  }

  uint32_t webSocketConnect(kj::AsyncIoProvider& ioProvider, kj::WaitScope& waitScope,
                            const std::string& address, uint32_t portHint,
                            const std::string& path) {
    auto promiseId = webSocketConnectStart(ioProvider, address, portHint, path);
    return awaitWebSocketPromise(waitScope, promiseId);
  }

  void webSocketRelease(uint32_t webSocketId) {
    auto erased = webSockets_.erase(webSocketId);
    if (erased == 0) {
      throw std::runtime_error("unknown KJ websocket id: " + std::to_string(webSocketId));
    }
  }

  void webSocketSendText(kj::WaitScope& waitScope, uint32_t webSocketId, const std::string& text) {
    auto it = webSockets_.find(webSocketId);
    if (it == webSockets_.end()) {
      throw std::runtime_error("unknown KJ websocket id: " + std::to_string(webSocketId));
    }
    it->second.socket->send(kj::ArrayPtr<const char>(text.data(), text.size())).wait(waitScope);
  }

  uint32_t webSocketSendTextStart(uint32_t webSocketId, std::string text) {
    auto it = webSockets_.find(webSocketId);
    if (it == webSockets_.end()) {
      throw std::runtime_error("unknown KJ websocket id: " + std::to_string(webSocketId));
    }
    auto canceler = kj::heap<kj::Canceler>();
    auto textCopy = kj::heapArray<char>(text.size());
    if (!text.empty()) {
      std::memcpy(textCopy.begin(), text.data(), text.size());
    }
    auto promise = canceler->wrap(it->second.socket->send(
                                      kj::ArrayPtr<const char>(textCopy.begin(), textCopy.size()))
                                      .attach(kj::mv(textCopy)));
    return addPromise(PendingPromise(kj::mv(promise), kj::mv(canceler)));
  }

  void webSocketSendBinary(kj::WaitScope& waitScope, uint32_t webSocketId,
                           const std::vector<uint8_t>& bytes) {
    auto it = webSockets_.find(webSocketId);
    if (it == webSockets_.end()) {
      throw std::runtime_error("unknown KJ websocket id: " + std::to_string(webSocketId));
    }
    auto ptr = kj::ArrayPtr<const kj::byte>(reinterpret_cast<const kj::byte*>(bytes.data()),
                                            bytes.size());
    it->second.socket->send(ptr).wait(waitScope);
  }

  uint32_t webSocketSendBinaryStart(uint32_t webSocketId, const std::vector<uint8_t>& bytes) {
    auto it = webSockets_.find(webSocketId);
    if (it == webSockets_.end()) {
      throw std::runtime_error("unknown KJ websocket id: " + std::to_string(webSocketId));
    }
    auto canceler = kj::heap<kj::Canceler>();
    auto bytesCopy = kj::heapArray<kj::byte>(bytes.size());
    if (!bytes.empty()) {
      std::memcpy(bytesCopy.begin(), bytes.data(), bytes.size());
    }
    auto promise =
        canceler->wrap(it->second.socket->send(bytesCopy.asPtr()).attach(kj::mv(bytesCopy)));
    return addPromise(PendingPromise(kj::mv(promise), kj::mv(canceler)));
  }

  uint32_t webSocketReceiveStart(uint32_t webSocketId) {
    auto it = webSockets_.find(webSocketId);
    if (it == webSockets_.end()) {
      throw std::runtime_error("unknown KJ websocket id: " + std::to_string(webSocketId));
    }
    auto canceler = kj::heap<kj::Canceler>();
    auto promise = canceler->wrap(
        it->second.socket->receive().then([](kj::WebSocket::Message&& message) {
          return decodeWebSocketMessage(kj::mv(message));
        }));
    return addWebSocketMessagePromise(
        PendingWebSocketMessagePromise(kj::mv(promise), kj::mv(canceler)));
  }

  WebSocketMessageResult webSocketReceive(kj::WaitScope& waitScope, uint32_t webSocketId) {
    auto promiseId = webSocketReceiveStart(webSocketId);
    return awaitWebSocketMessagePromise(waitScope, promiseId);
  }

  uint32_t webSocketCloseStart(uint32_t webSocketId, uint16_t closeCode, std::string reason) {
    auto it = webSockets_.find(webSocketId);
    if (it == webSockets_.end()) {
      throw std::runtime_error("unknown KJ websocket id: " + std::to_string(webSocketId));
    }
    auto canceler = kj::heap<kj::Canceler>();
    auto reasonCopy = kj::heapArray<char>(reason.size());
    if (!reason.empty()) {
      std::memcpy(reasonCopy.begin(), reason.data(), reason.size());
    }
    auto promise = canceler->wrap(
        it->second.socket
            ->close(closeCode, kj::StringPtr(reasonCopy.begin(), reasonCopy.size()))
            .attach(kj::mv(reasonCopy)));
    return addPromise(PendingPromise(kj::mv(promise), kj::mv(canceler)));
  }

  void webSocketClose(kj::WaitScope& waitScope, uint32_t webSocketId, uint16_t closeCode,
                      const std::string& reason) {
    auto promiseId = webSocketCloseStart(webSocketId, closeCode, reason);
    awaitPromise(waitScope, promiseId);
  }

  void webSocketDisconnect(uint32_t webSocketId) {
    auto it = webSockets_.find(webSocketId);
    if (it == webSockets_.end()) {
      throw std::runtime_error("unknown KJ websocket id: " + std::to_string(webSocketId));
    }
    it->second.socket->disconnect();
  }

  void webSocketAbort(uint32_t webSocketId) {
    auto it = webSockets_.find(webSocketId);
    if (it == webSockets_.end()) {
      throw std::runtime_error("unknown KJ websocket id: " + std::to_string(webSocketId));
    }
    it->second.socket->abort();
  }

  std::pair<uint32_t, uint32_t> newWebSocketPipe() {
    auto pipe = kj::newWebSocketPipe();
    auto first = addWebSocket(kj::mv(pipe.ends[0]));
    auto second = addWebSocket(kj::mv(pipe.ends[1]));
    return {first, second};
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
      } else if (std::holds_alternative<QueuedConnectionWriteStart>(op)) {
        completePromiseIdFailure(std::get<QueuedConnectionWriteStart>(op).completion, message);
      } else if (std::holds_alternative<QueuedConnectionRead>(op)) {
        completeBytesFailure(std::get<QueuedConnectionRead>(op).completion, message);
      } else if (std::holds_alternative<QueuedConnectionReadStart>(op)) {
        completePromiseIdFailure(std::get<QueuedConnectionReadStart>(op).completion, message);
      } else if (std::holds_alternative<QueuedBytesPromiseAwait>(op)) {
        completeBytesFailure(std::get<QueuedBytesPromiseAwait>(op).completion, message);
      } else if (std::holds_alternative<QueuedBytesPromiseCancel>(op)) {
        completeUnitFailure(std::get<QueuedBytesPromiseCancel>(op).completion, message);
      } else if (std::holds_alternative<QueuedBytesPromiseRelease>(op)) {
        completeUnitFailure(std::get<QueuedBytesPromiseRelease>(op).completion, message);
      } else if (std::holds_alternative<QueuedConnectionShutdownWrite>(op)) {
        completeUnitFailure(std::get<QueuedConnectionShutdownWrite>(op).completion, message);
      } else if (std::holds_alternative<QueuedConnectionShutdownWriteStart>(op)) {
        completePromiseIdFailure(std::get<QueuedConnectionShutdownWriteStart>(op).completion,
                                 message);
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
      } else if (std::holds_alternative<QueuedDatagramSendStart>(op)) {
        completePromiseIdFailure(std::get<QueuedDatagramSendStart>(op).completion, message);
      } else if (std::holds_alternative<QueuedUInt32PromiseAwait>(op)) {
        completeUInt32Failure(std::get<QueuedUInt32PromiseAwait>(op).completion, message);
      } else if (std::holds_alternative<QueuedUInt32PromiseCancel>(op)) {
        completeUnitFailure(std::get<QueuedUInt32PromiseCancel>(op).completion, message);
      } else if (std::holds_alternative<QueuedUInt32PromiseRelease>(op)) {
        completeUnitFailure(std::get<QueuedUInt32PromiseRelease>(op).completion, message);
      } else if (std::holds_alternative<QueuedDatagramReceive>(op)) {
        completeDatagramReceiveFailure(std::get<QueuedDatagramReceive>(op).completion, message);
      } else if (std::holds_alternative<QueuedDatagramReceiveStart>(op)) {
        completePromiseIdFailure(std::get<QueuedDatagramReceiveStart>(op).completion, message);
      } else if (std::holds_alternative<QueuedDatagramReceivePromiseAwait>(op)) {
        completeDatagramReceiveFailure(std::get<QueuedDatagramReceivePromiseAwait>(op).completion,
                                       message);
      } else if (std::holds_alternative<QueuedDatagramReceivePromiseCancel>(op)) {
        completeUnitFailure(std::get<QueuedDatagramReceivePromiseCancel>(op).completion, message);
      } else if (std::holds_alternative<QueuedDatagramReceivePromiseRelease>(op)) {
        completeUnitFailure(std::get<QueuedDatagramReceivePromiseRelease>(op).completion,
                            message);
      } else if (std::holds_alternative<QueuedHttpRequest>(op)) {
        completeHttpResponseFailure(std::get<QueuedHttpRequest>(op).completion, message);
      } else if (std::holds_alternative<QueuedHttpRequestStart>(op)) {
        completePromiseIdFailure(std::get<QueuedHttpRequestStart>(op).completion, message);
      } else if (std::holds_alternative<QueuedHttpResponsePromiseAwait>(op)) {
        completeHttpResponseFailure(std::get<QueuedHttpResponsePromiseAwait>(op).completion,
                                    message);
      } else if (std::holds_alternative<QueuedHttpResponsePromiseCancel>(op)) {
        completeUnitFailure(std::get<QueuedHttpResponsePromiseCancel>(op).completion, message);
      } else if (std::holds_alternative<QueuedHttpResponsePromiseRelease>(op)) {
        completeUnitFailure(std::get<QueuedHttpResponsePromiseRelease>(op).completion, message);
      } else if (std::holds_alternative<QueuedWebSocketConnect>(op)) {
        completeHandleFailure(std::get<QueuedWebSocketConnect>(op).completion, message);
      } else if (std::holds_alternative<QueuedWebSocketConnectStart>(op)) {
        completePromiseIdFailure(std::get<QueuedWebSocketConnectStart>(op).completion, message);
      } else if (std::holds_alternative<QueuedWebSocketPromiseAwait>(op)) {
        completeHandleFailure(std::get<QueuedWebSocketPromiseAwait>(op).completion, message);
      } else if (std::holds_alternative<QueuedWebSocketPromiseCancel>(op)) {
        completeUnitFailure(std::get<QueuedWebSocketPromiseCancel>(op).completion, message);
      } else if (std::holds_alternative<QueuedWebSocketPromiseRelease>(op)) {
        completeUnitFailure(std::get<QueuedWebSocketPromiseRelease>(op).completion, message);
      } else if (std::holds_alternative<QueuedWebSocketRelease>(op)) {
        completeUnitFailure(std::get<QueuedWebSocketRelease>(op).completion, message);
      } else if (std::holds_alternative<QueuedWebSocketSendTextStart>(op)) {
        completePromiseIdFailure(std::get<QueuedWebSocketSendTextStart>(op).completion, message);
      } else if (std::holds_alternative<QueuedWebSocketSendText>(op)) {
        completeUnitFailure(std::get<QueuedWebSocketSendText>(op).completion, message);
      } else if (std::holds_alternative<QueuedWebSocketSendBinaryStart>(op)) {
        completePromiseIdFailure(std::get<QueuedWebSocketSendBinaryStart>(op).completion, message);
      } else if (std::holds_alternative<QueuedWebSocketSendBinary>(op)) {
        completeUnitFailure(std::get<QueuedWebSocketSendBinary>(op).completion, message);
      } else if (std::holds_alternative<QueuedWebSocketReceiveStart>(op)) {
        completePromiseIdFailure(std::get<QueuedWebSocketReceiveStart>(op).completion, message);
      } else if (std::holds_alternative<QueuedWebSocketMessagePromiseAwait>(op)) {
        completeWebSocketMessageFailure(
            std::get<QueuedWebSocketMessagePromiseAwait>(op).completion, message);
      } else if (std::holds_alternative<QueuedWebSocketMessagePromiseCancel>(op)) {
        completeUnitFailure(std::get<QueuedWebSocketMessagePromiseCancel>(op).completion, message);
      } else if (std::holds_alternative<QueuedWebSocketMessagePromiseRelease>(op)) {
        completeUnitFailure(std::get<QueuedWebSocketMessagePromiseRelease>(op).completion,
                            message);
      } else if (std::holds_alternative<QueuedWebSocketReceive>(op)) {
        completeWebSocketMessageFailure(std::get<QueuedWebSocketReceive>(op).completion, message);
      } else if (std::holds_alternative<QueuedWebSocketCloseStart>(op)) {
        completePromiseIdFailure(std::get<QueuedWebSocketCloseStart>(op).completion, message);
      } else if (std::holds_alternative<QueuedWebSocketClose>(op)) {
        completeUnitFailure(std::get<QueuedWebSocketClose>(op).completion, message);
      } else if (std::holds_alternative<QueuedWebSocketDisconnect>(op)) {
        completeUnitFailure(std::get<QueuedWebSocketDisconnect>(op).completion, message);
      } else if (std::holds_alternative<QueuedWebSocketAbort>(op)) {
        completeUnitFailure(std::get<QueuedWebSocketAbort>(op).completion, message);
      } else if (std::holds_alternative<QueuedNewWebSocketPipe>(op)) {
        completeHandlePairFailure(std::get<QueuedNewWebSocketPipe>(op).completion, message);
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
        } else if (std::holds_alternative<QueuedConnectionWriteStart>(op)) {
          auto writeReq = std::get<QueuedConnectionWriteStart>(std::move(op));
          try {
            auto promiseId = connectionWriteStart(writeReq.connectionId, writeReq.bytes);
            completePromiseIdSuccess(writeReq.completion, promiseId);
          } catch (const kj::Exception& e) {
            completePromiseIdFailure(writeReq.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completePromiseIdFailure(writeReq.completion, e.what());
          } catch (...) {
            completePromiseIdFailure(
                writeReq.completion,
                "unknown exception in Capnp.KjAsync connection write start");
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
        } else if (std::holds_alternative<QueuedConnectionReadStart>(op)) {
          auto readReq = std::get<QueuedConnectionReadStart>(std::move(op));
          try {
            auto promiseId =
                connectionReadStart(readReq.connectionId, readReq.minBytes, readReq.maxBytes);
            completePromiseIdSuccess(readReq.completion, promiseId);
          } catch (const kj::Exception& e) {
            completePromiseIdFailure(readReq.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completePromiseIdFailure(readReq.completion, e.what());
          } catch (...) {
            completePromiseIdFailure(readReq.completion,
                                     "unknown exception in Capnp.KjAsync connection read start");
          }
        } else if (std::holds_alternative<QueuedBytesPromiseAwait>(op)) {
          auto await = std::get<QueuedBytesPromiseAwait>(std::move(op));
          try {
            auto bytes = awaitBytesPromise(io.waitScope, await.promiseId);
            completeBytesSuccess(await.completion, std::move(bytes));
          } catch (const kj::Exception& e) {
            completeBytesFailure(await.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeBytesFailure(await.completion, e.what());
          } catch (...) {
            completeBytesFailure(await.completion,
                                 "unknown exception in Capnp.KjAsync bytes promise await");
          }
        } else if (std::holds_alternative<QueuedBytesPromiseCancel>(op)) {
          auto cancel = std::get<QueuedBytesPromiseCancel>(std::move(op));
          try {
            cancelBytesPromise(cancel.promiseId);
            completeUnitSuccess(cancel.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(cancel.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(cancel.completion, e.what());
          } catch (...) {
            completeUnitFailure(cancel.completion,
                                "unknown exception in Capnp.KjAsync bytes promise cancel");
          }
        } else if (std::holds_alternative<QueuedBytesPromiseRelease>(op)) {
          auto release = std::get<QueuedBytesPromiseRelease>(std::move(op));
          try {
            releaseBytesPromise(release.promiseId);
            completeUnitSuccess(release.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(release.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(release.completion, e.what());
          } catch (...) {
            completeUnitFailure(release.completion,
                                "unknown exception in Capnp.KjAsync bytes promise release");
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
        } else if (std::holds_alternative<QueuedConnectionShutdownWriteStart>(op)) {
          auto shutdown = std::get<QueuedConnectionShutdownWriteStart>(std::move(op));
          try {
            auto promiseId = connectionShutdownWriteStart(shutdown.connectionId);
            completePromiseIdSuccess(shutdown.completion, promiseId);
          } catch (const kj::Exception& e) {
            completePromiseIdFailure(shutdown.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completePromiseIdFailure(shutdown.completion, e.what());
          } catch (...) {
            completePromiseIdFailure(
                shutdown.completion,
                "unknown exception in Capnp.KjAsync connection shutdownWrite start");
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
                                     send.portHint, std::move(send.bytes));
            completeUInt32Success(send.completion, sent);
          } catch (const kj::Exception& e) {
            completeUInt32Failure(send.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUInt32Failure(send.completion, e.what());
          } catch (...) {
            completeUInt32Failure(send.completion,
                                  "unknown exception in Capnp.KjAsync datagramSend");
          }
        } else if (std::holds_alternative<QueuedDatagramSendStart>(op)) {
          auto send = std::get<QueuedDatagramSendStart>(std::move(op));
          try {
            auto promiseId =
                datagramSendStart(*io.provider, send.portId, send.address, send.portHint,
                                  std::move(send.bytes));
            completePromiseIdSuccess(send.completion, promiseId);
          } catch (const kj::Exception& e) {
            completePromiseIdFailure(send.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completePromiseIdFailure(send.completion, e.what());
          } catch (...) {
            completePromiseIdFailure(send.completion,
                                     "unknown exception in Capnp.KjAsync datagramSendStart");
          }
        } else if (std::holds_alternative<QueuedUInt32PromiseAwait>(op)) {
          auto await = std::get<QueuedUInt32PromiseAwait>(std::move(op));
          try {
            auto value = awaitUInt32Promise(io.waitScope, await.promiseId);
            completeUInt32Success(await.completion, value);
          } catch (const kj::Exception& e) {
            completeUInt32Failure(await.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUInt32Failure(await.completion, e.what());
          } catch (...) {
            completeUInt32Failure(await.completion,
                                  "unknown exception in Capnp.KjAsync uint32 promise await");
          }
        } else if (std::holds_alternative<QueuedUInt32PromiseCancel>(op)) {
          auto cancel = std::get<QueuedUInt32PromiseCancel>(std::move(op));
          try {
            cancelUInt32Promise(cancel.promiseId);
            completeUnitSuccess(cancel.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(cancel.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(cancel.completion, e.what());
          } catch (...) {
            completeUnitFailure(cancel.completion,
                                "unknown exception in Capnp.KjAsync uint32 promise cancel");
          }
        } else if (std::holds_alternative<QueuedUInt32PromiseRelease>(op)) {
          auto release = std::get<QueuedUInt32PromiseRelease>(std::move(op));
          try {
            releaseUInt32Promise(release.promiseId);
            completeUnitSuccess(release.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(release.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(release.completion, e.what());
          } catch (...) {
            completeUnitFailure(release.completion,
                                "unknown exception in Capnp.KjAsync uint32 promise release");
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
        } else if (std::holds_alternative<QueuedDatagramReceiveStart>(op)) {
          auto recv = std::get<QueuedDatagramReceiveStart>(std::move(op));
          try {
            auto promiseId = datagramReceiveStart(recv.portId, recv.maxBytes);
            completePromiseIdSuccess(recv.completion, promiseId);
          } catch (const kj::Exception& e) {
            completePromiseIdFailure(recv.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completePromiseIdFailure(recv.completion, e.what());
          } catch (...) {
            completePromiseIdFailure(
                recv.completion, "unknown exception in Capnp.KjAsync datagramReceiveStart");
          }
        } else if (std::holds_alternative<QueuedDatagramReceivePromiseAwait>(op)) {
          auto await = std::get<QueuedDatagramReceivePromiseAwait>(std::move(op));
          try {
            auto result = awaitDatagramReceivePromise(io.waitScope, await.promiseId);
            completeDatagramReceiveSuccess(
                await.completion, std::move(result.sourceAddress), std::move(result.bytes));
          } catch (const kj::Exception& e) {
            completeDatagramReceiveFailure(await.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeDatagramReceiveFailure(await.completion, e.what());
          } catch (...) {
            completeDatagramReceiveFailure(
                await.completion,
                "unknown exception in Capnp.KjAsync datagram receive promise await");
          }
        } else if (std::holds_alternative<QueuedDatagramReceivePromiseCancel>(op)) {
          auto cancel = std::get<QueuedDatagramReceivePromiseCancel>(std::move(op));
          try {
            cancelDatagramReceivePromise(cancel.promiseId);
            completeUnitSuccess(cancel.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(cancel.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(cancel.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                cancel.completion,
                "unknown exception in Capnp.KjAsync datagram receive promise cancel");
          }
        } else if (std::holds_alternative<QueuedDatagramReceivePromiseRelease>(op)) {
          auto release = std::get<QueuedDatagramReceivePromiseRelease>(std::move(op));
          try {
            releaseDatagramReceivePromise(release.promiseId);
            completeUnitSuccess(release.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(release.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(release.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                release.completion,
                "unknown exception in Capnp.KjAsync datagram receive promise release");
          }
        } else if (std::holds_alternative<QueuedHttpRequest>(op)) {
          auto request = std::get<QueuedHttpRequest>(std::move(op));
          try {
            auto result = httpRequest(*io.provider, io.waitScope, request.method, request.address,
                                      request.portHint, request.path, request.body);
            completeHttpResponseSuccess(
                request.completion, result.first, std::move(result.second));
          } catch (const kj::Exception& e) {
            completeHttpResponseFailure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeHttpResponseFailure(request.completion, e.what());
          } catch (...) {
            completeHttpResponseFailure(request.completion,
                                        "unknown exception in Capnp.KjAsync httpRequest");
          }
        } else if (std::holds_alternative<QueuedHttpRequestStart>(op)) {
          auto request = std::get<QueuedHttpRequestStart>(std::move(op));
          try {
            auto promiseId =
                httpRequestStart(*io.provider, request.method, request.address, request.portHint,
                                 request.path, std::move(request.body));
            completePromiseIdSuccess(request.completion, promiseId);
          } catch (const kj::Exception& e) {
            completePromiseIdFailure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completePromiseIdFailure(request.completion, e.what());
          } catch (...) {
            completePromiseIdFailure(request.completion,
                                     "unknown exception in Capnp.KjAsync httpRequestStart");
          }
        } else if (std::holds_alternative<QueuedHttpResponsePromiseAwait>(op)) {
          auto await = std::get<QueuedHttpResponsePromiseAwait>(std::move(op));
          try {
            auto result = awaitHttpResponsePromise(io.waitScope, await.promiseId);
            completeHttpResponseSuccess(await.completion, result.statusCode, std::move(result.body));
          } catch (const kj::Exception& e) {
            completeHttpResponseFailure(await.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeHttpResponseFailure(await.completion, e.what());
          } catch (...) {
            completeHttpResponseFailure(
                await.completion, "unknown exception in Capnp.KjAsync HTTP response promise await");
          }
        } else if (std::holds_alternative<QueuedHttpResponsePromiseCancel>(op)) {
          auto cancel = std::get<QueuedHttpResponsePromiseCancel>(std::move(op));
          try {
            cancelHttpResponsePromise(cancel.promiseId);
            completeUnitSuccess(cancel.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(cancel.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(cancel.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                cancel.completion, "unknown exception in Capnp.KjAsync HTTP response promise cancel");
          }
        } else if (std::holds_alternative<QueuedHttpResponsePromiseRelease>(op)) {
          auto release = std::get<QueuedHttpResponsePromiseRelease>(std::move(op));
          try {
            releaseHttpResponsePromise(release.promiseId);
            completeUnitSuccess(release.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(release.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(release.completion, e.what());
          } catch (...) {
            completeUnitFailure(release.completion,
                                "unknown exception in Capnp.KjAsync HTTP response promise release");
          }
        } else if (std::holds_alternative<QueuedWebSocketConnect>(op)) {
          auto connectReq = std::get<QueuedWebSocketConnect>(std::move(op));
          try {
            auto webSocketId = webSocketConnect(*io.provider, io.waitScope, connectReq.address,
                                                connectReq.portHint, connectReq.path);
            completeHandleSuccess(connectReq.completion, webSocketId);
          } catch (const kj::Exception& e) {
            completeHandleFailure(connectReq.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeHandleFailure(connectReq.completion, e.what());
          } catch (...) {
            completeHandleFailure(connectReq.completion,
                                  "unknown exception in Capnp.KjAsync websocketConnect");
          }
        } else if (std::holds_alternative<QueuedWebSocketConnectStart>(op)) {
          auto connectReq = std::get<QueuedWebSocketConnectStart>(std::move(op));
          try {
            auto promiseId = webSocketConnectStart(*io.provider, connectReq.address,
                                                   connectReq.portHint, connectReq.path);
            completePromiseIdSuccess(connectReq.completion, promiseId);
          } catch (const kj::Exception& e) {
            completePromiseIdFailure(connectReq.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completePromiseIdFailure(connectReq.completion, e.what());
          } catch (...) {
            completePromiseIdFailure(connectReq.completion,
                                     "unknown exception in Capnp.KjAsync websocketConnectStart");
          }
        } else if (std::holds_alternative<QueuedWebSocketPromiseAwait>(op)) {
          auto await = std::get<QueuedWebSocketPromiseAwait>(std::move(op));
          try {
            auto webSocketId = awaitWebSocketPromise(io.waitScope, await.promiseId);
            completeHandleSuccess(await.completion, webSocketId);
          } catch (const kj::Exception& e) {
            completeHandleFailure(await.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeHandleFailure(await.completion, e.what());
          } catch (...) {
            completeHandleFailure(await.completion,
                                  "unknown exception in Capnp.KjAsync websocket promise await");
          }
        } else if (std::holds_alternative<QueuedWebSocketPromiseCancel>(op)) {
          auto cancel = std::get<QueuedWebSocketPromiseCancel>(std::move(op));
          try {
            cancelWebSocketPromise(cancel.promiseId);
            completeUnitSuccess(cancel.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(cancel.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(cancel.completion, e.what());
          } catch (...) {
            completeUnitFailure(cancel.completion,
                                "unknown exception in Capnp.KjAsync websocket promise cancel");
          }
        } else if (std::holds_alternative<QueuedWebSocketPromiseRelease>(op)) {
          auto release = std::get<QueuedWebSocketPromiseRelease>(std::move(op));
          try {
            releaseWebSocketPromise(release.promiseId);
            completeUnitSuccess(release.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(release.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(release.completion, e.what());
          } catch (...) {
            completeUnitFailure(release.completion,
                                "unknown exception in Capnp.KjAsync websocket promise release");
          }
        } else if (std::holds_alternative<QueuedWebSocketRelease>(op)) {
          auto release = std::get<QueuedWebSocketRelease>(std::move(op));
          try {
            webSocketRelease(release.webSocketId);
            completeUnitSuccess(release.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(release.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(release.completion, e.what());
          } catch (...) {
            completeUnitFailure(release.completion,
                                "unknown exception in Capnp.KjAsync websocketRelease");
          }
        } else if (std::holds_alternative<QueuedWebSocketSendTextStart>(op)) {
          auto send = std::get<QueuedWebSocketSendTextStart>(std::move(op));
          try {
            auto promiseId = webSocketSendTextStart(send.webSocketId, std::move(send.text));
            completePromiseIdSuccess(send.completion, promiseId);
          } catch (const kj::Exception& e) {
            completePromiseIdFailure(send.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completePromiseIdFailure(send.completion, e.what());
          } catch (...) {
            completePromiseIdFailure(send.completion,
                                     "unknown exception in Capnp.KjAsync websocketSendTextStart");
          }
        } else if (std::holds_alternative<QueuedWebSocketSendText>(op)) {
          auto send = std::get<QueuedWebSocketSendText>(std::move(op));
          try {
            webSocketSendText(io.waitScope, send.webSocketId, send.text);
            completeUnitSuccess(send.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(send.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(send.completion, e.what());
          } catch (...) {
            completeUnitFailure(send.completion,
                                "unknown exception in Capnp.KjAsync websocketSendText");
          }
        } else if (std::holds_alternative<QueuedWebSocketSendBinaryStart>(op)) {
          auto send = std::get<QueuedWebSocketSendBinaryStart>(std::move(op));
          try {
            auto promiseId = webSocketSendBinaryStart(send.webSocketId, send.bytes);
            completePromiseIdSuccess(send.completion, promiseId);
          } catch (const kj::Exception& e) {
            completePromiseIdFailure(send.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completePromiseIdFailure(send.completion, e.what());
          } catch (...) {
            completePromiseIdFailure(send.completion,
                                     "unknown exception in Capnp.KjAsync websocketSendBinaryStart");
          }
        } else if (std::holds_alternative<QueuedWebSocketSendBinary>(op)) {
          auto send = std::get<QueuedWebSocketSendBinary>(std::move(op));
          try {
            webSocketSendBinary(io.waitScope, send.webSocketId, send.bytes);
            completeUnitSuccess(send.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(send.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(send.completion, e.what());
          } catch (...) {
            completeUnitFailure(send.completion,
                                "unknown exception in Capnp.KjAsync websocketSendBinary");
          }
        } else if (std::holds_alternative<QueuedWebSocketReceiveStart>(op)) {
          auto recv = std::get<QueuedWebSocketReceiveStart>(std::move(op));
          try {
            auto promiseId = webSocketReceiveStart(recv.webSocketId);
            completePromiseIdSuccess(recv.completion, promiseId);
          } catch (const kj::Exception& e) {
            completePromiseIdFailure(recv.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completePromiseIdFailure(recv.completion, e.what());
          } catch (...) {
            completePromiseIdFailure(recv.completion,
                                     "unknown exception in Capnp.KjAsync websocketReceiveStart");
          }
        } else if (std::holds_alternative<QueuedWebSocketMessagePromiseAwait>(op)) {
          auto await = std::get<QueuedWebSocketMessagePromiseAwait>(std::move(op));
          try {
            auto message = awaitWebSocketMessagePromise(io.waitScope, await.promiseId);
            completeWebSocketMessageSuccess(await.completion, message.tag, message.closeCode,
                                            std::move(message.text), std::move(message.bytes));
          } catch (const kj::Exception& e) {
            completeWebSocketMessageFailure(await.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeWebSocketMessageFailure(await.completion, e.what());
          } catch (...) {
            completeWebSocketMessageFailure(
                await.completion,
                "unknown exception in Capnp.KjAsync websocket message promise await");
          }
        } else if (std::holds_alternative<QueuedWebSocketMessagePromiseCancel>(op)) {
          auto cancel = std::get<QueuedWebSocketMessagePromiseCancel>(std::move(op));
          try {
            cancelWebSocketMessagePromise(cancel.promiseId);
            completeUnitSuccess(cancel.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(cancel.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(cancel.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                cancel.completion,
                "unknown exception in Capnp.KjAsync websocket message promise cancel");
          }
        } else if (std::holds_alternative<QueuedWebSocketMessagePromiseRelease>(op)) {
          auto release = std::get<QueuedWebSocketMessagePromiseRelease>(std::move(op));
          try {
            releaseWebSocketMessagePromise(release.promiseId);
            completeUnitSuccess(release.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(release.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(release.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                release.completion,
                "unknown exception in Capnp.KjAsync websocket message promise release");
          }
        } else if (std::holds_alternative<QueuedWebSocketReceive>(op)) {
          auto recv = std::get<QueuedWebSocketReceive>(std::move(op));
          try {
            auto message = webSocketReceive(io.waitScope, recv.webSocketId);
            completeWebSocketMessageSuccess(recv.completion, message.tag, message.closeCode,
                                            std::move(message.text), std::move(message.bytes));
          } catch (const kj::Exception& e) {
            completeWebSocketMessageFailure(recv.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeWebSocketMessageFailure(recv.completion, e.what());
          } catch (...) {
            completeWebSocketMessageFailure(recv.completion,
                                            "unknown exception in Capnp.KjAsync websocketReceive");
          }
        } else if (std::holds_alternative<QueuedWebSocketCloseStart>(op)) {
          auto close = std::get<QueuedWebSocketCloseStart>(std::move(op));
          try {
            auto promiseId =
                webSocketCloseStart(close.webSocketId, close.closeCode, std::move(close.reason));
            completePromiseIdSuccess(close.completion, promiseId);
          } catch (const kj::Exception& e) {
            completePromiseIdFailure(close.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completePromiseIdFailure(close.completion, e.what());
          } catch (...) {
            completePromiseIdFailure(close.completion,
                                     "unknown exception in Capnp.KjAsync websocketCloseStart");
          }
        } else if (std::holds_alternative<QueuedWebSocketClose>(op)) {
          auto close = std::get<QueuedWebSocketClose>(std::move(op));
          try {
            webSocketClose(io.waitScope, close.webSocketId, close.closeCode, close.reason);
            completeUnitSuccess(close.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(close.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(close.completion, e.what());
          } catch (...) {
            completeUnitFailure(close.completion,
                                "unknown exception in Capnp.KjAsync websocketClose");
          }
        } else if (std::holds_alternative<QueuedWebSocketDisconnect>(op)) {
          auto disconnect = std::get<QueuedWebSocketDisconnect>(std::move(op));
          try {
            webSocketDisconnect(disconnect.webSocketId);
            completeUnitSuccess(disconnect.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(disconnect.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(disconnect.completion, e.what());
          } catch (...) {
            completeUnitFailure(disconnect.completion,
                                "unknown exception in Capnp.KjAsync websocketDisconnect");
          }
        } else if (std::holds_alternative<QueuedWebSocketAbort>(op)) {
          auto abort = std::get<QueuedWebSocketAbort>(std::move(op));
          try {
            webSocketAbort(abort.webSocketId);
            completeUnitSuccess(abort.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(abort.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(abort.completion, e.what());
          } catch (...) {
            completeUnitFailure(abort.completion,
                                "unknown exception in Capnp.KjAsync websocketAbort");
          }
        } else if (std::holds_alternative<QueuedNewWebSocketPipe>(op)) {
          auto create = std::get<QueuedNewWebSocketPipe>(std::move(op));
          try {
            auto pair = newWebSocketPipe();
            completeHandlePairSuccess(create.completion, pair.first, pair.second);
          } catch (const kj::Exception& e) {
            completeHandlePairFailure(create.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeHandlePairFailure(create.completion, e.what());
          } catch (...) {
            completeHandlePairFailure(create.completion,
                                      "unknown exception in Capnp.KjAsync newWebSocketPipe");
          }
        }
      }

      promises_.clear();
      connectionPromises_.clear();
      bytesPromises_.clear();
      uint32Promises_.clear();
      datagramReceivePromises_.clear();
      httpResponsePromises_.clear();
      webSocketPromises_.clear();
      webSocketMessagePromises_.clear();
      listeners_.clear();
      connections_.clear();
      taskSets_.clear();
      datagramPorts_.clear();
      webSockets_.clear();
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
  uint32_t nextBytesPromiseId_ = 1;
  std::unordered_map<uint32_t, PendingBytesPromise> bytesPromises_;
  uint32_t nextUInt32PromiseId_ = 1;
  std::unordered_map<uint32_t, PendingUInt32Promise> uint32Promises_;
  uint32_t nextDatagramReceivePromiseId_ = 1;
  std::unordered_map<uint32_t, PendingDatagramReceivePromise> datagramReceivePromises_;
  uint32_t nextHttpResponsePromiseId_ = 1;
  std::unordered_map<uint32_t, PendingHttpResponsePromise> httpResponsePromises_;
  uint32_t nextWebSocketPromiseId_ = 1;
  std::unordered_map<uint32_t, PendingWebSocketPromise> webSocketPromises_;
  uint32_t nextWebSocketMessagePromiseId_ = 1;
  std::unordered_map<uint32_t, PendingWebSocketMessagePromise> webSocketMessagePromises_;
  uint32_t nextListenerId_ = 1;
  std::unordered_map<uint32_t, kj::Own<kj::ConnectionReceiver>> listeners_;
  uint32_t nextConnectionId_ = 1;
  std::unordered_map<uint32_t, kj::Own<kj::AsyncIoStream>> connections_;
  uint32_t nextTaskSetId_ = 1;
  std::unordered_map<uint32_t, kj::Own<RuntimeTaskSet>> taskSets_;
  uint32_t nextDatagramPortId_ = 1;
  std::unordered_map<uint32_t, kj::Own<kj::DatagramPort>> datagramPorts_;
  uint32_t nextWebSocketId_ = 1;
  std::unordered_map<uint32_t, RuntimeWebSocket> webSockets_;
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

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_connection_write_start(
    uint64_t runtimeId, uint32_t connectionId, b_lean_obj_arg bytes) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueConnectionWriteStart(connectionId, copyByteArray(bytes));
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
        "unknown exception in capnp_lean_kj_async_runtime_connection_write_start");
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

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_connection_read_start(
    uint64_t runtimeId, uint32_t connectionId, uint32_t minBytes, uint32_t maxBytes) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueConnectionReadStart(connectionId, minBytes, maxBytes);
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
        "unknown exception in capnp_lean_kj_async_runtime_connection_read_start");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_bytes_promise_await(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueBytesPromiseAwait(promiseId);
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_bytes_promise_await");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_bytes_promise_cancel(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueBytesPromiseCancel(promiseId);
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_bytes_promise_cancel");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_bytes_promise_release(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueBytesPromiseRelease(promiseId);
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
        "unknown exception in capnp_lean_kj_async_runtime_bytes_promise_release");
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

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_connection_shutdown_write_start(
    uint64_t runtimeId, uint32_t connectionId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueConnectionShutdownWriteStart(connectionId);
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
        "unknown exception in capnp_lean_kj_async_runtime_connection_shutdown_write_start");
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

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_datagram_send_start(
    uint64_t runtimeId, uint32_t portId, b_lean_obj_arg address, uint32_t portHint,
    b_lean_obj_arg bytes) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueDatagramSendStart(
        portId, std::string(lean_string_cstr(address)), portHint, copyByteArray(bytes));
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_datagram_send_start");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_uint32_promise_await(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueUInt32PromiseAwait(promiseId);
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_uint32_promise_await");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_uint32_promise_cancel(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueUInt32PromiseCancel(promiseId);
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
        "unknown exception in capnp_lean_kj_async_runtime_uint32_promise_cancel");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_uint32_promise_release(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueUInt32PromiseRelease(promiseId);
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
        "unknown exception in capnp_lean_kj_async_runtime_uint32_promise_release");
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

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_datagram_receive_start(
    uint64_t runtimeId, uint32_t portId, uint32_t maxBytes) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueDatagramReceiveStart(portId, maxBytes);
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
        "unknown exception in capnp_lean_kj_async_runtime_datagram_receive_start");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_datagram_receive_promise_await(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueDatagramReceivePromiseAwait(promiseId);
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
    return mkIoUserError(
        "unknown exception in capnp_lean_kj_async_runtime_datagram_receive_promise_await");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_datagram_receive_promise_cancel(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueDatagramReceivePromiseCancel(promiseId);
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
        "unknown exception in capnp_lean_kj_async_runtime_datagram_receive_promise_cancel");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_datagram_receive_promise_release(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueDatagramReceivePromiseRelease(promiseId);
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
        "unknown exception in capnp_lean_kj_async_runtime_datagram_receive_promise_release");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_http_request(
    uint64_t runtimeId, uint32_t method, b_lean_obj_arg address, uint32_t portHint,
    b_lean_obj_arg path, b_lean_obj_arg body) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueHttpRequest(
        method, std::string(lean_string_cstr(address)), portHint, std::string(lean_string_cstr(path)),
        copyByteArray(body));
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }

      auto pair = lean_alloc_ctor(0, 2, 0);
      lean_ctor_set(pair, 0, lean_box_uint32(completion->statusCode));
      lean_ctor_set(pair, 1, mkByteArrayCopy(completion->body.data(), completion->body.size()));
      return lean_io_result_mk_ok(pair);
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_http_request");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_http_request_start(
    uint64_t runtimeId, uint32_t method, b_lean_obj_arg address, uint32_t portHint,
    b_lean_obj_arg path, b_lean_obj_arg body) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueHttpRequestStart(
        method, std::string(lean_string_cstr(address)), portHint, std::string(lean_string_cstr(path)),
        copyByteArray(body));
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_http_request_start");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_http_response_promise_await(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueHttpResponsePromiseAwait(promiseId);
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }

      auto pair = lean_alloc_ctor(0, 2, 0);
      lean_ctor_set(pair, 0, lean_box_uint32(completion->statusCode));
      lean_ctor_set(pair, 1, mkByteArrayCopy(completion->body.data(), completion->body.size()));
      return lean_io_result_mk_ok(pair);
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError(
        "unknown exception in capnp_lean_kj_async_runtime_http_response_promise_await");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_http_response_promise_cancel(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueHttpResponsePromiseCancel(promiseId);
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
        "unknown exception in capnp_lean_kj_async_runtime_http_response_promise_cancel");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_http_response_promise_release(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueHttpResponsePromiseRelease(promiseId);
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
        "unknown exception in capnp_lean_kj_async_runtime_http_response_promise_release");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_websocket_connect(
    uint64_t runtimeId, b_lean_obj_arg address, uint32_t portHint, b_lean_obj_arg path) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueWebSocketConnect(
        std::string(lean_string_cstr(address)), portHint, std::string(lean_string_cstr(path)));
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_websocket_connect");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_websocket_connect_start(
    uint64_t runtimeId, b_lean_obj_arg address, uint32_t portHint, b_lean_obj_arg path) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueWebSocketConnectStart(
        std::string(lean_string_cstr(address)), portHint, std::string(lean_string_cstr(path)));
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
        "unknown exception in capnp_lean_kj_async_runtime_websocket_connect_start");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_websocket_promise_await(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueWebSocketPromiseAwait(promiseId);
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_websocket_promise_await");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_websocket_promise_cancel(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueWebSocketPromiseCancel(promiseId);
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_websocket_promise_cancel");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_websocket_promise_release(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueWebSocketPromiseRelease(promiseId);
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_websocket_promise_release");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_websocket_release(
    uint64_t runtimeId, uint32_t webSocketId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueWebSocketRelease(webSocketId);
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_websocket_release");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_websocket_send_text_start(
    uint64_t runtimeId, uint32_t webSocketId, b_lean_obj_arg text) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion =
        runtime->enqueueWebSocketSendTextStart(webSocketId, std::string(lean_string_cstr(text)));
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
        "unknown exception in capnp_lean_kj_async_runtime_websocket_send_text_start");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_websocket_send_text(
    uint64_t runtimeId, uint32_t webSocketId, b_lean_obj_arg text) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueWebSocketSendText(webSocketId, std::string(lean_string_cstr(text)));
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_websocket_send_text");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_websocket_send_binary_start(
    uint64_t runtimeId, uint32_t webSocketId, b_lean_obj_arg bytes) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueWebSocketSendBinaryStart(webSocketId, copyByteArray(bytes));
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
        "unknown exception in capnp_lean_kj_async_runtime_websocket_send_binary_start");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_websocket_send_binary(
    uint64_t runtimeId, uint32_t webSocketId, b_lean_obj_arg bytes) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueWebSocketSendBinary(webSocketId, copyByteArray(bytes));
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_websocket_send_binary");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_websocket_receive_start(
    uint64_t runtimeId, uint32_t webSocketId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueWebSocketReceiveStart(webSocketId);
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
        "unknown exception in capnp_lean_kj_async_runtime_websocket_receive_start");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_websocket_message_promise_await(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueWebSocketMessagePromiseAwait(promiseId);
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      auto pairInner = lean_alloc_ctor(0, 2, 0);
      lean_ctor_set(pairInner, 0, lean_mk_string(completion->text.c_str()));
      lean_ctor_set(pairInner, 1,
                    mkByteArrayCopy(completion->bytes.data(), completion->bytes.size()));
      auto pairMid = lean_alloc_ctor(0, 2, 0);
      lean_ctor_set(pairMid, 0, lean_box_uint32(completion->closeCode));
      lean_ctor_set(pairMid, 1, pairInner);
      auto pairOuter = lean_alloc_ctor(0, 2, 0);
      lean_ctor_set(pairOuter, 0, lean_box_uint32(completion->tag));
      lean_ctor_set(pairOuter, 1, pairMid);
      return lean_io_result_mk_ok(pairOuter);
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError(
        "unknown exception in capnp_lean_kj_async_runtime_websocket_message_promise_await");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_websocket_message_promise_cancel(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueWebSocketMessagePromiseCancel(promiseId);
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
        "unknown exception in capnp_lean_kj_async_runtime_websocket_message_promise_cancel");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_websocket_message_promise_release(
    uint64_t runtimeId, uint32_t promiseId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueWebSocketMessagePromiseRelease(promiseId);
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
        "unknown exception in capnp_lean_kj_async_runtime_websocket_message_promise_release");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_websocket_receive(
    uint64_t runtimeId, uint32_t webSocketId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueWebSocketReceive(webSocketId);
    {
      std::unique_lock<std::mutex> lock(completion->mutex);
      completion->cv.wait(lock, [&completion]() { return completion->done; });
      if (!completion->ok) {
        return mkIoUserError(completion->error);
      }
      auto pairInner = lean_alloc_ctor(0, 2, 0);
      lean_ctor_set(pairInner, 0, lean_mk_string(completion->text.c_str()));
      lean_ctor_set(pairInner, 1,
                    mkByteArrayCopy(completion->bytes.data(), completion->bytes.size()));
      auto pairMid = lean_alloc_ctor(0, 2, 0);
      lean_ctor_set(pairMid, 0, lean_box_uint32(completion->closeCode));
      lean_ctor_set(pairMid, 1, pairInner);
      auto pairOuter = lean_alloc_ctor(0, 2, 0);
      lean_ctor_set(pairOuter, 0, lean_box_uint32(completion->tag));
      lean_ctor_set(pairOuter, 1, pairMid);
      return lean_io_result_mk_ok(pairOuter);
    }
  } catch (const kj::Exception& e) {
    return mkIoUserError(describeKjException(e));
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_websocket_receive");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_websocket_close_start(
    uint64_t runtimeId, uint32_t webSocketId, uint32_t closeCode, b_lean_obj_arg reason) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }
  if (closeCode > std::numeric_limits<uint16_t>::max()) {
    return mkIoUserError("websocket close code exceeds UInt16 range");
  }

  try {
    auto completion = runtime->enqueueWebSocketCloseStart(
        webSocketId, static_cast<uint16_t>(closeCode), std::string(lean_string_cstr(reason)));
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
        "unknown exception in capnp_lean_kj_async_runtime_websocket_close_start");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_websocket_close(
    uint64_t runtimeId, uint32_t webSocketId, uint32_t closeCode, b_lean_obj_arg reason) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }
  if (closeCode > std::numeric_limits<uint16_t>::max()) {
    return mkIoUserError("websocket close code exceeds UInt16 range");
  }

  try {
    auto completion = runtime->enqueueWebSocketClose(
        webSocketId, static_cast<uint16_t>(closeCode), std::string(lean_string_cstr(reason)));
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_websocket_close");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_websocket_disconnect(
    uint64_t runtimeId, uint32_t webSocketId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueWebSocketDisconnect(webSocketId);
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
        "unknown exception in capnp_lean_kj_async_runtime_websocket_disconnect");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_websocket_abort(
    uint64_t runtimeId, uint32_t webSocketId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueWebSocketAbort(webSocketId);
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_websocket_abort");
  }
}

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_kj_async_runtime_new_websocket_pipe(
    uint64_t runtimeId) {
  auto runtime = getKjAsyncRuntime(runtimeId);
  if (!runtime) {
    return mkIoUserError("Capnp.KjAsync runtime handle is invalid or already released");
  }

  try {
    auto completion = runtime->enqueueNewWebSocketPipe();
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
    return mkIoUserError("unknown exception in capnp_lean_kj_async_runtime_new_websocket_pipe");
  }
}
