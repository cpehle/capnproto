#include <lean/lean.h>

#include <kj/async.h>
#include <kj/async-io.h>
#include <kj/time.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
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

namespace {

lean_obj_res mkIoUserError(const std::string& message) {
  lean_object* msg = lean_mk_string(message.c_str());
  lean_object* err = lean_mk_io_user_error(msg);
  return lean_io_result_mk_error(err);
}

void mkIoOkUnit(lean_obj_res& out) { out = lean_io_result_mk_ok(lean_box(0)); }

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

  using QueuedOperation = std::variant<QueuedSleepNanos, QueuedAwaitPromise, QueuedCancelPromise,
                                       QueuedReleasePromise>;

  uint32_t addPromise(PendingPromise&& promise) {
    uint32_t promiseId = nextPromiseId_++;
    while (promises_.find(promiseId) != promises_.end()) {
      promiseId = nextPromiseId_++;
    }
    promises_.emplace(promiseId, std::move(promise));
    return promiseId;
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
        }
      }

      promises_.clear();
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
