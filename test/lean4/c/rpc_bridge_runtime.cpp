#include "rpc_bridge_runtime.h"

#include <capnp/any.h>
#include <capnp/capability.h>
#include <capnp/rpc.h>
#include <capnp/rpc-twoparty.h>
#include <capnp/serialize.h>
#include <kj/async.h>
#include <kj/async-queue.h>
#include <kj/async-io.h>
#if _WIN32
#include <kj/async-win32.h>
#else
#include <kj/async-unix.h>
#endif
#include <kj/io.h>
#include <kj/map.h>
#include <kj/time.h>
#include <kj/vector.h>
#include <capnp/test.capnp.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
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
#include <utility>
#include <variant>
#include <vector>
#if !defined(_WIN32)
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace capnp_lean_rpc {

using TwoPartyRpcSystem = capnp::RpcSystem<capnp::rpc::twoparty::VatId>;
namespace capnp_test = ::capnproto_test::capnp::test;

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

const char* kjExceptionTypeName(kj::Exception::Type type) {
  switch (type) {
    case kj::Exception::Type::FAILED:
      return "FAILED";
    case kj::Exception::Type::OVERLOADED:
      return "OVERLOADED";
    case kj::Exception::Type::DISCONNECTED:
      return "DISCONNECTED";
    case kj::Exception::Type::UNIMPLEMENTED:
      return "UNIMPLEMENTED";
    default:
      return "OTHER";
  }
}

std::string describeKjException(const kj::Exception& e) {
  std::string message(e.getDescription().cStr());
  message += "\nexception type: ";
  message += kjExceptionTypeName(e.getType());
  auto remoteTrace = e.getRemoteTrace();
  if (remoteTrace != nullptr) {
    message += "\nremote trace: ";
    message += remoteTrace.cStr();
  }
  KJ_IF_SOME(detail, e.getDetail(1)) {
    message += "\nremote detail[1]: ";
    message.append(reinterpret_cast<const char*>(detail.begin()), detail.size());
  }
  return message;
}

uint32_t readUint32Le(const uint8_t* data) {
  return static_cast<uint32_t>(data[0]) |
         (static_cast<uint32_t>(data[1]) << 8) |
         (static_cast<uint32_t>(data[2]) << 16) |
         (static_cast<uint32_t>(data[3]) << 24);
}

uint16_t readUint16Le(const uint8_t* data) {
  return static_cast<uint16_t>(data[0]) |
         (static_cast<uint16_t>(data[1]) << 8);
}

uint64_t readUint64Le(const uint8_t* data) {
  return static_cast<uint64_t>(data[0]) |
         (static_cast<uint64_t>(data[1]) << 8) |
         (static_cast<uint64_t>(data[2]) << 16) |
         (static_cast<uint64_t>(data[3]) << 24) |
         (static_cast<uint64_t>(data[4]) << 32) |
         (static_cast<uint64_t>(data[5]) << 40) |
         (static_cast<uint64_t>(data[6]) << 48) |
         (static_cast<uint64_t>(data[7]) << 56);
}

void appendUint32Le(std::vector<uint8_t>& out, uint32_t value) {
  out.push_back(static_cast<uint8_t>(value & 0xff));
  out.push_back(static_cast<uint8_t>((value >> 8) & 0xff));
  out.push_back(static_cast<uint8_t>((value >> 16) & 0xff));
  out.push_back(static_cast<uint8_t>((value >> 24) & 0xff));
}

void appendUint64Le(std::vector<uint8_t>& out, uint64_t value) {
  out.push_back(static_cast<uint8_t>(value & 0xff));
  out.push_back(static_cast<uint8_t>((value >> 8) & 0xff));
  out.push_back(static_cast<uint8_t>((value >> 16) & 0xff));
  out.push_back(static_cast<uint8_t>((value >> 24) & 0xff));
  out.push_back(static_cast<uint8_t>((value >> 32) & 0xff));
  out.push_back(static_cast<uint8_t>((value >> 40) & 0xff));
  out.push_back(static_cast<uint8_t>((value >> 48) & 0xff));
  out.push_back(static_cast<uint8_t>((value >> 56) & 0xff));
}

struct LeanVatId {
  std::string host;
  bool unique = false;
};

struct LeanThirdPartyContact {
  LeanVatId path;
  uint64_t token = 0;
  std::string sentBy;
};

LeanVatId decodeLeanVatId(capnp_test::TestSturdyRefHostId::Reader data) {
  LeanVatId vatId;
  vatId.host = std::string(data.getHost().cStr());
  vatId.unique = data.getUnique();
  return vatId;
}

void setLeanVatId(capnp_test::TestSturdyRefHostId::Builder builder, const LeanVatId& vatId) {
  builder.setHost(vatId.host.c_str());
  builder.setUnique(vatId.unique);
}

void setThirdPartyToken(capnp_test::TestThirdPartyCompletion::Builder builder, uint64_t token) {
  builder.setToken(token);
}

void setThirdPartyToken(capnp_test::TestThirdPartyToAwait::Builder builder, uint64_t token) {
  builder.setToken(token);
}

uint64_t decodeThirdPartyToken(capnp_test::TestThirdPartyCompletion::Reader data) {
  return data.getToken();
}

uint64_t decodeThirdPartyToken(capnp_test::TestThirdPartyToAwait::Reader data) {
  return data.getToken();
}

LeanThirdPartyContact decodeThirdPartyContact(capnp_test::TestThirdPartyToContact::Reader data) {
  LeanThirdPartyContact contact;
  contact.path = decodeLeanVatId(data.getPath());
  contact.token = data.getToken();
  contact.sentBy = std::string(data.getSentBy().cStr());
  return contact;
}

void setThirdPartyContact(capnp_test::TestThirdPartyToContact::Builder builder,
                          const LeanThirdPartyContact& contact) {
  auto path = builder.initPath();
  setLeanVatId(path, contact.path);
  builder.setToken(contact.token);
  builder.setSentBy(contact.sentBy.c_str());
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

std::vector<uint32_t> decodeCapTable(b_lean_obj_arg bytes) {
  const auto size = lean_sarray_size(bytes);
  const auto* data =
      reinterpret_cast<const uint8_t*>(lean_sarray_cptr(const_cast<lean_object*>(bytes)));
  return decodeCapTable(data, size);
}

std::vector<uint16_t> decodePipelineOps(const uint8_t* data, size_t size) {
  if ((size % 2) != 0) {
    throw std::runtime_error("RPC pipeline ops payload must be a multiple of 2 bytes");
  }
  std::vector<uint16_t> ops;
  ops.reserve(size / 2);
  for (size_t i = 0; i < size; i += 2) {
    ops.push_back(readUint16Le(data + i));
  }
  return ops;
}

std::vector<uint16_t> decodePipelineOps(b_lean_obj_arg bytes) {
  const auto size = lean_sarray_size(bytes);
  const auto* data =
      reinterpret_cast<const uint8_t*>(lean_sarray_cptr(const_cast<lean_object*>(bytes)));
  return decodePipelineOps(data, size);
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

struct DeferredLeanTask {
  explicit DeferredLeanTask(lean_object* task): task(task) {}
  ~DeferredLeanTask() { lean_dec(task); }
  lean_object* task;
};

struct DeferredLeanTaskState {
  DeferredLeanTaskState(kj::Own<DeferredLeanTask>&& waitTask,
                        kj::Own<DeferredLeanTask>&& cancelTask,
                        bool allowCancellation)
      : waitTask(kj::mv(waitTask)),
        cancelTask(kj::mv(cancelTask)),
        allowCancellation(allowCancellation) {}

  bool requestCancellation() {
    if (!allowCancellation || completed.load(std::memory_order_acquire)) {
      return false;
    }
    if (cancellationRequested.exchange(true, std::memory_order_acq_rel)) {
      return false;
    }
    lean_io_cancel_core(cancelTask->task);
    if (waitTask.get() != cancelTask.get()) {
      lean_io_cancel_core(waitTask->task);
    }
    return true;
  }

  ~DeferredLeanTaskState() { requestCancellation(); }

  kj::Own<DeferredLeanTask> waitTask;
  kj::Own<DeferredLeanTask> cancelTask;
  bool allowCancellation;
  std::atomic<bool> completed = false;
  std::atomic<bool> cancellationRequested = false;
};

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

[[noreturn]] void throwRemoteException(kj::Exception::Type type, const std::string& message,
                                       const std::vector<uint8_t>& detailBytes) {
  auto ex = kj::Exception(type, __FILE__, __LINE__, kj::str(message.c_str()));
  if (!detailBytes.empty()) {
    auto copy = kj::heapArray<kj::byte>(detailBytes.size());
    std::memcpy(copy.begin(), detailBytes.data(), detailBytes.size());
    ex.setDetail(1, kj::mv(copy));
  }
  throw kj::mv(ex);
}

class OneShotCaptureServer final : public capnp::Capability::Server {
 public:
  OneShotCaptureServer(uint64_t expectedInterfaceId, uint16_t expectedMethodId,
                       kj::Own<kj::PromiseFulfiller<RawCallResult>> fulfiller,
                       uint32_t delayMillis = 0, bool throwException = false,
                       bool throwWithDetail = false)
      : expectedInterfaceId_(expectedInterfaceId),
        expectedMethodId_(expectedMethodId),
        fulfiller_(kj::mv(fulfiller)),
        delayMillis_(delayMillis),
        throwException_(throwException),
        throwWithDetail_(throwWithDetail) {}

  DispatchCallResult dispatchCall(
      uint64_t interfaceId, uint16_t methodId,
      capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer> context) override {
    if (interfaceId != expectedInterfaceId_ || methodId != expectedMethodId_) {
      throw std::runtime_error("unexpected method in capnp_lean_rpc_cpp_serve_echo_once");
    }

    capnp::MallocMessageBuilder requestMessage;
    capnp::BuilderCapabilityTable requestCapTable;
    requestCapTable
        .imbue(requestMessage.getRoot<capnp::AnyPointer>())
        .setAs<capnp::AnyPointer>(context.getParams().getAs<capnp::AnyPointer>());

    auto requestWords = capnp::messageToFlatArray(requestMessage);
    auto requestBytes = requestWords.asBytes();
    std::vector<uint8_t> requestCopy(requestBytes.begin(), requestBytes.end());

    std::vector<uint8_t> requestCaps;
    auto requestCapEntries = requestCapTable.getTable();
    requestCaps.reserve(requestCapEntries.size() * 4);
    for (auto& maybeHook : requestCapEntries) {
      KJ_IF_SOME(_hook, maybeHook) {
        throw std::runtime_error(
            "capnp_lean_rpc_cpp_serve_echo_once does not support capability arguments");
      } else {
        appendUint32Le(requestCaps, 0);
      }
    }

    RawCallResult captured{std::move(requestCopy), std::move(requestCaps)};
    auto fulfillCaptured = [&]() {
      if (!fulfilled_) {
        fulfiller_->fulfill(kj::mv(captured));
        fulfilled_ = true;
      }
    };

    if (delayMillis_ != 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(delayMillis_));
    }

    if (throwException_) {
      fulfillCaptured();
      auto ex = KJ_EXCEPTION(FAILED, "test exception");
      if (throwWithDetail_) {
        ex.setDetail(1, kj::heapArray("cpp-detail-1"_kj.asBytes()));
      }
      throw kj::mv(ex);
    }

    context.getResults().setAs<capnp::AnyPointer>(context.getParams().getAs<capnp::AnyPointer>());
    fulfillCaptured();
    return {kj::READY_NOW, false};
  }

 private:
  uint64_t expectedInterfaceId_;
  uint16_t expectedMethodId_;
  kj::Own<kj::PromiseFulfiller<RawCallResult>> fulfiller_;
  bool fulfilled_ = false;
  uint32_t delayMillis_ = 0;
  bool throwException_ = false;
  bool throwWithDetail_ = false;
};

RawCallResult cppCallOneShot(const std::string& address, uint32_t portHint, uint64_t interfaceId,
                             uint16_t methodId, const std::vector<uint8_t>& requestBytes,
                             const std::vector<uint32_t>& requestCapIds) {
  for (auto capId : requestCapIds) {
    if (capId != 0) {
      throw std::runtime_error(
          "capnp_lean_rpc_cpp_call_one_shot does not support non-zero capability ids");
    }
  }

  auto io = kj::setupAsyncIo();
  auto addr = io.provider->getNetwork().parseAddress(address.c_str(), portHint).wait(io.waitScope);
  auto stream = addr->connect().wait(io.waitScope);
  auto network = kj::heap<capnp::TwoPartyVatNetwork>(*stream, capnp::rpc::twoparty::Side::CLIENT);
  auto rpcSystem = kj::heap<TwoPartyRpcSystem>(capnp::makeRpcClient(*network));

  capnp::word scratch[4];
  memset(&scratch, 0, sizeof(scratch));
  capnp::MallocMessageBuilder message(scratch);
  auto vatId = message.getRoot<capnp::rpc::twoparty::VatId>();
  vatId.setSide(capnp::rpc::twoparty::Side::SERVER);
  auto target = rpcSystem->bootstrap(vatId);

  auto requestBuilder = target.typelessRequest(interfaceId, methodId, kj::none, {});
  if (!requestBytes.empty()) {
    kj::ArrayPtr<const kj::byte> reqBytes(reinterpret_cast<const kj::byte*>(requestBytes.data()),
                                          requestBytes.size());
    kj::ArrayInputStream input(reqBytes);
    capnp::ReaderOptions options;
    options.traversalLimitInWords = 1ull << 30;
    capnp::InputStreamMessageReader reader(input, options);
    requestBuilder.setAs<capnp::AnyPointer>(reader.getRoot<capnp::AnyPointer>());
  }

  auto response = requestBuilder.send().wait(io.waitScope);
  capnp::MallocMessageBuilder responseMessage;
  capnp::BuilderCapabilityTable responseCapTable;
  responseCapTable
      .imbue(responseMessage.getRoot<capnp::AnyPointer>())
      .setAs<capnp::AnyPointer>(response.getAs<capnp::AnyPointer>());

  auto responseWords = capnp::messageToFlatArray(responseMessage);
  auto responseBytes = responseWords.asBytes();
  std::vector<uint8_t> responseCopy(responseBytes.begin(), responseBytes.end());

  std::vector<uint8_t> responseCaps;
  auto responseCapEntries = responseCapTable.getTable();
  responseCaps.reserve(responseCapEntries.size() * 4);
  for (auto& maybeHook : responseCapEntries) {
    KJ_IF_SOME(_hook, maybeHook) {
      throw std::runtime_error(
          "capnp_lean_rpc_cpp_call_one_shot does not support capability results");
    } else {
      appendUint32Le(responseCaps, 0);
    }
  }

  return RawCallResult{std::move(responseCopy), std::move(responseCaps)};
}

void setRequestPayloadNoCaps(capnp::Request<capnp::AnyPointer, capnp::AnyPointer>& requestBuilder,
                             const std::vector<uint8_t>& requestBytes,
                             const std::vector<uint32_t>& requestCapIds,
                             const char* context) {
  for (auto capId : requestCapIds) {
    if (capId != 0) {
      throw std::runtime_error(std::string(context) + " does not support non-zero capability ids");
    }
  }
  if (!requestBytes.empty()) {
    kj::ArrayPtr<const kj::byte> reqBytes(reinterpret_cast<const kj::byte*>(requestBytes.data()),
                                          requestBytes.size());
    kj::ArrayInputStream input(reqBytes);
    capnp::ReaderOptions options;
    options.traversalLimitInWords = 1ull << 30;
    capnp::InputStreamMessageReader reader(input, options);
    requestBuilder.setAs<capnp::AnyPointer>(reader.getRoot<capnp::AnyPointer>());
  }
}

RawCallResult serializeResponseNoCaps(capnp::Response<capnp::AnyPointer>& response,
                                      const char* context) {
  capnp::MallocMessageBuilder responseMessage;
  capnp::BuilderCapabilityTable responseCapTable;
  responseCapTable
      .imbue(responseMessage.getRoot<capnp::AnyPointer>())
      .setAs<capnp::AnyPointer>(response.getAs<capnp::AnyPointer>());

  auto responseWords = capnp::messageToFlatArray(responseMessage);
  auto responseBytes = responseWords.asBytes();
  std::vector<uint8_t> responseCopy(responseBytes.begin(), responseBytes.end());

  std::vector<uint8_t> responseCaps;
  auto responseCapEntries = responseCapTable.getTable();
  responseCaps.reserve(responseCapEntries.size() * 4);
  for (auto& maybeHook : responseCapEntries) {
    KJ_IF_SOME(_hook, maybeHook) {
      throw std::runtime_error(std::string(context) + " does not support capability results");
    } else {
      appendUint32Le(responseCaps, 0);
    }
  }

  return RawCallResult{std::move(responseCopy), std::move(responseCaps)};
}

RawCallResult cppCallPipelinedCapOneShot(const std::string& address, uint32_t portHint,
                                         uint64_t interfaceId, uint16_t methodId,
                                         const std::vector<uint8_t>& requestBytes,
                                         const std::vector<uint32_t>& requestCapIds,
                                         const std::vector<uint8_t>& pipelinedRequestBytes,
                                         const std::vector<uint32_t>& pipelinedRequestCapIds) {
  auto io = kj::setupAsyncIo();
  auto addr = io.provider->getNetwork().parseAddress(address.c_str(), portHint).wait(io.waitScope);
  auto stream = addr->connect().wait(io.waitScope).downcast<kj::AsyncCapabilityStream>();
  auto network = kj::heap<capnp::TwoPartyVatNetwork>(
      *stream, 16, capnp::rpc::twoparty::Side::CLIENT);
  auto rpcSystem = kj::heap<TwoPartyRpcSystem>(capnp::makeRpcClient(*network));

  capnp::word scratch[4];
  memset(&scratch, 0, sizeof(scratch));
  capnp::MallocMessageBuilder message(scratch);
  auto vatId = message.getRoot<capnp::rpc::twoparty::VatId>();
  vatId.setSide(capnp::rpc::twoparty::Side::SERVER);
  auto target = rpcSystem->bootstrap(vatId);

  auto firstRequest = target.typelessRequest(interfaceId, methodId, kj::none, {});
  setRequestPayloadNoCaps(firstRequest, requestBytes, requestCapIds,
                          "capnp_lean_rpc_cpp_call_pipelined_cap_one_shot(first call)");
  auto firstPromise = firstRequest.send();

  auto pipelinedTarget = capnp::Capability::Client(firstPromise.noop().asCap());
  auto pipelinedRequest = pipelinedTarget.typelessRequest(interfaceId, methodId, kj::none, {});
  setRequestPayloadNoCaps(pipelinedRequest, pipelinedRequestBytes, pipelinedRequestCapIds,
                          "capnp_lean_rpc_cpp_call_pipelined_cap_one_shot(pipelined call)");
  auto pipelinedResponse = pipelinedRequest.send().wait(io.waitScope);

  // Ensure the original call eventually settles as well.
  (void)kj::mv(firstPromise).wait(io.waitScope);
  return serializeResponseNoCaps(
      pipelinedResponse, "capnp_lean_rpc_cpp_call_pipelined_cap_one_shot");
}

RawCallResult cppServeOneShotEx(const std::string& address, uint32_t portHint, uint64_t interfaceId,
                                uint16_t methodId, uint32_t delayMillis, bool throwException,
                                bool throwWithDetail, bool waitForDisconnect = true) {
  auto io = kj::setupAsyncIo();
  auto addr = io.provider->getNetwork().parseAddress(address.c_str(), portHint).wait(io.waitScope);
  auto listener = addr->listen();

  auto paf = kj::newPromiseAndFulfiller<RawCallResult>();
  auto server = kj::heap<capnp::TwoPartyServer>(capnp::Capability::Client(
      kj::heap<OneShotCaptureServer>(interfaceId, methodId, kj::mv(paf.fulfiller), delayMillis,
                                     throwException, throwWithDetail)));

  auto connection = listener->accept().wait(io.waitScope);
  server->accept(kj::mv(connection));
  auto result = paf.promise.wait(io.waitScope);
  if (waitForDisconnect) {
    server->drain().wait(io.waitScope);
  } else {
    // Keep the server alive briefly so the first response can flush before closing.
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return result;
}

RawCallResult cppServeOneShot(const std::string& address, uint32_t portHint, uint64_t interfaceId,
                              uint16_t methodId) {
  return cppServeOneShotEx(address, portHint, interfaceId, methodId, 0, false, false, true);
}

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
    completion->exceptionTypeTag = static_cast<uint8_t>(kj::Exception::Type::FAILED);
    completion->exceptionDescription = completion->error;
    completion->remoteTrace.clear();
    completion->detailBytes.clear();
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeFailureKj(const std::shared_ptr<RawCallCompletion>& completion,
                       const kj::Exception& e) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = false;
    completion->error = describeKjException(e);
    completion->exceptionTypeTag = static_cast<uint8_t>(e.getType());
    completion->exceptionDescription = std::string(e.getDescription().cStr());
    auto remoteTrace = e.getRemoteTrace();
    if (remoteTrace != nullptr) {
      completion->remoteTrace = std::string(remoteTrace.cStr());
    } else {
      completion->remoteTrace.clear();
    }
    completion->detailBytes.clear();
    KJ_IF_SOME(detail, e.getDetail(1)) {
      completion->detailBytes.assign(detail.begin(), detail.end());
    }
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

void completeInt64Success(const std::shared_ptr<Int64Completion>& completion, int64_t value) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = true;
    completion->value = value;
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeInt64Failure(const std::shared_ptr<Int64Completion>& completion,
                          std::string message) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = false;
    completion->error = std::move(message);
    completion->done = true;
  }
  completion->cv.notify_one();
}

void completeRegisterPairSuccess(const std::shared_ptr<RegisterPairCompletion>& completion,
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

void completeRegisterPairFailure(const std::shared_ptr<RegisterPairCompletion>& completion,
                                 std::string message) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    completion->ok = false;
    completion->error = std::move(message);
    completion->done = true;
  }
  completion->cv.notify_one();
}

class LoopbackCapabilityServer final : public capnp::Capability::Server {
 public:
  DispatchCallResult dispatchCall(uint64_t interfaceId, uint16_t methodId,
                                  capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer>
                                      context) override {
    (void)interfaceId;
    (void)methodId;

    debugLog(
        "loopback.dispatch",
        "interfaceId=" + std::to_string(interfaceId) + " methodId=" + std::to_string(methodId));

    context.getResults().setAs<capnp::AnyPointer>(context.getParams().getAs<capnp::AnyPointer>());
    return {kj::READY_NOW, false};
  }
};

class TailCallForwardingServer final : public capnp::Capability::Server {
 public:
  explicit TailCallForwardingServer(capnp::Capability::Client target)
      : target_(kj::mv(target)) {}

  DispatchCallResult dispatchCall(
      uint64_t interfaceId, uint16_t methodId,
      capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer> context) override {
    debugLog(
        "tailcall.dispatch",
        "interfaceId=" + std::to_string(interfaceId) + " methodId=" + std::to_string(methodId));
    auto requestBuilder = target_.typelessRequest(interfaceId, methodId, kj::none, {});
    requestBuilder.setAs<capnp::AnyPointer>(context.getParams().getAs<capnp::AnyPointer>());
    return {context.tailCall(kj::mv(requestBuilder)), false};
  }

 private:
  capnp::Capability::Client target_;
};

class FdCapabilityServer final : public capnp::Capability::Server {
 public:
  explicit FdCapabilityServer(int fd) : fd_(fd) {}
  ~FdCapabilityServer() {
#if !defined(_WIN32)
    if (fd_ >= 0) {
      close(fd_);
    }
#endif
  }

  DispatchCallResult dispatchCall(
      uint64_t interfaceId, uint16_t methodId,
      capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer> context) override {
    (void)interfaceId;
    (void)methodId;
    context.getResults().setAs<capnp::AnyPointer>(context.getParams().getAs<capnp::AnyPointer>());
    return {kj::READY_NOW, false};
  }

  kj::Maybe<int> getFd() override {
#if defined(_WIN32)
    return kj::none;
#else
    if (fd_ < 0) {
      return kj::none;
    }
    return fd_;
#endif
  }

 private:
  int fd_;
};

using GenericVatId = capnp_test::TestSturdyRefHostId;
using GenericThirdPartyCompletion = capnp_test::TestThirdPartyCompletion;
using GenericThirdPartyToAwait = capnp_test::TestThirdPartyToAwait;
using GenericThirdPartyToContact = capnp_test::TestThirdPartyToContact;
using GenericJoinResult = capnp_test::TestJoinResult;
using GenericVatNetworkBase = capnp::VatNetwork<GenericVatId, GenericThirdPartyCompletion,
                                                GenericThirdPartyToAwait,
                                                GenericThirdPartyToContact, GenericJoinResult>;
using GenericRpcSystem = capnp::RpcSystem<GenericVatId>;

class GenericVat;

class GenericVatNetwork {
 public:
  GenericVatNetwork() = default;
  ~GenericVatNetwork() = default;

  GenericVat& add(const std::string& name);
  GenericVat* find(const std::string& name);

  uint64_t newToken() { return ++tokenCounter_; }
  uint64_t tokenCount() const { return tokenCounter_; }

  bool forwardingEnabled() const { return forwardingEnabled_; }
  void setForwardingEnabled(bool enabled) { forwardingEnabled_ = enabled; }
  void resetForwardingStats() {
    forwardCount_ = 0;
    deniedForwardCount_ = 0;
  }

  uint64_t forwardCount() const { return forwardCount_; }
  uint64_t deniedForwardCount() const { return deniedForwardCount_; }
  void incrementForwardCount() { ++forwardCount_; }
  void incrementDeniedForwardCount() { ++deniedForwardCount_; }

 private:
  std::unordered_map<std::string, std::unique_ptr<GenericVat>> vats_;
  uint64_t tokenCounter_ = 0;
  bool forwardingEnabled_ = false;
  uint64_t forwardCount_ = 0;
  uint64_t deniedForwardCount_ = 0;
};

class GenericVat final : public GenericVatNetworkBase {
 public:
  using Connection = GenericVatNetworkBase::Connection;

  GenericVat(GenericVatNetwork& network, std::string name)
      : network_(network), name_(std::move(name)) {}

  ~GenericVat() {
    kj::Exception exception(kj::Exception::Type::FAILED, __FILE__, __LINE__,
                            kj::str("GenericVat network was destroyed."));
    for (auto& entry : connections_) {
      entry.second->disconnect(kj::cp(exception));
    }
  }

  class ConnectionImpl final
      : public Connection, public kj::Refcounted, public kj::TaskSet::ErrorHandler {
   public:
    ConnectionImpl(GenericVat& vat, GenericVat& peerVat, bool unique)
        : vat_(vat),
          peerVat_(peerVat),
          unique_(unique),
          peerVatIdMessage_(8) {
      auto peerVatId = peerVatIdMessage_.initRoot<GenericVatId>();
      peerVatId.setHost(peerVat.name_.c_str());
      peerVatId.setUnique(unique);
      if (!unique_) {
        vat_.connections_[&peerVat_] = this;
      }
      tasks_ = kj::heap<kj::TaskSet>(static_cast<kj::TaskSet::ErrorHandler&>(*this));
    }

    ~ConnectionImpl() noexcept(false) {
      if (!unique_) {
        vat_.connections_.erase(&peerVat_);
      }
      KJ_IF_SOME(partner, partner_) {
        partner.partner_ = kj::none;
      }
    }

    void attach(ConnectionImpl& other) {
      KJ_REQUIRE(partner_ == kj::none);
      KJ_REQUIRE(other.partner_ == kj::none);
      partner_ = other;
      partnerName_ = other.vat_.name_;
      other.partner_ = *this;
      other.partnerName_ = vat_.name_;
    }

    bool isIdle() const { return idle_; }

    void initiateIdleShutdown() {
      initiatedIdleShutdown_ = true;
      messageQueue_.push(kj::none);
      KJ_IF_SOME(f, fulfillOnEnd_) {
        f->fulfill();
      }
    }

    void disconnect(kj::Exception&& exception) {
      messageQueue_.rejectAll(kj::cp(exception));
      networkException_ = kj::mv(exception);
      tasks_ = nullptr;
    }

    class IncomingRpcMessageImpl final : public capnp::IncomingRpcMessage, public kj::Refcounted {
     public:
      explicit IncomingRpcMessageImpl(kj::Array<capnp::word> data)
          : data_(kj::mv(data)), message_(data_) {}

      capnp::AnyPointer::Reader getBody() override {
        return message_.getRoot<capnp::AnyPointer>();
      }

      size_t sizeInWords() override { return data_.size(); }

     private:
      kj::Array<capnp::word> data_;
      capnp::FlatArrayMessageReader message_;
    };

    class OutgoingRpcMessageImpl final : public capnp::OutgoingRpcMessage {
     public:
      OutgoingRpcMessageImpl(ConnectionImpl& connection, uint firstSegmentWordSize)
          : connection_(connection),
            message_(firstSegmentWordSize == 0 ? capnp::SUGGESTED_FIRST_SEGMENT_WORDS
                                               : firstSegmentWordSize) {}

      capnp::AnyPointer::Builder getBody() override {
        return message_.getRoot<capnp::AnyPointer>();
      }

      void send() override {
        if (connection_.networkException_ != kj::none) {
          return;
        }
        ++connection_.vat_.sent_;
        auto incoming = kj::heap<IncomingRpcMessageImpl>(capnp::messageToFlatArray(message_));
        auto* connectionPtr = &connection_;
        connection_.tasks_->add(
            kj::yield().then([connectionPtr, message = kj::mv(incoming)]() mutable {
              KJ_IF_SOME(partner, connectionPtr->partner_) {
                partner.messageQueue_.push(kj::Own<capnp::IncomingRpcMessage>(kj::mv(message)));
              }
            }));
      }

      size_t sizeInWords() override { return message_.sizeInWords(); }

     private:
      ConnectionImpl& connection_;
      capnp::MallocMessageBuilder message_;
    };

    GenericVatId::Reader getPeerVatId() override {
      return peerVatIdMessage_.getRoot<GenericVatId>().asReader();
    }

    kj::Own<capnp::OutgoingRpcMessage> newOutgoingMessage(uint firstSegmentWordSize) override {
      KJ_REQUIRE(!idle_);
      return kj::heap<OutgoingRpcMessageImpl>(*this, firstSegmentWordSize);
    }

    kj::Promise<kj::Maybe<kj::Own<capnp::IncomingRpcMessage>>> receiveIncomingMessage() override {
      KJ_IF_SOME(e, networkException_) {
        kj::throwFatalException(kj::cp(e));
      }
      if (initiatedIdleShutdown_) {
        co_return kj::none;
      }
      auto result = co_await messageQueue_.pop();
      if (result == kj::none) {
        KJ_IF_SOME(f, fulfillOnEnd_) {
          f->fulfill();
        }
      } else {
        ++vat_.received_;
      }
      co_return result;
    }

    kj::Promise<void> shutdown() override {
      KJ_IF_SOME(partner, partner_) {
        if (partner.initiatedIdleShutdown_) {
          return kj::READY_NOW;
        }
        return kj::evalLater([this]() -> kj::Promise<void> {
          KJ_IF_SOME(activePartner, partner_) {
            activePartner.messageQueue_.push(kj::none);
            auto paf = kj::newPromiseAndFulfiller<void>();
            activePartner.fulfillOnEnd_ = kj::mv(paf.fulfiller);
            return kj::mv(paf.promise);
          }
          return kj::READY_NOW;
        });
      }
      return kj::READY_NOW;
    }

    void setIdle(bool idle) override {
      KJ_REQUIRE(idle != idle_);
      idle_ = idle;
    }

    bool canIntroduceTo(Connection& other) override {
      (void)other;
      return true;
    }

    void introduceTo(Connection& other, GenericThirdPartyToContact::Builder otherContactInfo,
                     GenericThirdPartyToAwait::Builder thisAwaitInfo) override {
      auto token = vat_.network_.newToken();
      LeanThirdPartyContact contact;
      contact.path = LeanVatId{kj::downcast<ConnectionImpl>(other).partnerName_, false};
      contact.token = token;
      contact.sentBy = vat_.name_;
      setThirdPartyContact(otherContactInfo, contact);
      setThirdPartyToken(thisAwaitInfo, token);
    }

    kj::Maybe<kj::Own<Connection>> connectToIntroduced(
        GenericThirdPartyToContact::Reader contact,
        GenericThirdPartyCompletion::Builder completion) override {
      auto decoded = decodeThirdPartyContact(contact);
      KJ_REQUIRE(decoded.sentBy == partnerName_);
      setThirdPartyToken(completion, decoded.token);
      return vat_.connectByVatId(decoded.path);
    }

    bool canForwardThirdPartyToContact(GenericThirdPartyToContact::Reader contact,
                                       Connection& destination) override {
      (void)contact;
      (void)destination;
      if (!vat_.network_.forwardingEnabled()) {
        vat_.network_.incrementDeniedForwardCount();
      }
      return vat_.network_.forwardingEnabled();
    }

    void forwardThirdPartyToContact(GenericThirdPartyToContact::Reader contact,
                                    Connection& destination,
                                    GenericThirdPartyToContact::Builder result) override {
      (void)destination;
      KJ_REQUIRE(vat_.network_.forwardingEnabled());
      auto decoded = decodeThirdPartyContact(contact);
      KJ_REQUIRE(decoded.sentBy == partnerName_);
      vat_.network_.incrementForwardCount();
      decoded.sentBy = vat_.name_;
      setThirdPartyContact(result, decoded);
    }

    kj::Own<void> awaitThirdParty(GenericThirdPartyToAwait::Reader party,
                                  kj::Rc<kj::Refcounted> value) override {
      auto token = decodeThirdPartyToken(party);
      auto& exchange = vat_.getThirdPartyExchange(token);
      exchange.fulfiller->fulfill(kj::mv(value));
      class TokenRelease final {
       public:
        TokenRelease(GenericVat& vat, uint64_t token) : vat_(vat), token_(token) {}
        ~TokenRelease() { vat_.eraseThirdPartyExchange(token_); }

       private:
        GenericVat& vat_;
        uint64_t token_;
      };
      return kj::heap<TokenRelease>(vat_, token);
    }

    kj::Promise<kj::Rc<kj::Refcounted>> completeThirdParty(
        GenericThirdPartyCompletion::Reader completion) override {
      auto token = decodeThirdPartyToken(completion);
      auto& exchange = vat_.getThirdPartyExchange(token);
      return exchange.promise.addBranch();
    }

    kj::Array<capnp::byte> generateEmbargoId() override {
      static uint32_t counter = 0;
      auto out = kj::heapArray<capnp::byte>(sizeof(counter));
      out.asPtr().copyFrom(kj::asBytes(counter));
      ++counter;
      return out;
    }

    void taskFailed(kj::Exception&& exception) override { (void)exception; }

   private:
    GenericVat& vat_;
    GenericVat& peerVat_;
    bool unique_;
    capnp::MallocMessageBuilder peerVatIdMessage_;
    kj::Maybe<ConnectionImpl&> partner_;
    std::string partnerName_;
    kj::Maybe<kj::Exception> networkException_;
    kj::ProducerConsumerQueue<kj::Maybe<kj::Own<capnp::IncomingRpcMessage>>> messageQueue_;
    kj::Maybe<kj::Own<kj::PromiseFulfiller<void>>> fulfillOnEnd_;
    bool idle_ = true;
    bool initiatedIdleShutdown_ = false;
    kj::Own<kj::TaskSet> tasks_;
  };

  kj::Maybe<kj::Own<Connection>> connect(GenericVatId::Reader hostId) override {
    return connectByVatId(decodeLeanVatId(hostId));
  }

  kj::Promise<kj::Own<Connection>> accept() override {
    return acceptQueue_.pop();
  }

  kj::Maybe<ConnectionImpl&> getConnectionTo(GenericVat& other) {
    auto it = connections_.find(&other);
    if (it == connections_.end()) {
      return kj::none;
    }
    return *it->second;
  }

  const std::string& name() const { return name_; }

 private:
  friend class GenericVatNetwork;
  friend class ConnectionImpl;

  struct ThirdPartyExchange {
    kj::ForkedPromise<kj::Rc<kj::Refcounted>> promise;
    kj::Own<kj::PromiseFulfiller<kj::Rc<kj::Refcounted>>> fulfiller;

    ThirdPartyExchange(
        kj::PromiseFulfillerPair<kj::Rc<kj::Refcounted>> paf =
            kj::newPromiseAndFulfiller<kj::Rc<kj::Refcounted>>())
        : promise(paf.promise.fork()), fulfiller(kj::mv(paf.fulfiller)) {}
  };

  ThirdPartyExchange& getThirdPartyExchange(uint64_t token) {
    auto it = tphExchanges_.find(token);
    if (it != tphExchanges_.end()) {
      return *it->second;
    }
    auto exchange = std::make_unique<ThirdPartyExchange>();
    auto* ptr = exchange.get();
    tphExchanges_.emplace(token, std::move(exchange));
    return *ptr;
  }

  void eraseThirdPartyExchange(uint64_t token) {
    tphExchanges_.erase(token);
  }

  kj::Maybe<kj::Own<Connection>> connectByVatId(const LeanVatId& hostId) {
    if (hostId.host == name_) {
      return kj::none;
    }
    auto* destination = network_.find(hostId.host);
    if (destination == nullptr) {
      throw std::runtime_error("unknown vat host: " + hostId.host);
    }
    if (!hostId.unique) {
      auto existingIt = connections_.find(destination);
      if (existingIt != connections_.end()) {
        return kj::Own<Connection>(kj::addRef(*existingIt->second));
      }
    }
    auto local = kj::refcounted<ConnectionImpl>(*this, *destination, hostId.unique);
    auto remote = kj::refcounted<ConnectionImpl>(*destination, *this, hostId.unique);
    local->attach(*remote);
    destination->acceptQueue_.push(kj::mv(remote));
    return kj::Own<Connection>(kj::mv(local));
  }

  GenericVatNetwork& network_;
  std::string name_;
  uint64_t sent_ = 0;
  uint64_t received_ = 0;
  std::unordered_map<const GenericVat*, ConnectionImpl*> connections_;
  kj::ProducerConsumerQueue<kj::Own<Connection>> acceptQueue_;
  std::unordered_map<uint64_t, std::unique_ptr<ThirdPartyExchange>> tphExchanges_;
};

GenericVat& GenericVatNetwork::add(const std::string& name) {
  auto it = vats_.find(name);
  if (it != vats_.end()) {
    throw std::runtime_error("vat already exists: " + name);
  }
  auto vat = std::make_unique<GenericVat>(*this, name);
  auto* ptr = vat.get();
  vats_.emplace(name, std::move(vat));
  return *ptr;
}

GenericVat* GenericVatNetwork::find(const std::string& name) {
  auto it = vats_.find(name);
  if (it == vats_.end()) {
    return nullptr;
  }
  return it->second.get();
}

class RuntimeLoop {
 public:
  explicit RuntimeLoop(uint32_t maxFdsPerMessage = kRuntimeDefaultMaxFdsPerMessage)
      : maxFdsPerMessage_(sanitizeMaxFdsPerMessage(maxFdsPerMessage)),
        worker_(&RuntimeLoop::run, this) {
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
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueStartPendingCall(
      uint32_t target, uint64_t interfaceId, uint16_t methodId, std::vector<uint8_t> request,
      std::vector<uint32_t> requestCaps) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedStartPendingCall{
          target, interfaceId, methodId, std::move(request), std::move(requestCaps), completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RawCallCompletion> enqueueAwaitPendingCall(uint32_t pendingCallId) {
    auto completion = std::make_shared<RawCallCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedAwaitPendingCall{pendingCallId, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueReleasePendingCall(uint32_t pendingCallId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedReleasePendingCall{pendingCallId, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueGetPipelinedCap(
      uint32_t pendingCallId, std::vector<uint16_t> pointerPath) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(
          QueuedGetPipelinedCap{pendingCallId, std::move(pointerPath), completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueStreamingCall(
      uint32_t target, uint64_t interfaceId, uint16_t methodId, std::vector<uint8_t> request,
      std::vector<uint32_t> requestCaps) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedStreamingCall{
          target, interfaceId, methodId, std::move(request), std::move(requestCaps), completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<Int64Completion> enqueueTargetGetFd(uint32_t target) {
    auto completion = std::make_shared<Int64Completion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeInt64Failure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedTargetGetFd{target, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueTargetWhenResolved(uint32_t target) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedTargetWhenResolved{target, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueTargetWhenResolvedStart(uint32_t target) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedTargetWhenResolvedStart{target, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueEnableTraceEncoder() {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedEnableTraceEncoder{completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueDisableTraceEncoder() {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedDisableTraceEncoder{completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueSetTraceEncoder(b_lean_obj_arg encoder) {
    auto completion = std::make_shared<UnitCompletion>();
    auto* encoderObj = const_cast<lean_object*>(encoder);
    lean_mark_mt(encoderObj);
    lean_inc(encoderObj);
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        lean_dec(encoderObj);
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedSetTraceEncoder{encoderObj, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RawCallCompletion> enqueueCppCallWithAccept(
      uint32_t serverId, uint32_t listenerId, std::string address, uint32_t portHint,
      uint64_t interfaceId, uint16_t methodId, std::vector<uint8_t> request,
      std::vector<uint32_t> requestCaps) {
    auto completion = std::make_shared<RawCallCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedCppCallWithAccept{
          serverId, listenerId, std::move(address), portHint, interfaceId, methodId,
          std::move(request), std::move(requestCaps), completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RawCallCompletion> enqueueCppCallPipelinedWithAccept(
      uint32_t serverId, uint32_t listenerId, std::string address, uint32_t portHint,
      uint64_t interfaceId, uint16_t methodId, std::vector<uint8_t> request,
      std::vector<uint32_t> requestCaps, std::vector<uint8_t> pipelinedRequest,
      std::vector<uint32_t> pipelinedRequestCaps) {
    auto completion = std::make_shared<RawCallCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedCppCallPipelinedWithAccept{
          serverId, listenerId, std::move(address), portHint, interfaceId, methodId,
          std::move(request), std::move(requestCaps), std::move(pipelinedRequest),
          std::move(pipelinedRequestCaps), completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueRegisterLoopbackTarget() {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedRegisterLoopbackTarget{completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueRegisterLoopbackTarget(
      uint32_t bootstrapTarget) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedRegisterLoopbackBootstrapTarget{bootstrapTarget, completion});
    }
    notifyWorker();
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
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueRegisterAdvancedHandlerTarget(
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
      queue_.emplace_back(QueuedRegisterAdvancedHandlerTarget{handlerObj, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueRegisterTailCallHandlerTarget(
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
      queue_.emplace_back(QueuedRegisterTailCallHandlerTarget{handlerObj, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueRegisterTailCallTarget(uint32_t target) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedRegisterTailCallTarget{target, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueRegisterFdTarget(uint32_t fd) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedRegisterFdTarget{fd, completion});
    }
    notifyWorker();
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
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueReleaseTargets(std::vector<uint32_t> targets) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedReleaseTargets{std::move(targets), completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueRetainTarget(uint32_t target) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedRetainTarget{target, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterPairCompletion> enqueueNewPromiseCapability() {
    auto completion = std::make_shared<RegisterPairCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterPairFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedNewPromiseCapability{completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueuePromiseCapabilityFulfill(uint32_t fulfillerId,
                                                                  uint32_t target) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedPromiseCapabilityFulfill{fulfillerId, target, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueuePromiseCapabilityReject(uint32_t fulfillerId,
                                                                 uint8_t exceptionTypeTag,
                                                                 std::string message,
                                                                 std::vector<uint8_t> detailBytes) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedPromiseCapabilityReject{
          fulfillerId, exceptionTypeTag, std::move(message), std::move(detailBytes), completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueuePromiseCapabilityRelease(uint32_t fulfillerId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedPromiseCapabilityRelease{fulfillerId, completion});
    }
    notifyWorker();
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
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueConnectTargetStart(std::string address,
                                                                      uint32_t portHint) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedConnectTargetStart{std::move(address), portHint, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueConnectTargetFd(uint32_t fd) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedConnectTargetFd{fd, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterPairCompletion> enqueueNewTransportPipe() {
    auto completion = std::make_shared<RegisterPairCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterPairFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedNewTransportPipe{completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueNewTransportFromFd(uint32_t fd) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedNewTransportFromFd{fd, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueNewTransportFromFdTake(uint32_t fd) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedNewTransportFromFdTake{fd, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueReleaseTransport(uint32_t transportId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedReleaseTransport{transportId, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<Int64Completion> enqueueTransportGetFd(uint32_t transportId) {
    auto completion = std::make_shared<Int64Completion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeInt64Failure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedTransportGetFd{transportId, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueConnectTargetTransport(uint32_t transportId) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedConnectTargetTransport{transportId, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueListenLoopback(std::string address,
                                                              uint32_t portHint) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedListenLoopback{std::move(address), portHint, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueAcceptLoopback(uint32_t listenerId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedAcceptLoopback{listenerId, completion});
    }
    notifyWorker();
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
    notifyWorker();
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
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueNewClientStart(std::string address,
                                                                  uint32_t portHint) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedNewClientStart{std::move(address), portHint, completion});
    }
    notifyWorker();
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
    notifyWorker();
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
    notifyWorker();
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
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueClientOnDisconnectStart(uint32_t clientId) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedClientOnDisconnectStart{clientId, completion});
    }
    notifyWorker();
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
    notifyWorker();
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
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueNewServerWithBootstrapFactory(
      b_lean_obj_arg bootstrapFactory) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    auto* bootstrapFactoryObj = const_cast<lean_object*>(bootstrapFactory);
    lean_mark_mt(bootstrapFactoryObj);
    lean_inc(bootstrapFactoryObj);
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        lean_dec(bootstrapFactoryObj);
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedNewServerWithBootstrapFactory{bootstrapFactoryObj, completion});
    }
    notifyWorker();
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
    notifyWorker();
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
    notifyWorker();
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
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueServerAcceptStart(
      uint32_t serverId, uint32_t listenerId) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedServerAcceptStart{serverId, listenerId, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueServerAcceptFd(uint32_t serverId, uint32_t fd) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedServerAcceptFd{serverId, fd, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueServerAcceptTransport(uint32_t serverId,
                                                               uint32_t transportId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedServerAcceptTransport{serverId, transportId, completion});
    }
    notifyWorker();
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
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueServerDrainStart(uint32_t serverId) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedServerDrainStart{serverId, completion});
    }
    notifyWorker();
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
    notifyWorker();
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
    notifyWorker();
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
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UInt64Completion> enqueueTargetCount() {
    auto completion = std::make_shared<UInt64Completion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUInt64Failure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedTargetCount{completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UInt64Completion> enqueueListenerCount() {
    auto completion = std::make_shared<UInt64Completion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUInt64Failure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedListenerCount{completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UInt64Completion> enqueueClientCount() {
    auto completion = std::make_shared<UInt64Completion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUInt64Failure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedClientCount{completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UInt64Completion> enqueueServerCount() {
    auto completion = std::make_shared<UInt64Completion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUInt64Failure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedServerCount{completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UInt64Completion> enqueuePendingCallCount() {
    auto completion = std::make_shared<UInt64Completion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUInt64Failure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedPendingCallCount{completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueAwaitRegisterPromise(uint32_t promiseId) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedAwaitRegisterPromise{promiseId, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueCancelRegisterPromise(uint32_t promiseId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedCancelRegisterPromise{promiseId, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueReleaseRegisterPromise(uint32_t promiseId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedReleaseRegisterPromise{promiseId, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueAwaitUnitPromise(uint32_t promiseId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedAwaitUnitPromise{promiseId, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueCancelUnitPromise(uint32_t promiseId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedCancelUnitPromise{promiseId, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueReleaseUnitPromise(uint32_t promiseId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedReleaseUnitPromise{promiseId, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueuePump() {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedPump{completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueMultiVatNewClient(std::string name) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedMultiVatNewClient{std::move(name), completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueMultiVatNewServer(std::string name,
                                                                     uint32_t bootstrapTarget) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedMultiVatNewServer{std::move(name), bootstrapTarget, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueMultiVatNewServerWithBootstrapFactory(
      std::string name, b_lean_obj_arg bootstrapFactory) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    auto* factoryObj = const_cast<lean_object*>(bootstrapFactory);
    lean_mark_mt(factoryObj);
    lean_inc(factoryObj);
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        lean_dec(factoryObj);
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(
          QueuedMultiVatNewServerWithBootstrapFactory{std::move(name), factoryObj, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueMultiVatReleasePeer(uint32_t peerId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedMultiVatReleasePeer{peerId, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueMultiVatBootstrap(
      uint32_t sourcePeerId, std::string host, bool unique) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(
          QueuedMultiVatBootstrap{sourcePeerId, std::move(host), unique, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueMultiVatBootstrapPeer(
      uint32_t sourcePeerId, uint32_t targetPeerId, bool unique) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(
          QueuedMultiVatBootstrapPeer{sourcePeerId, targetPeerId, unique, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueMultiVatSetForwardingEnabled(bool enabled) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedMultiVatSetForwardingEnabled{enabled, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueMultiVatResetForwardingStats() {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedMultiVatResetForwardingStats{completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UInt64Completion> enqueueMultiVatForwardCount() {
    auto completion = std::make_shared<UInt64Completion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUInt64Failure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedMultiVatForwardCount{completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UInt64Completion> enqueueMultiVatThirdPartyTokenCount() {
    auto completion = std::make_shared<UInt64Completion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUInt64Failure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedMultiVatThirdPartyTokenCount{completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UInt64Completion> enqueueMultiVatDeniedForwardCount() {
    auto completion = std::make_shared<UInt64Completion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUInt64Failure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedMultiVatDeniedForwardCount{completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UInt64Completion> enqueueMultiVatHasConnection(uint32_t fromPeerId,
                                                                 uint32_t toPeerId) {
    auto completion = std::make_shared<UInt64Completion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUInt64Failure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedMultiVatHasConnection{fromPeerId, toPeerId, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueMultiVatSetRestorer(uint32_t peerId,
                                                             b_lean_obj_arg restorer) {
    auto completion = std::make_shared<UnitCompletion>();
    auto* restorerObj = const_cast<lean_object*>(restorer);
    lean_mark_mt(restorerObj);
    lean_inc(restorerObj);
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        lean_dec(restorerObj);
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedMultiVatSetRestorer{peerId, restorerObj, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueMultiVatClearRestorer(uint32_t peerId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedMultiVatClearRestorer{peerId, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<UnitCompletion> enqueueMultiVatPublishSturdyRef(
      uint32_t hostPeerId, std::vector<uint8_t> objectId, uint32_t targetId) {
    auto completion = std::make_shared<UnitCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeUnitFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedMultiVatPublishSturdyRef{
          hostPeerId, std::move(objectId), targetId, completion});
    }
    notifyWorker();
    return completion;
  }

  std::shared_ptr<RegisterTargetCompletion> enqueueMultiVatRestoreSturdyRef(
      uint32_t sourcePeerId, std::string host, bool unique, std::vector<uint8_t> objectId) {
    auto completion = std::make_shared<RegisterTargetCompletion>();
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        completeRegisterFailure(completion, "Capnp.Rpc runtime is shutting down");
        return completion;
      }
      queue_.emplace_back(QueuedMultiVatRestoreSturdyRef{
          sourcePeerId, std::move(host), unique, std::move(objectId), completion});
    }
    notifyWorker();
    return completion;
  }

  bool isWorkerThread() const {
    return std::this_thread::get_id() == worker_.get_id();
  }

  uint32_t retainTargetInline(uint32_t target) {
    return retainTarget(target);
  }

  void releaseTargetInline(uint32_t target) {
    releaseTarget(target);
  }

  void releaseTargetsInline(const std::vector<uint32_t>& targets) {
    releaseTargets(targets);
  }

  std::pair<uint32_t, uint32_t> newPromiseCapabilityInline() {
    return newPromiseCapability();
  }

  void promiseCapabilityFulfillInline(uint32_t fulfillerId, uint32_t target) {
    promiseCapabilityFulfill(fulfillerId, target);
  }

  void promiseCapabilityRejectInline(uint32_t fulfillerId, uint8_t exceptionTypeTag,
                                     std::string message,
                                     std::vector<uint8_t> detailBytes) {
    promiseCapabilityReject(fulfillerId, exceptionTypeTag, message, detailBytes);
  }

  void promiseCapabilityReleaseInline(uint32_t fulfillerId) {
    promiseCapabilityRelease(fulfillerId);
  }

  void notifyWorker() {
    // Wake the runtime thread whether it's blocked on the legacy condition variable or on the KJ
    // event port. This keeps KJ async I/O progressing without requiring explicit Runtime.pump calls.
    queueCv_.notify_one();
    auto* port = eventPort_.load(std::memory_order_acquire);
    if (port != nullptr) {
      port->wake();
    }
  }

  void shutdown() {
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      if (stopping_) {
        return;
      }
      stopping_ = true;
    }
    notifyWorker();
    if (worker_.joinable()) {
      worker_.join();
    }
  }

 private:
  static uint sanitizeMaxFdsPerMessage(uint32_t maxFdsPerMessage) {
    if (maxFdsPerMessage == 0) {
      return static_cast<uint>(kRuntimeDefaultMaxFdsPerMessage);
    }
    return static_cast<uint>(maxFdsPerMessage);
  }

  struct QueuedRawCall {
    uint32_t target;
    uint64_t interfaceId;
    uint16_t methodId;
    std::vector<uint8_t> request;
    std::vector<uint32_t> requestCaps;
    std::shared_ptr<RawCallCompletion> completion;
  };

  struct QueuedStartPendingCall {
    uint32_t target;
    uint64_t interfaceId;
    uint16_t methodId;
    std::vector<uint8_t> request;
    std::vector<uint32_t> requestCaps;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedAwaitPendingCall {
    uint32_t pendingCallId;
    std::shared_ptr<RawCallCompletion> completion;
  };

  struct QueuedReleasePendingCall {
    uint32_t pendingCallId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedGetPipelinedCap {
    uint32_t pendingCallId;
    std::vector<uint16_t> pointerPath;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedStreamingCall {
    uint32_t target;
    uint64_t interfaceId;
    uint16_t methodId;
    std::vector<uint8_t> request;
    std::vector<uint32_t> requestCaps;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedTargetGetFd {
    uint32_t target;
    std::shared_ptr<Int64Completion> completion;
  };

  struct QueuedTargetWhenResolved {
    uint32_t target;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedTargetWhenResolvedStart {
    uint32_t target;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedEnableTraceEncoder {
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedDisableTraceEncoder {
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedSetTraceEncoder {
    lean_object* encoder;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedCppCallWithAccept {
    uint32_t serverId;
    uint32_t listenerId;
    std::string address;
    uint32_t portHint;
    uint64_t interfaceId;
    uint16_t methodId;
    std::vector<uint8_t> request;
    std::vector<uint32_t> requestCaps;
    std::shared_ptr<RawCallCompletion> completion;
  };

  struct QueuedCppCallPipelinedWithAccept {
    uint32_t serverId;
    uint32_t listenerId;
    std::string address;
    uint32_t portHint;
    uint64_t interfaceId;
    uint16_t methodId;
    std::vector<uint8_t> request;
    std::vector<uint32_t> requestCaps;
    std::vector<uint8_t> pipelinedRequest;
    std::vector<uint32_t> pipelinedRequestCaps;
    std::shared_ptr<RawCallCompletion> completion;
  };

  struct QueuedRegisterLoopbackTarget {
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedRegisterLoopbackBootstrapTarget {
    uint32_t bootstrapTarget;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedRegisterHandlerTarget {
    lean_object* handler;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedRegisterAdvancedHandlerTarget {
    lean_object* handler;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedRegisterTailCallHandlerTarget {
    lean_object* handler;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedRegisterTailCallTarget {
    uint32_t target;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedRegisterFdTarget {
    uint32_t fd;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedReleaseTarget {
    uint32_t target;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedReleaseTargets {
    std::vector<uint32_t> targets;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedRetainTarget {
    uint32_t target;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedNewPromiseCapability {
    std::shared_ptr<RegisterPairCompletion> completion;
  };

  struct QueuedPromiseCapabilityFulfill {
    uint32_t fulfillerId;
    uint32_t target;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedPromiseCapabilityReject {
    uint32_t fulfillerId;
    uint8_t exceptionTypeTag;
    std::string message;
    std::vector<uint8_t> detailBytes;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedPromiseCapabilityRelease {
    uint32_t fulfillerId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedConnectTarget {
    std::string address;
    uint32_t portHint;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedConnectTargetStart {
    std::string address;
    uint32_t portHint;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedConnectTargetFd {
    uint32_t fd;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedNewTransportPipe {
    std::shared_ptr<RegisterPairCompletion> completion;
  };

  struct QueuedNewTransportFromFd {
    uint32_t fd;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedNewTransportFromFdTake {
    uint32_t fd;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedReleaseTransport {
    uint32_t transportId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedTransportGetFd {
    uint32_t transportId;
    std::shared_ptr<Int64Completion> completion;
  };

  struct QueuedConnectTargetTransport {
    uint32_t transportId;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedListenLoopback {
    std::string address;
    uint32_t portHint;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedAcceptLoopback {
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

  struct QueuedNewClientStart {
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

  struct QueuedClientOnDisconnectStart {
    uint32_t clientId;
    std::shared_ptr<RegisterTargetCompletion> completion;
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

  struct QueuedNewServerWithBootstrapFactory {
    lean_object* bootstrapFactory;
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

  struct QueuedServerAcceptStart {
    uint32_t serverId;
    uint32_t listenerId;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedServerAcceptFd {
    uint32_t serverId;
    uint32_t fd;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedServerAcceptTransport {
    uint32_t serverId;
    uint32_t transportId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedServerDrain {
    uint32_t serverId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedServerDrainStart {
    uint32_t serverId;
    std::shared_ptr<RegisterTargetCompletion> completion;
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

  struct QueuedTargetCount {
    std::shared_ptr<UInt64Completion> completion;
  };

  struct QueuedListenerCount {
    std::shared_ptr<UInt64Completion> completion;
  };

  struct QueuedClientCount {
    std::shared_ptr<UInt64Completion> completion;
  };

  struct QueuedServerCount {
    std::shared_ptr<UInt64Completion> completion;
  };

  struct QueuedPendingCallCount {
    std::shared_ptr<UInt64Completion> completion;
  };

  struct QueuedAwaitRegisterPromise {
    uint32_t promiseId;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedCancelRegisterPromise {
    uint32_t promiseId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedReleaseRegisterPromise {
    uint32_t promiseId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedAwaitUnitPromise {
    uint32_t promiseId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedCancelUnitPromise {
    uint32_t promiseId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedReleaseUnitPromise {
    uint32_t promiseId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedPump {
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedMultiVatNewClient {
    std::string name;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedMultiVatNewServer {
    std::string name;
    uint32_t bootstrapTarget;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedMultiVatNewServerWithBootstrapFactory {
    std::string name;
    lean_object* bootstrapFactory;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedMultiVatReleasePeer {
    uint32_t peerId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedMultiVatBootstrap {
    uint32_t sourcePeerId;
    std::string host;
    bool unique;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedMultiVatBootstrapPeer {
    uint32_t sourcePeerId;
    uint32_t targetPeerId;
    bool unique;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  struct QueuedMultiVatSetForwardingEnabled {
    bool enabled;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedMultiVatResetForwardingStats {
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedMultiVatForwardCount {
    std::shared_ptr<UInt64Completion> completion;
  };

  struct QueuedMultiVatThirdPartyTokenCount {
    std::shared_ptr<UInt64Completion> completion;
  };

  struct QueuedMultiVatDeniedForwardCount {
    std::shared_ptr<UInt64Completion> completion;
  };

  struct QueuedMultiVatHasConnection {
    uint32_t fromPeerId;
    uint32_t toPeerId;
    std::shared_ptr<UInt64Completion> completion;
  };

  struct QueuedMultiVatSetRestorer {
    uint32_t peerId;
    lean_object* restorer;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedMultiVatClearRestorer {
    uint32_t peerId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedMultiVatPublishSturdyRef {
    uint32_t hostPeerId;
    std::vector<uint8_t> objectId;
    uint32_t targetId;
    std::shared_ptr<UnitCompletion> completion;
  };

  struct QueuedMultiVatRestoreSturdyRef {
    uint32_t sourcePeerId;
    std::string host;
    bool unique;
    std::vector<uint8_t> objectId;
    std::shared_ptr<RegisterTargetCompletion> completion;
  };

  using QueuedOperation =
      std::variant<QueuedRawCall, QueuedStartPendingCall, QueuedAwaitPendingCall,
                   QueuedReleasePendingCall, QueuedGetPipelinedCap, QueuedStreamingCall,
                   QueuedTargetGetFd, QueuedTargetWhenResolved, QueuedTargetWhenResolvedStart,
                   QueuedEnableTraceEncoder,
                   QueuedDisableTraceEncoder, QueuedSetTraceEncoder, QueuedCppCallWithAccept,
                   QueuedCppCallPipelinedWithAccept,
                   QueuedRegisterLoopbackTarget, QueuedRegisterLoopbackBootstrapTarget,
                   QueuedRegisterHandlerTarget, QueuedRegisterAdvancedHandlerTarget,
                   QueuedRegisterTailCallHandlerTarget,
                   QueuedRegisterTailCallTarget, QueuedRegisterFdTarget,
                   QueuedReleaseTarget, QueuedReleaseTargets, QueuedRetainTarget,
                   QueuedNewPromiseCapability, QueuedPromiseCapabilityFulfill,
                   QueuedPromiseCapabilityReject, QueuedPromiseCapabilityRelease,
                   QueuedConnectTarget, QueuedConnectTargetStart, QueuedConnectTargetFd,
                   QueuedNewTransportPipe, QueuedNewTransportFromFd, QueuedNewTransportFromFdTake,
                   QueuedReleaseTransport, QueuedTransportGetFd,
                   QueuedConnectTargetTransport,
                   QueuedListenLoopback,
                   QueuedAcceptLoopback, QueuedReleaseListener, QueuedNewClient,
                   QueuedNewClientStart, QueuedReleaseClient, QueuedClientBootstrap,
                   QueuedClientOnDisconnect, QueuedClientOnDisconnectStart,
                   QueuedClientSetFlowLimit,
                   QueuedNewServer, QueuedNewServerWithBootstrapFactory,
                   QueuedReleaseServer, QueuedServerListen, QueuedServerAccept,
                   QueuedServerAcceptStart, QueuedServerAcceptFd, QueuedServerAcceptTransport,
                   QueuedServerDrain, QueuedServerDrainStart,
                   QueuedClientQueueSize, QueuedClientQueueCount,
                   QueuedClientOutgoingWaitNanos, QueuedTargetCount,
                   QueuedListenerCount, QueuedClientCount, QueuedServerCount,
                   QueuedPendingCallCount, QueuedAwaitRegisterPromise,
                   QueuedCancelRegisterPromise, QueuedReleaseRegisterPromise,
                   QueuedAwaitUnitPromise, QueuedCancelUnitPromise,
                   QueuedReleaseUnitPromise, QueuedPump,
                   QueuedMultiVatNewClient, QueuedMultiVatNewServer,
                   QueuedMultiVatNewServerWithBootstrapFactory, QueuedMultiVatReleasePeer,
                   QueuedMultiVatBootstrap, QueuedMultiVatBootstrapPeer,
                   QueuedMultiVatSetForwardingEnabled, QueuedMultiVatResetForwardingStats,
                   QueuedMultiVatForwardCount, QueuedMultiVatThirdPartyTokenCount,
                   QueuedMultiVatDeniedForwardCount,
                   QueuedMultiVatHasConnection, QueuedMultiVatSetRestorer,
                   QueuedMultiVatClearRestorer, QueuedMultiVatPublishSturdyRef,
                   QueuedMultiVatRestoreSturdyRef>;

  struct LoopbackPeer {
    kj::Own<kj::AsyncCapabilityStream> clientStream;
    kj::Own<kj::AsyncCapabilityStream> serverStream;
    kj::Own<capnp::TwoPartyVatNetwork> serverNetwork;
    kj::Own<TwoPartyRpcSystem> serverRpcSystem;
    kj::Own<capnp::TwoPartyClient> client;
  };

  struct NetworkClientPeer {
    kj::Own<kj::AsyncCapabilityStream> stream;
    kj::Own<capnp::TwoPartyVatNetwork> network;
    kj::Own<TwoPartyRpcSystem> rpcSystem;
  };

  struct NetworkServerPeer {
    kj::Maybe<kj::Own<kj::AsyncIoStream>> ioConnection;
    kj::Maybe<kj::Own<kj::AsyncCapabilityStream>> capConnection;
    kj::Own<capnp::TwoPartyVatNetwork> network;
    kj::Own<TwoPartyRpcSystem> rpcSystem;
  };

  struct MultiVatPeer {
    std::string name;
    GenericVat* vat = nullptr;
    kj::Own<GenericRpcSystem> rpcSystem;
    lean_object* sturdyRefRestorer = nullptr;

    ~MultiVatPeer() {
      if (sturdyRefRestorer != nullptr) {
        lean_dec(sturdyRefRestorer);
        sturdyRefRestorer = nullptr;
      }
    }
  };

  class LeanGenericBootstrapFactory final : public capnp::BootstrapFactory<GenericVatId> {
   public:
    LeanGenericBootstrapFactory(RuntimeLoop& runtime, lean_object* bootstrapFactory)
        : runtime_(runtime), bootstrapFactory_(bootstrapFactory) {
      lean_inc(bootstrapFactory_);
    }

    ~LeanGenericBootstrapFactory() { lean_dec(bootstrapFactory_); }

    capnp::Capability::Client createFor(GenericVatId::Reader clientId) override {
      auto decoded = decodeLeanVatId(clientId);
      lean_inc(bootstrapFactory_);
      auto ioResult = lean_apply_3(bootstrapFactory_, lean_mk_string(decoded.host.c_str()),
                                   lean_box(decoded.unique ? 1 : 0), lean_box(0));
      if (lean_io_result_is_error(ioResult)) {
        lean_dec(ioResult);
        throw std::runtime_error("Lean generic bootstrap factory returned IO error");
      }

      auto targetObj = lean_io_result_take_value(ioResult);
      uint32_t targetId = lean_unbox_uint32(targetObj);
      lean_dec(targetObj);

      auto targetIt = runtime_.targets_.find(targetId);
      if (targetIt == runtime_.targets_.end()) {
        throw std::runtime_error(
            "unknown RPC bootstrap capability id from Lean generic bootstrap factory: " +
            std::to_string(targetId));
      }
      return targetIt->second;
    }

   private:
    RuntimeLoop& runtime_;
    lean_object* bootstrapFactory_;
  };

  class LeanBootstrapFactory final : public capnp::BootstrapFactory<capnp::rpc::twoparty::VatId> {
   public:
    LeanBootstrapFactory(RuntimeLoop& runtime, lean_object* bootstrapFactory)
        : runtime_(runtime), bootstrapFactory_(bootstrapFactory) {
      lean_inc(bootstrapFactory_);
    }

    ~LeanBootstrapFactory() { lean_dec(bootstrapFactory_); }

    capnp::Capability::Client createFor(
        capnp::rpc::twoparty::VatId::Reader clientId) override {
      auto side = static_cast<uint16_t>(clientId.getSide());
      lean_inc(bootstrapFactory_);
      auto ioResult = lean_apply_2(bootstrapFactory_, lean_box(static_cast<size_t>(side)),
                                   lean_box(0));
      if (lean_io_result_is_error(ioResult)) {
        lean_dec(ioResult);
        throw std::runtime_error("Lean bootstrap factory returned IO error");
      }

      auto targetObj = lean_io_result_take_value(ioResult);
      uint32_t targetId = lean_unbox_uint32(targetObj);
      lean_dec(targetObj);

      auto targetIt = runtime_.targets_.find(targetId);
      if (targetIt == runtime_.targets_.end()) {
        throw std::runtime_error(
            "unknown RPC bootstrap capability id from Lean bootstrap factory: " +
            std::to_string(targetId));
      }
      return targetIt->second;
    }

   private:
    RuntimeLoop& runtime_;
    lean_object* bootstrapFactory_;
  };

  struct RuntimeServer {
    explicit RuntimeServer(capnp::Capability::Client bootstrap)
        : bootstrap(kj::mv(bootstrap)) {}
    explicit RuntimeServer(
        kj::Own<capnp::BootstrapFactory<capnp::rpc::twoparty::VatId>> bootstrapFactory)
        : bootstrapFactory(kj::mv(bootstrapFactory)) {}
    kj::Maybe<capnp::Capability::Client> bootstrap;
    kj::Maybe<kj::Own<capnp::BootstrapFactory<capnp::rpc::twoparty::VatId>>> bootstrapFactory;
    kj::Vector<kj::Own<NetworkServerPeer>> peers;
  };

  struct PendingCall {
    PendingCall(kj::Promise<capnp::Response<capnp::AnyPointer>>&& promise,
                capnp::AnyPointer::Pipeline&& pipeline, kj::Own<kj::Canceler>&& canceler)
        : promise(kj::mv(promise)), pipeline(kj::mv(pipeline)), canceler(kj::mv(canceler)) {}
    PendingCall(PendingCall&&) = default;
    PendingCall& operator=(PendingCall&&) = default;
    PendingCall(const PendingCall&) = delete;
    PendingCall& operator=(const PendingCall&) = delete;

    kj::Promise<capnp::Response<capnp::AnyPointer>> promise;
    capnp::AnyPointer::Pipeline pipeline;
    kj::Own<kj::Canceler> canceler;
  };

  struct PendingRegisterPromise {
    PendingRegisterPromise(kj::Promise<uint32_t>&& promise, kj::Own<kj::Canceler>&& canceler)
        : promise(kj::mv(promise)), canceler(kj::mv(canceler)) {}
    PendingRegisterPromise(PendingRegisterPromise&&) = default;
    PendingRegisterPromise& operator=(PendingRegisterPromise&&) = default;
    PendingRegisterPromise(const PendingRegisterPromise&) = delete;
    PendingRegisterPromise& operator=(const PendingRegisterPromise&) = delete;

    kj::Promise<uint32_t> promise;
    kj::Own<kj::Canceler> canceler;
  };

  struct PendingUnitPromise {
    PendingUnitPromise(kj::Promise<void>&& promise, kj::Own<kj::Canceler>&& canceler)
        : promise(kj::mv(promise)), canceler(kj::mv(canceler)) {}
    PendingUnitPromise(PendingUnitPromise&&) = default;
    PendingUnitPromise& operator=(PendingUnitPromise&&) = default;
    PendingUnitPromise(const PendingUnitPromise&) = delete;
    PendingUnitPromise& operator=(const PendingUnitPromise&) = delete;

    kj::Promise<void> promise;
    kj::Own<kj::Canceler> canceler;
  };

  struct PendingPromiseCapability {
    PendingPromiseCapability(
        kj::Own<kj::PromiseFulfiller<kj::Own<capnp::ClientHook>>>&& fulfiller,
        uint32_t promiseTarget)
        : fulfiller(kj::mv(fulfiller)), promiseTarget(promiseTarget) {}
    PendingPromiseCapability(PendingPromiseCapability&&) = default;
    PendingPromiseCapability& operator=(PendingPromiseCapability&&) = default;
    PendingPromiseCapability(const PendingPromiseCapability&) = delete;
    PendingPromiseCapability& operator=(const PendingPromiseCapability&) = delete;

    kj::Own<kj::PromiseFulfiller<kj::Own<capnp::ClientHook>>> fulfiller;
    uint32_t promiseTarget = 0;
  };

  void retainPeerOwnership(
      uint32_t sourceTarget, uint32_t retainedTarget,
      std::unordered_map<uint32_t, uint32_t>& ownerByTarget,
      std::unordered_map<uint32_t, uint32_t>& ownerRefCounts) {
    auto ownerIt = ownerByTarget.find(sourceTarget);
    if (ownerIt == ownerByTarget.end()) {
      return;
    }
    auto owner = ownerIt->second;
    ownerByTarget.emplace(retainedTarget, owner);
    auto refIt = ownerRefCounts.find(owner);
    if (refIt == ownerRefCounts.end()) {
      ownerRefCounts.emplace(owner, 1);
      refIt = ownerRefCounts.find(owner);
    }
    ++(refIt->second);
  }

  template <typename PeerMap>
  void releasePeerOwnership(
      uint32_t target, PeerMap& peers, std::unordered_map<uint32_t, uint32_t>& ownerByTarget,
      std::unordered_map<uint32_t, uint32_t>& ownerRefCounts) {
    auto ownerIt = ownerByTarget.find(target);
    if (ownerIt == ownerByTarget.end()) {
      return;
    }
    auto owner = ownerIt->second;
    ownerByTarget.erase(ownerIt);

    auto refIt = ownerRefCounts.find(owner);
    if (refIt == ownerRefCounts.end()) {
      return;
    }
    if (refIt->second > 1) {
      --(refIt->second);
      return;
    }
    ownerRefCounts.erase(refIt);
    peers.erase(owner);
  }

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
      std::vector<uint32_t> requestCapIds;
      auto requestCapEntries = requestCapTable.getTable();
      requestCaps.reserve(requestCapEntries.size() * 4);
      requestCapIds.reserve(requestCapEntries.size());
      for (auto& maybeHook : requestCapEntries) {
        KJ_IF_SOME(hook, maybeHook) {
          auto cap = capnp::Capability::Client(hook->addRef());
          auto capId = runtime_.addTarget(kj::mv(cap));
          appendUint32Le(requestCaps, capId);
          requestCapIds.push_back(capId);
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

      auto cleanupRequestCaps = [&](const std::vector<uint32_t>& retainedCaps) {
        kj::HashSet<uint32_t> retained;
        retained.reserve(retainedCaps.size());
        for (auto capId : retainedCaps) {
          if (capId != 0) {
            retained.insert(capId);
          }
        }
        for (auto capId : requestCapIds) {
          if (!retained.contains(capId)) {
            runtime_.dropTargetIfPresent(capId);
          }
        }
      };

      try {
        kj::ArrayPtr<const kj::byte> responseBytes(
            reinterpret_cast<const kj::byte*>(responseBytesCopy.data()), responseBytesCopy.size());
        kj::ArrayInputStream input(responseBytes);
        capnp::ReaderOptions options;
        options.traversalLimitInWords = 1ull << 30;
        capnp::InputStreamMessageReader reader(input, options);
        auto responseRoot = reader.getRoot<capnp::AnyPointer>();

        auto responseCapIds = decodeCapTable(responseCapsCopy.data(), responseCapsCopy.size());
        cleanupRequestCaps(responseCapIds);
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
      } catch (...) {
        cleanupRequestCaps({});
        throw;
      }

      return {kj::READY_NOW, false};
    }

  private:
    RuntimeLoop& runtime_;
    std::shared_ptr<std::atomic<uint32_t>> targetId_;
    lean_object* handler_;
  };

  class LeanTailCallHandlerServer final : public capnp::Capability::Server {
   public:
    LeanTailCallHandlerServer(RuntimeLoop& runtime,
                              std::shared_ptr<std::atomic<uint32_t>> targetId,
                              lean_object* handler)
        : runtime_(runtime), targetId_(kj::mv(targetId)), handler_(handler) {
      lean_inc(handler_);
    }

    ~LeanTailCallHandlerServer() { lean_dec(handler_); }

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
      std::vector<uint32_t> requestCapIds;
      auto requestCapEntries = requestCapTable.getTable();
      requestCaps.reserve(requestCapEntries.size() * 4);
      requestCapIds.reserve(requestCapEntries.size());
      for (auto& maybeHook : requestCapEntries) {
        KJ_IF_SOME(hook, maybeHook) {
          auto cap = capnp::Capability::Client(hook->addRef());
          auto capId = runtime_.addTarget(kj::mv(cap));
          appendUint32Le(requestCaps, capId);
          requestCapIds.push_back(capId);
        } else {
          appendUint32Le(requestCaps, 0);
        }
      }
      const auto* requestCapsPtr = requestCaps.empty() ? nullptr : requestCaps.data();
      auto requestCapsObj = mkByteArrayCopy(requestCapsPtr, requestCaps.size());

      auto cleanupRequestCaps = [&](uint32_t retainedCapId) {
        for (auto capId : requestCapIds) {
          if (capId == 0 || capId == retainedCapId) {
            continue;
          }
          runtime_.dropTargetIfPresent(capId);
        }
      };

      auto targetId = targetId_->load(std::memory_order_relaxed);
      lean_inc(handler_);
      auto ioResult = lean_apply_6(handler_, lean_box_uint32(targetId), lean_box_uint64(interfaceId),
                                   lean_box(static_cast<size_t>(methodId)), requestObj,
                                   requestCapsObj, lean_box(0));
      if (lean_io_result_is_error(ioResult)) {
        cleanupRequestCaps(0);
        lean_dec(ioResult);
        throw std::runtime_error("Lean RPC tail-call handler returned IO error");
      }

      auto targetObj = lean_io_result_take_value(ioResult);
      uint32_t forwardTargetId = lean_unbox_uint32(targetObj);
      lean_dec(targetObj);

      auto targetIt = runtime_.targets_.find(forwardTargetId);
      if (targetIt == runtime_.targets_.end()) {
        cleanupRequestCaps(0);
        throw std::runtime_error("unknown RPC tail-call target capability id from Lean handler: " +
                                 std::to_string(forwardTargetId));
      }

      cleanupRequestCaps(forwardTargetId);

      auto requestBuilder = targetIt->second.typelessRequest(interfaceId, methodId, kj::none, {});
      requestBuilder.setAs<capnp::AnyPointer>(context.getParams().getAs<capnp::AnyPointer>());
      return {context.tailCall(kj::mv(requestBuilder)), false};
    }

   private:
    RuntimeLoop& runtime_;
    std::shared_ptr<std::atomic<uint32_t>> targetId_;
    lean_object* handler_;
  };

  class LeanAdvancedCapabilityServer final : public capnp::Capability::Server {
   public:
    LeanAdvancedCapabilityServer(RuntimeLoop& runtime,
                                 std::shared_ptr<std::atomic<uint32_t>> targetId,
                                 lean_object* handler)
        : runtime_(runtime), targetId_(kj::mv(targetId)), handler_(handler) {
      lean_inc(handler_);
    }

    ~LeanAdvancedCapabilityServer() { lean_dec(handler_); }

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
      std::vector<uint32_t> requestCapIds;
      auto requestCapEntries = requestCapTable.getTable();
      requestCaps.reserve(requestCapEntries.size() * 4);
      requestCapIds.reserve(requestCapEntries.size());
      for (auto& maybeHook : requestCapEntries) {
        KJ_IF_SOME(hook, maybeHook) {
          auto cap = capnp::Capability::Client(hook->addRef());
          auto capId = runtime_.addTarget(kj::mv(cap));
          appendUint32Le(requestCaps, capId);
          requestCapIds.push_back(capId);
        } else {
          appendUint32Le(requestCaps, 0);
        }
      }
      const auto* requestCapsPtr = requestCaps.empty() ? nullptr : requestCaps.data();
      auto requestCapsObj = mkByteArrayCopy(requestCapsPtr, requestCaps.size());
      auto cleanupState =
          std::make_shared<RequestCapCleanupState>(std::move(requestCapIds));

      auto targetId = targetId_->load(std::memory_order_relaxed);
      lean_inc(handler_);
      auto ioResult = lean_apply_6(handler_, lean_box_uint32(targetId), lean_box_uint64(interfaceId),
                                   lean_box(static_cast<size_t>(methodId)), requestObj,
                                   requestCapsObj, lean_box(0));
      if (lean_io_result_is_error(ioResult)) {
        cleanupRequestCaps(cleanupState, {});
        lean_dec(ioResult);
        throw std::runtime_error("Lean RPC advanced handler returned IO error");
      }

      auto actionObj = lean_io_result_take_value(ioResult);
      auto action = decodeAction(actionObj);
      lean_dec(actionObj);

      try {
        return dispatchDecodedAction(kj::mv(action), kj::mv(context), cleanupState);
      } catch (...) {
        cleanupRequestCaps(cleanupState, {});
        throw;
      }
    }

   private:
    struct RequestCapCleanupState {
      explicit RequestCapCleanupState(std::vector<uint32_t>&& capIds)
          : requestCapIds(kj::mv(capIds)) {}
      std::vector<uint32_t> requestCapIds;
      bool done = false;
    };

    void cleanupRequestCaps(const std::shared_ptr<RequestCapCleanupState>& cleanupState,
                            const std::vector<uint32_t>& retainedCaps) {
      if (cleanupState->done) {
        return;
      }
      kj::HashSet<uint32_t> retained;
      retained.reserve(retainedCaps.size());
      for (auto capId : retainedCaps) {
        if (capId != 0) {
          retained.insert(capId);
        }
      }
      for (auto capId : cleanupState->requestCapIds) {
        if (!retained.contains(capId)) {
          runtime_.dropTargetIfPresent(capId);
        }
      }
      cleanupState->done = true;
    }

    kj::Promise<LeanAdvancedHandlerAction> waitLeanActionTask(
        const std::shared_ptr<DeferredLeanTaskState>& taskState) {
      constexpr uint8_t kLeanTaskStateFinished = 2;
      if (lean_io_get_task_state_core(taskState->waitTask->task) == kLeanTaskStateFinished) {
        auto taskResult = lean_task_get(taskState->waitTask->task);
        taskState->completed.store(true, std::memory_order_release);
        auto taskResultTag = lean_obj_tag(taskResult);
        if (taskResultTag == 0) {
          throw std::runtime_error("Lean RPC advanced deferred handler task returned IO error");
        }
        if (taskResultTag != 1) {
          throw std::runtime_error("Lean RPC advanced deferred handler task returned invalid result");
        }
        auto actionObj = lean_ctor_get(taskResult, 0);
        lean_inc(actionObj);
        auto action = decodeAction(actionObj);
        lean_dec(actionObj);
        return action;
      }
      return kj::yield().then([this, taskState]() mutable {
        return waitLeanActionTask(taskState);
      });
    }

    DispatchCallResult dispatchDecodedAction(
        LeanAdvancedHandlerAction action,
        capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer> context,
        const std::shared_ptr<RequestCapCleanupState>& cleanupState) {
      capnp::Capability::Client::CallHints callHints;
      callHints.noPromisePipelining = action.noPromisePipelining;
      callHints.onlyPromisePipeline = action.onlyPromisePipeline;

      if ((action.sendResultsToCaller || action.noPromisePipelining || action.onlyPromisePipeline) &&
          action.kind != LeanAdvancedHandlerAction::Kind::ASYNC_CALL) {
        cleanupRequestCaps(cleanupState, {});
        throw std::runtime_error(
            "Lean RPC advanced handler: forward options are only valid with forwardCall");
      }
      if (action.remoteExceptionType != kj::Exception::Type::FAILED &&
          action.kind != LeanAdvancedHandlerAction::Kind::THROW_REMOTE) {
        cleanupRequestCaps(cleanupState, {});
        throw std::runtime_error(
            "Lean RPC advanced handler: exceptionType is only valid with throwRemote");
      }

      if (action.releaseParams) {
        context.releaseParams();
      }

      if (action.hasPipeline) {
        if (action.kind != LeanAdvancedHandlerAction::Kind::AWAIT_TASK) {
          throw std::runtime_error(
              "Lean RPC advanced handler: setPipeline is only valid with defer");
        }
        setContextPipelineFromPayload(context, action.pipelineBytes, action.pipelineCaps);
      }

      if (action.kind == LeanAdvancedHandlerAction::Kind::RETURN_PAYLOAD) {
        auto responseCapIds = decodeCapTable(action.payloadCaps.data(), action.payloadCaps.size());
        if (action.isStreaming) {
          cleanupRequestCaps(cleanupState, {});
          for (auto capId : responseCapIds) {
            if (capId != 0) {
              runtime_.dropTargetIfPresent(capId);
            }
          }
        } else {
          cleanupRequestCaps(cleanupState, responseCapIds);
          setContextResultsFromPayload(context, action.payloadBytes, action.payloadCaps);
        }
        return {kj::READY_NOW, action.isStreaming, action.allowCancellation};
      }

      if (action.kind == LeanAdvancedHandlerAction::Kind::ASYNC_CALL) {
        auto targetIt = runtime_.targets_.find(action.target);
        if (targetIt == runtime_.targets_.end()) {
          throw std::runtime_error(
              "unknown RPC async-call target capability id from Lean handler: " +
              std::to_string(action.target));
        }
        auto requestCapIdsOut = decodeCapTable(action.payloadCaps.data(), action.payloadCaps.size());
        auto requestBuilder =
            targetIt->second.typelessRequest(action.interfaceId, action.methodId, kj::none, callHints);
        runtime_.setRequestPayload(requestBuilder, action.payloadBytes, requestCapIdsOut);
        cleanupRequestCaps(cleanupState, {});

        if (action.sendResultsToCaller) {
          return {context.tailCall(kj::mv(requestBuilder)), action.isStreaming,
                  action.allowCancellation};
        }
        if (action.onlyPromisePipeline) {
          throw std::runtime_error(
              "Lean RPC advanced handler: onlyPromisePipeline requires sendResultsTo.caller");
        }

        auto promiseAndPipeline = requestBuilder.send();
        if (action.isStreaming) {
          auto completion = promiseAndPipeline.then(
              [](capnp::Response<capnp::AnyPointer>&&) mutable {});
          return {kj::mv(completion), true, action.allowCancellation};
        }
        context.setPipeline(promiseAndPipeline.noop());
        auto completion = promiseAndPipeline.then(
            [this, context = kj::mv(context)](
                capnp::Response<capnp::AnyPointer>&& response) mutable {
              setContextResultsFromResponse(context, response);
            });
        return {kj::mv(completion), action.isStreaming, action.allowCancellation};
      }

      if (action.kind == LeanAdvancedHandlerAction::Kind::TAIL_CALL) {
        auto targetIt = runtime_.targets_.find(action.target);
        if (targetIt == runtime_.targets_.end()) {
          throw std::runtime_error(
              "unknown RPC tail-call target capability id from Lean advanced handler: " +
              std::to_string(action.target));
        }
        auto requestCapIdsOut = decodeCapTable(action.payloadCaps.data(), action.payloadCaps.size());
        auto requestBuilder =
            targetIt->second.typelessRequest(action.interfaceId, action.methodId, kj::none, callHints);
        runtime_.setRequestPayload(requestBuilder, action.payloadBytes, requestCapIdsOut);
        cleanupRequestCaps(cleanupState, {});
        return {context.tailCall(kj::mv(requestBuilder)), action.isStreaming,
                action.allowCancellation};
      }

      if (action.kind == LeanAdvancedHandlerAction::Kind::THROW_REMOTE) {
        cleanupRequestCaps(cleanupState, {});
        throwRemoteException(action.remoteExceptionType, action.message, action.detailBytes);
      }

      auto deferredTaskState =
          std::make_shared<DeferredLeanTaskState>(kj::mv(action.deferredWaitTask),
                                                  kj::mv(action.deferredCancelTask),
                                                  action.allowCancellation);
      if (action.allowCancellation) {
        if (runtime_.pendingDeferredCancelRequests_ > 0) {
          --runtime_.pendingDeferredCancelRequests_;
          deferredTaskState->requestCancellation();
        } else {
          runtime_.activeCancelableDeferredTasks_.push_back(deferredTaskState);
        }
      }
      auto completion = waitLeanActionTask(deferredTaskState).then(
          [this, context = kj::mv(context), cleanupState, earlyAllowCancellation = action.allowCancellation,
           earlyIsStreaming = action.isStreaming](
              LeanAdvancedHandlerAction nextAction) mutable -> kj::Promise<void> {
            if (nextAction.allowCancellation && !earlyAllowCancellation) {
              cleanupRequestCaps(cleanupState, {});
              throw std::runtime_error(
                  "Lean RPC advanced deferred handler: allowCancellation must be set before defer");
            }
            if (nextAction.isStreaming && !earlyIsStreaming) {
              cleanupRequestCaps(cleanupState, {});
              throw std::runtime_error(
                  "Lean RPC advanced deferred handler: isStreaming must be set before defer");
            }
            auto nextResult = dispatchDecodedAction(kj::mv(nextAction), kj::mv(context), cleanupState);
            return kj::mv(nextResult.promise).then(
                []() {},
                [this, cleanupState](kj::Exception&& e) {
                  cleanupRequestCaps(cleanupState, {});
                  throw kj::mv(e);
                });
          },
          [this, cleanupState](kj::Exception&& e) -> kj::Promise<void> {
            cleanupRequestCaps(cleanupState, {});
            return kj::mv(e);
          }).attach(std::move(deferredTaskState));
      return {kj::mv(completion), action.isStreaming, action.allowCancellation};
    }

    static LeanAdvancedHandlerAction decodeAction(lean_object* actionObj) {
      LeanAdvancedHandlerAction action;
      constexpr unsigned kRawAdvancedWrapperScalarBase = sizeof(void*) * 1;
      constexpr unsigned kRawAdvancedControlReleaseOffset = kRawAdvancedWrapperScalarBase;
      constexpr unsigned kRawAdvancedControlAllowOffset = kRawAdvancedWrapperScalarBase + 1;
      constexpr unsigned kRawAdvancedControlStreamingOffset = kRawAdvancedWrapperScalarBase + 2;
      constexpr unsigned kRawAdvancedHintsNoPromiseOffset = kRawAdvancedWrapperScalarBase;
      constexpr unsigned kRawAdvancedHintsOnlyPipelineOffset = kRawAdvancedWrapperScalarBase + 1;
      constexpr unsigned kRawAdvancedExceptionTypeOffset = kRawAdvancedWrapperScalarBase;
      constexpr unsigned kRawAdvancedScalarBase = sizeof(void*) * 2;
      constexpr unsigned kRawAdvancedInterfaceIdOffset = kRawAdvancedScalarBase;
      constexpr unsigned kRawAdvancedTargetOffset = kRawAdvancedScalarBase + 8;
      constexpr unsigned kRawAdvancedMethodIdOffset = kRawAdvancedScalarBase + 12;

      while (true) {
        auto tag = lean_obj_tag(actionObj);
        if (tag == 4) {
          if (lean_ctor_get_uint8(actionObj, kRawAdvancedControlReleaseOffset) != 0) {
            action.releaseParams = true;
          }
          if (lean_ctor_get_uint8(actionObj, kRawAdvancedControlAllowOffset) != 0) {
            action.allowCancellation = true;
          }
          if (lean_ctor_get_uint8(actionObj, kRawAdvancedControlStreamingOffset) != 0) {
            action.isStreaming = true;
          }
          actionObj = lean_ctor_get(actionObj, 0);
          continue;
        }
        if (tag == 6) {
          action.sendResultsToCaller = true;
          actionObj = lean_ctor_get(actionObj, 0);
          continue;
        }
        if (tag == 7) {
          if (lean_ctor_get_uint8(actionObj, kRawAdvancedHintsNoPromiseOffset) != 0) {
            action.noPromisePipelining = true;
          }
          if (lean_ctor_get_uint8(actionObj, kRawAdvancedHintsOnlyPipelineOffset) != 0) {
            action.onlyPromisePipeline = true;
          }
          actionObj = lean_ctor_get(actionObj, 0);
          continue;
        }
        if (tag == 8) {
          auto exTypeTag = lean_ctor_get_uint8(actionObj, kRawAdvancedExceptionTypeOffset);
          switch (exTypeTag) {
            case 0:
              action.remoteExceptionType = kj::Exception::Type::FAILED;
              break;
            case 1:
              action.remoteExceptionType = kj::Exception::Type::OVERLOADED;
              break;
            case 2:
              action.remoteExceptionType = kj::Exception::Type::DISCONNECTED;
              break;
            case 3:
              action.remoteExceptionType = kj::Exception::Type::UNIMPLEMENTED;
              break;
            default:
              action.remoteExceptionType = kj::Exception::Type::FAILED;
              break;
          }
          actionObj = lean_ctor_get(actionObj, 0);
          continue;
        }
        if (tag == 9) {
          if (action.hasPipeline) {
            throw std::runtime_error("Lean RPC advanced handler: setPipeline may only be specified once");
          }
          action.hasPipeline = true;
          action.pipelineBytes = copyByteArray(lean_ctor_get(actionObj, 0));
          action.pipelineCaps = copyByteArray(lean_ctor_get(actionObj, 1));
          actionObj = lean_ctor_get(actionObj, 2);
          continue;
        }

        switch (tag) {
          case 0: {
            action.kind = LeanAdvancedHandlerAction::Kind::RETURN_PAYLOAD;
            action.payloadBytes = copyByteArray(lean_ctor_get(actionObj, 0));
            action.payloadCaps = copyByteArray(lean_ctor_get(actionObj, 1));
            return action;
          }
          case 1: {
            action.kind = LeanAdvancedHandlerAction::Kind::ASYNC_CALL;
            action.interfaceId = lean_ctor_get_uint64(actionObj, kRawAdvancedInterfaceIdOffset);
            action.target = lean_ctor_get_uint32(actionObj, kRawAdvancedTargetOffset);
            action.methodId = lean_ctor_get_uint16(actionObj, kRawAdvancedMethodIdOffset);
            action.payloadBytes = copyByteArray(lean_ctor_get(actionObj, 0));
            action.payloadCaps = copyByteArray(lean_ctor_get(actionObj, 1));
            return action;
          }
          case 2: {
            action.kind = LeanAdvancedHandlerAction::Kind::TAIL_CALL;
            action.interfaceId = lean_ctor_get_uint64(actionObj, kRawAdvancedInterfaceIdOffset);
            action.target = lean_ctor_get_uint32(actionObj, kRawAdvancedTargetOffset);
            action.methodId = lean_ctor_get_uint16(actionObj, kRawAdvancedMethodIdOffset);
            action.payloadBytes = copyByteArray(lean_ctor_get(actionObj, 0));
            action.payloadCaps = copyByteArray(lean_ctor_get(actionObj, 1));
            return action;
          }
          case 3: {
            action.kind = LeanAdvancedHandlerAction::Kind::THROW_REMOTE;
            action.message = std::string(lean_string_cstr(lean_ctor_get(actionObj, 0)));
            action.detailBytes = copyByteArray(lean_ctor_get(actionObj, 1));
            return action;
          }
          case 5: {
            action.kind = LeanAdvancedHandlerAction::Kind::AWAIT_TASK;
            auto waitTaskObj = lean_ctor_get(actionObj, 0);
            auto cancelTaskObj = lean_ctor_get(actionObj, 1);
            lean_inc(waitTaskObj);
            lean_inc(cancelTaskObj);
            action.deferredWaitTask = kj::heap<DeferredLeanTask>(waitTaskObj);
            action.deferredCancelTask = kj::heap<DeferredLeanTask>(cancelTaskObj);
            return action;
          }
          default:
            throw std::runtime_error("unknown Lean RPC advanced handler result tag");
        }
      }
    }

    void setContextResultsFromPayload(
        capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer>& context,
        const std::vector<uint8_t>& responseBytesCopy,
        const std::vector<uint8_t>& responseCapsCopy) {
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
            throw std::runtime_error(
                "unknown RPC response capability id from Lean advanced handler: " +
                std::to_string(capId));
          }
          capnp::Capability::Client cap = capIt->second;
          capTableBuilder.add(capnp::ClientHook::from(kj::mv(cap)));
        }
        capnp::ReaderCapabilityTable responseCapTable(capTableBuilder.finish());
        context.getResults().setAs<capnp::AnyPointer>(responseCapTable.imbue(responseRoot));
      }
    }

    void setContextPipelineFromPayload(
        capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer>& context,
        const std::vector<uint8_t>& pipelineBytesCopy,
        const std::vector<uint8_t>& pipelineCapsCopy) {
      if (pipelineBytesCopy.empty()) {
        if (!pipelineCapsCopy.empty()) {
          throw std::runtime_error(
              "RPC pipeline capability table requires a non-empty payload");
        }
        return;
      }

      kj::ArrayPtr<const kj::byte> pipelineBytes(
          reinterpret_cast<const kj::byte*>(pipelineBytesCopy.data()), pipelineBytesCopy.size());
      kj::ArrayInputStream input(pipelineBytes);
      capnp::ReaderOptions options;
      options.traversalLimitInWords = 1ull << 30;
      capnp::InputStreamMessageReader reader(input, options);
      auto pipelineRoot = reader.getRoot<capnp::AnyPointer>();

      capnp::PipelineBuilder<capnp::AnyPointer> pipelineBuilder;
      if (pipelineCapsCopy.empty()) {
        pipelineBuilder.setAs<capnp::AnyPointer>(pipelineRoot);
      } else {
        auto pipelineCapIds = decodeCapTable(pipelineCapsCopy.data(), pipelineCapsCopy.size());
        auto capTableBuilder =
            kj::heapArrayBuilder<kj::Maybe<kj::Own<capnp::ClientHook>>>(pipelineCapIds.size());
        for (auto capId : pipelineCapIds) {
          if (capId == 0) {
            capTableBuilder.add(kj::none);
            continue;
          }
          auto capIt = runtime_.targets_.find(capId);
          if (capIt == runtime_.targets_.end()) {
            throw std::runtime_error(
                "unknown RPC pipeline capability id: " + std::to_string(capId));
          }
          capnp::Capability::Client cap = capIt->second;
          capTableBuilder.add(capnp::ClientHook::from(kj::mv(cap)));
        }
        capnp::ReaderCapabilityTable pipelineCapTable(capTableBuilder.finish());
        pipelineBuilder.setAs<capnp::AnyPointer>(pipelineCapTable.imbue(pipelineRoot));
      }

      context.setPipeline(pipelineBuilder.build());
    }

    void setContextResultsFromResponse(
        capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer>& context,
        capnp::Response<capnp::AnyPointer>& response) {
      auto raw = runtime_.serializeResponse(response);
      setContextResultsFromPayload(context, raw.response, raw.responseCaps);
    }

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

  uint32_t addPendingCall(PendingCall&& pendingCall) {
    uint32_t pendingCallId = nextPendingCallId_++;
    while (pendingCalls_.find(pendingCallId) != pendingCalls_.end()) {
      pendingCallId = nextPendingCallId_++;
    }
    pendingCalls_.emplace(pendingCallId, std::move(pendingCall));
    return pendingCallId;
  }

  uint32_t addRegisterPromise(PendingRegisterPromise&& promise) {
    uint32_t promiseId = nextRegisterPromiseId_++;
    while (registerPromises_.find(promiseId) != registerPromises_.end()) {
      promiseId = nextRegisterPromiseId_++;
    }
    registerPromises_.emplace(promiseId, std::move(promise));
    return promiseId;
  }

  uint32_t addUnitPromise(PendingUnitPromise&& promise) {
    uint32_t promiseId = nextUnitPromiseId_++;
    while (unitPromises_.find(promiseId) != unitPromises_.end()) {
      promiseId = nextUnitPromiseId_++;
    }
    unitPromises_.emplace(promiseId, std::move(promise));
    return promiseId;
  }

  uint32_t addPromiseCapabilityFulfiller(PendingPromiseCapability&& promise) {
    uint32_t fulfillerId = nextPromiseCapabilityFulfillerId_++;
    while (promiseCapabilityFulfillers_.find(fulfillerId) != promiseCapabilityFulfillers_.end()) {
      fulfillerId = nextPromiseCapabilityFulfillerId_++;
    }
    promiseCapabilityFulfillers_.emplace(fulfillerId, std::move(promise));
    return fulfillerId;
  }

  uint32_t addMultiVatPeer(kj::Own<MultiVatPeer>&& peer) {
    uint32_t peerId = nextMultiVatPeerId_++;
    while (multiVatPeers_.find(peerId) != multiVatPeers_.end()) {
      peerId = nextMultiVatPeerId_++;
    }
    multiVatPeerIdsByName_[peer->name] = peerId;
    multiVatPeers_.emplace(peerId, kj::mv(peer));
    return peerId;
  }

  MultiVatPeer& requireMultiVatPeer(uint32_t peerId) {
    auto peerIt = multiVatPeers_.find(peerId);
    if (peerIt == multiVatPeers_.end()) {
      throw std::runtime_error("unknown multi-vat peer id: " + std::to_string(peerId));
    }
    return *peerIt->second;
  }

  MultiVatPeer& requireMultiVatPeerByHost(const std::string& host) {
    auto idIt = multiVatPeerIdsByName_.find(host);
    if (idIt == multiVatPeerIdsByName_.end()) {
      throw std::runtime_error("unknown multi-vat host: " + host);
    }
    return requireMultiVatPeer(idIt->second);
  }

  static std::string sturdyObjectKey(const std::vector<uint8_t>& objectId) {
    return std::string(reinterpret_cast<const char*>(objectId.data()), objectId.size());
  }

  uint32_t newMultiVatClient(const std::string& name) {
    auto& vat = genericVatNetwork_.add(name);
    auto peer = kj::heap<MultiVatPeer>();
    peer->name = name;
    peer->vat = &vat;
    peer->rpcSystem = kj::heap<GenericRpcSystem>(capnp::makeRpcClient(vat));
    return addMultiVatPeer(kj::mv(peer));
  }

  uint32_t newMultiVatServer(const std::string& name, uint32_t bootstrapTarget) {
    auto targetIt = targets_.find(bootstrapTarget);
    if (targetIt == targets_.end()) {
      throw std::runtime_error("unknown RPC target capability id: " +
                               std::to_string(bootstrapTarget));
    }
    auto& vat = genericVatNetwork_.add(name);
    auto peer = kj::heap<MultiVatPeer>();
    peer->name = name;
    peer->vat = &vat;
    peer->rpcSystem = kj::heap<GenericRpcSystem>(capnp::makeRpcServer(vat, targetIt->second));
    return addMultiVatPeer(kj::mv(peer));
  }

  uint32_t newMultiVatServerWithBootstrapFactory(const std::string& name,
                                                 lean_object* bootstrapFactory) {
    auto& vat = genericVatNetwork_.add(name);
    auto peer = kj::heap<MultiVatPeer>();
    peer->name = name;
    peer->vat = &vat;
    auto factory = kj::heap<LeanGenericBootstrapFactory>(*this, bootstrapFactory);
    peer->rpcSystem = kj::heap<GenericRpcSystem>(capnp::makeRpcServer(vat, *factory));
    // Keep bootstrap factory alive for the lifetime of the RPC system.
    genericBootstrapFactories_.add(kj::mv(factory));
    return addMultiVatPeer(kj::mv(peer));
  }

  void releaseMultiVatPeer(uint32_t peerId) {
    auto peerIt = multiVatPeers_.find(peerId);
    if (peerIt == multiVatPeers_.end()) {
      throw std::runtime_error("unknown multi-vat peer id: " + std::to_string(peerId));
    }
    multiVatPeerIdsByName_.erase(peerIt->second->name);
    sturdyRefs_.erase(peerId);
    multiVatPeers_.erase(peerIt);
  }

  uint32_t multiVatBootstrap(uint32_t sourcePeerId, const LeanVatId& vatId) {
    auto& source = requireMultiVatPeer(sourcePeerId);
    capnp::MallocMessageBuilder message;
    auto hostId = message.initRoot<GenericVatId>();
    setLeanVatId(hostId, vatId);
    auto cap = source.rpcSystem->bootstrap(hostId.asReader());
    return addTarget(kj::mv(cap));
  }

  uint32_t multiVatBootstrapPeer(uint32_t sourcePeerId, uint32_t targetPeerId, bool unique) {
    auto& target = requireMultiVatPeer(targetPeerId);
    return multiVatBootstrap(sourcePeerId, LeanVatId{target.name, unique});
  }

  void multiVatSetForwardingEnabled(bool enabled) {
    genericVatNetwork_.setForwardingEnabled(enabled);
  }

  void multiVatResetForwardingStats() {
    genericVatNetwork_.resetForwardingStats();
  }

  uint64_t multiVatForwardCount() const {
    return genericVatNetwork_.forwardCount();
  }

  uint64_t multiVatThirdPartyTokenCount() const {
    return genericVatNetwork_.tokenCount();
  }

  uint64_t multiVatDeniedForwardCount() const {
    return genericVatNetwork_.deniedForwardCount();
  }

  bool multiVatHasConnection(uint32_t fromPeerId, uint32_t toPeerId) {
    auto& from = requireMultiVatPeer(fromPeerId);
    auto& to = requireMultiVatPeer(toPeerId);
    auto conn = from.vat->getConnectionTo(*to.vat);
    return conn != kj::none;
  }

  void multiVatSetRestorer(uint32_t peerId, lean_object* restorer) {
    auto& peer = requireMultiVatPeer(peerId);
    if (peer.sturdyRefRestorer != nullptr) {
      lean_dec(peer.sturdyRefRestorer);
    }
    peer.sturdyRefRestorer = restorer;
  }

  void multiVatClearRestorer(uint32_t peerId) {
    auto& peer = requireMultiVatPeer(peerId);
    if (peer.sturdyRefRestorer != nullptr) {
      lean_dec(peer.sturdyRefRestorer);
      peer.sturdyRefRestorer = nullptr;
    }
  }

  void multiVatPublishSturdyRef(uint32_t hostPeerId, const std::vector<uint8_t>& objectId,
                                uint32_t targetId) {
    requireMultiVatPeer(hostPeerId);
    auto targetIt = targets_.find(targetId);
    if (targetIt == targets_.end()) {
      throw std::runtime_error("unknown RPC target capability id: " + std::to_string(targetId));
    }
    auto& hostRefs = sturdyRefs_[hostPeerId];
    hostRefs.insert_or_assign(sturdyObjectKey(objectId), targetIt->second);
  }

  uint32_t multiVatRestoreSturdyRef(uint32_t sourcePeerId, const LeanVatId& hostVatId,
                                    const std::vector<uint8_t>& objectId) {
    auto& source = requireMultiVatPeer(sourcePeerId);
    auto& host = requireMultiVatPeerByHost(hostVatId.host);

    if (host.sturdyRefRestorer != nullptr) {
      lean_inc(host.sturdyRefRestorer);
      auto objectIdObj =
          mkByteArrayCopy(objectId.empty() ? nullptr : objectId.data(), objectId.size());
      auto ioResult = lean_apply_4(host.sturdyRefRestorer, lean_mk_string(source.name.c_str()),
                                   lean_box(hostVatId.unique ? 1 : 0), objectIdObj, lean_box(0));
      if (lean_io_result_is_error(ioResult)) {
        lean_dec(ioResult);
        throw std::runtime_error("multi-vat sturdy ref restorer returned IO error");
      }
      auto targetObj = lean_io_result_take_value(ioResult);
      uint32_t targetId = lean_unbox_uint32(targetObj);
      lean_dec(targetObj);
      return retainTarget(targetId);
    }

    auto hostRefsIt = sturdyRefs_.find(multiVatPeerIdsByName_.at(host.name));
    if (hostRefsIt == sturdyRefs_.end()) {
      throw std::runtime_error("no sturdy refs published for host: " + host.name);
    }
    auto key = sturdyObjectKey(objectId);
    auto refIt = hostRefsIt->second.find(key);
    if (refIt == hostRefsIt->second.end()) {
      throw std::runtime_error("unknown sturdy ref object id");
    }
    return addTarget(refIt->second);
  }

  uint32_t awaitRegisterPromise(kj::WaitScope& waitScope, uint32_t promiseId) {
    auto it = registerPromises_.find(promiseId);
    if (it == registerPromises_.end()) {
      throw std::runtime_error("unknown RPC register promise id: " + std::to_string(promiseId));
    }
    auto pending = kj::mv(it->second);
    registerPromises_.erase(it);
    pending.canceler->release();
    return kj::mv(pending.promise).wait(waitScope);
  }

  void cancelRegisterPromise(uint32_t promiseId) {
    auto it = registerPromises_.find(promiseId);
    if (it == registerPromises_.end()) {
      throw std::runtime_error("unknown RPC register promise id: " + std::to_string(promiseId));
    }
    it->second.canceler->cancel("Capnp.Rpc register promise canceled from Lean");
  }

  void releaseRegisterPromise(uint32_t promiseId) {
    auto it = registerPromises_.find(promiseId);
    if (it == registerPromises_.end()) {
      throw std::runtime_error("unknown RPC register promise id: " + std::to_string(promiseId));
    }
    registerPromises_.erase(it);
  }

  void awaitUnitPromise(kj::WaitScope& waitScope, uint32_t promiseId) {
    auto it = unitPromises_.find(promiseId);
    if (it == unitPromises_.end()) {
      throw std::runtime_error("unknown RPC unit promise id: " + std::to_string(promiseId));
    }
    auto pending = kj::mv(it->second);
    unitPromises_.erase(it);
    pending.canceler->release();
    kj::mv(pending.promise).wait(waitScope);
  }

  void cancelUnitPromise(uint32_t promiseId) {
    auto it = unitPromises_.find(promiseId);
    if (it == unitPromises_.end()) {
      throw std::runtime_error("unknown RPC unit promise id: " + std::to_string(promiseId));
    }
    it->second.canceler->cancel("Capnp.Rpc unit promise canceled from Lean");
  }

  void releaseUnitPromise(uint32_t promiseId) {
    auto it = unitPromises_.find(promiseId);
    if (it == unitPromises_.end()) {
      throw std::runtime_error("unknown RPC unit promise id: " + std::to_string(promiseId));
    }
    unitPromises_.erase(it);
  }

  uint32_t registerHandlerTarget(lean_object* handler) {
    auto targetIdRef = std::make_shared<std::atomic<uint32_t>>(0);
    auto server = kj::heap<LeanCapabilityServer>(*this, targetIdRef, handler);
    auto targetId = addTarget(capnp::Capability::Client(kj::mv(server)));
    targetIdRef->store(targetId, std::memory_order_relaxed);
    return targetId;
  }

  uint32_t registerAdvancedHandlerTarget(lean_object* handler) {
    auto targetIdRef = std::make_shared<std::atomic<uint32_t>>(0);
    auto server = kj::heap<LeanAdvancedCapabilityServer>(*this, targetIdRef, handler);
    auto targetId = addTarget(capnp::Capability::Client(kj::mv(server)));
    targetIdRef->store(targetId, std::memory_order_relaxed);
    return targetId;
  }

  uint32_t registerTailCallHandlerTarget(lean_object* handler) {
    auto targetIdRef = std::make_shared<std::atomic<uint32_t>>(0);
    auto server = kj::heap<LeanTailCallHandlerServer>(*this, targetIdRef, handler);
    auto targetId = addTarget(capnp::Capability::Client(kj::mv(server)));
    targetIdRef->store(targetId, std::memory_order_relaxed);
    return targetId;
  }

  uint32_t registerTailCallTarget(uint32_t target) {
    auto targetIt = targets_.find(target);
    if (targetIt == targets_.end()) {
      throw std::runtime_error("unknown RPC target capability id: " + std::to_string(target));
    }

    auto server = kj::heap<TailCallForwardingServer>(targetIt->second);
    auto forwarderId = addTarget(capnp::Capability::Client(kj::mv(server)));

    // Keep any underlying peer ownership alive while the forwarder exists.
    retainPeerOwnership(target, forwarderId, loopbackPeerOwnerByTarget_, loopbackPeerOwnerRefCount_);
    retainPeerOwnership(target, forwarderId, networkPeerOwnerByTarget_, networkPeerOwnerRefCount_);
    return forwarderId;
  }

  uint32_t registerFdTarget(uint32_t fd) {
#if defined(_WIN32)
    (void)fd;
    throw std::runtime_error("registerFdTarget is not supported on Windows");
#else
    constexpr uint32_t maxInt = static_cast<uint32_t>(std::numeric_limits<int>::max());
    if (fd > maxInt) {
      throw std::runtime_error("fd exceeds platform int range");
    }
    int fdCopy = dup(static_cast<int>(fd));
    if (fdCopy < 0) {
      throw std::runtime_error("dup() failed while registering fd target");
    }
    auto server = kj::heap<FdCapabilityServer>(fdCopy);
    return addTarget(capnp::Capability::Client(kj::mv(server)));
#endif
  }

  template <typename RequestBuilder>
  void setRequestPayload(RequestBuilder& requestBuilder, const std::vector<uint8_t>& request,
                         const std::vector<uint32_t>& requestCaps) {
    if (request.empty()) {
      if (!requestCaps.empty()) {
        throw std::runtime_error("RPC request capability table requires a non-empty payload");
      }
      return;
    }

    kj::ArrayPtr<const kj::byte> reqBytes(reinterpret_cast<const kj::byte*>(request.data()),
                                          request.size());
    kj::ArrayInputStream input(reqBytes);
    capnp::ReaderOptions options;
    options.traversalLimitInWords = 1ull << 30;
    capnp::InputStreamMessageReader reader(input, options);
    auto requestRoot = reader.getRoot<capnp::AnyPointer>();
    if (requestCaps.empty()) {
      requestBuilder.template setAs<capnp::AnyPointer>(requestRoot);
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
          throw std::runtime_error("unknown RPC request capability id: " + std::to_string(capId));
        }
        capnp::Capability::Client cap = capIt->second;
        capTableBuilder.add(capnp::ClientHook::from(kj::mv(cap)));
      }
      capnp::ReaderCapabilityTable requestCapTable(capTableBuilder.finish());
      requestBuilder.template setAs<capnp::AnyPointer>(requestCapTable.imbue(requestRoot));
    }
  }

  RawCallResult serializeResponse(capnp::Response<capnp::AnyPointer>& response) {
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

  uint32_t startPendingCall(uint32_t target, uint64_t interfaceId, uint16_t methodId,
                            const std::vector<uint8_t>& request,
                            const std::vector<uint32_t>& requestCaps) {
    auto targetIt = targets_.find(target);
    if (targetIt == targets_.end()) {
      throw std::runtime_error("unknown RPC target capability id: " + std::to_string(target));
    }

    auto requestBuilder = targetIt->second.typelessRequest(interfaceId, methodId, kj::none, {});
    setRequestPayload(requestBuilder, request, requestCaps);
    auto promiseAndPipeline = requestBuilder.send();
    auto canceler = kj::heap<kj::Canceler>();
    auto pipeline = promiseAndPipeline.noop();
    auto promise = canceler->wrap(kj::mv(promiseAndPipeline).dropPipeline());
    return addPendingCall(PendingCall(kj::mv(promise), kj::mv(pipeline), kj::mv(canceler)));
  }

  kj::Promise<RawCallResult> awaitPendingCall(uint32_t pendingCallId) {
    auto pendingIt = pendingCalls_.find(pendingCallId);
    if (pendingIt == pendingCalls_.end()) {
      throw std::runtime_error("unknown pending RPC call id: " + std::to_string(pendingCallId));
    }
    auto pending = kj::mv(pendingIt->second);
    pendingCalls_.erase(pendingIt);
    pending.canceler->release();
    return kj::mv(pending.promise).then([this](capnp::Response<capnp::AnyPointer>&& response) {
      return kj::evalNow([this, &response]() { return serializeResponse(response); });
    });
  }

  void releasePendingCall(uint32_t pendingCallId) {
    auto pendingIt = pendingCalls_.find(pendingCallId);
    if (pendingIt == pendingCalls_.end()) {
      throw std::runtime_error("unknown pending RPC call id: " + std::to_string(pendingCallId));
    }
    cancelOneActiveDeferredTask();
    pendingIt->second.canceler->cancel("Capnp.Rpc pending call released from Lean");
    pendingCalls_.erase(pendingIt);
  }

  void cancelOneActiveDeferredTask() {
    while (!activeCancelableDeferredTasks_.empty()) {
      auto taskState = activeCancelableDeferredTasks_.front().lock();
      activeCancelableDeferredTasks_.pop_front();
      if (taskState && taskState->requestCancellation()) {
        return;
      }
    }
    ++pendingDeferredCancelRequests_;
  }

  uint32_t getPipelinedCap(uint32_t pendingCallId, const std::vector<uint16_t>& pointerPath) {
    auto pendingIt = pendingCalls_.find(pendingCallId);
    if (pendingIt == pendingCalls_.end()) {
      throw std::runtime_error("unknown pending RPC call id: " + std::to_string(pendingCallId));
    }

    auto pipeline = pendingIt->second.pipeline.noop();
    for (auto pointerIndex : pointerPath) {
      pipeline = pipeline.getPointerField(pointerIndex);
    }
    auto cap = capnp::Capability::Client(pipeline.asCap());
    return addTarget(kj::mv(cap));
  }

  void processStreamingCall(uint32_t target, uint64_t interfaceId, uint16_t methodId,
                            const std::vector<uint8_t>& request,
                            const std::vector<uint32_t>& requestCaps,
                            kj::WaitScope& waitScope) {
    auto targetIt = targets_.find(target);
    if (targetIt == targets_.end()) {
      throw std::runtime_error("unknown RPC target capability id: " + std::to_string(target));
    }

    struct StreamingClientAccess final : public capnp::Capability::Client {
      explicit StreamingClientAccess(capnp::Capability::Client& client)
          : capnp::Capability::Client(client) {}
      using capnp::Capability::Client::newStreamingCall;
    };

    StreamingClientAccess client(targetIt->second);
    auto requestBuilder =
        client.newStreamingCall<capnp::AnyPointer>(interfaceId, methodId, kj::none, {});
    setRequestPayload(requestBuilder, request, requestCaps);
    requestBuilder.send().wait(waitScope);
  }

  int64_t targetGetFd(kj::WaitScope& waitScope, uint32_t target) {
    auto targetIt = targets_.find(target);
    if (targetIt == targets_.end()) {
      throw std::runtime_error("unknown RPC target capability id: " + std::to_string(target));
    }
    auto fdMaybe = targetIt->second.getFd().wait(waitScope);
    KJ_IF_SOME(fd, fdMaybe) {
      return static_cast<int64_t>(fd);
    }
    return -1;
  }

  void targetWhenResolved(kj::WaitScope& waitScope, uint32_t target) {
    auto targetIt = targets_.find(target);
    if (targetIt == targets_.end()) {
      throw std::runtime_error("unknown RPC target capability id: " + std::to_string(target));
    }
    targetIt->second.whenResolved().wait(waitScope);
  }

  uint32_t targetWhenResolvedStart(uint32_t target) {
    auto targetIt = targets_.find(target);
    if (targetIt == targets_.end()) {
      throw std::runtime_error("unknown RPC target capability id: " + std::to_string(target));
    }
    auto canceler = kj::heap<kj::Canceler>();
    auto promise = canceler->wrap(targetIt->second.whenResolved());
    return addUnitPromise(PendingUnitPromise(kj::mv(promise), kj::mv(canceler)));
  }

  kj::String encodeTraceWithLean(const kj::Exception& e) {
    if (traceEncoderHandler_ == nullptr) {
      return kj::String();
    }
    lean_inc(traceEncoderHandler_);
    auto ioResult = lean_apply_2(traceEncoderHandler_, lean_mk_string(e.getDescription().cStr()),
                                 lean_box(0));
    if (lean_io_result_is_error(ioResult)) {
      lean_dec(ioResult);
      return kj::String();
    }
    auto traceObj = lean_io_result_take_value(ioResult);
    auto traceCStr = lean_string_cstr(traceObj);
    lean_dec(traceObj);
    return kj::str(traceCStr);
  }

  kj::String encodeTrace(const kj::Exception& e) {
    if (traceEncoderHandler_ != nullptr) {
      return encodeTraceWithLean(e);
    }
    if (traceEncoderEnabled_) {
      return kj::str("lean4-rpc-trace: ", e.getDescription());
    }
    return kj::String();
  }

  bool traceEncoderActive() const { return traceEncoderEnabled_ || traceEncoderHandler_ != nullptr; }

  kj::Maybe<kj::Function<kj::String(const kj::Exception&)>> makeTraceEncoderMaybe() {
    if (!traceEncoderActive()) {
      return kj::none;
    }
    return kj::Function<kj::String(const kj::Exception&)>(
        [this](const kj::Exception& e) { return encodeTrace(e); });
  }

  void applyTraceEncoder(TwoPartyRpcSystem& rpcSystem) {
    if (!traceEncoderActive()) {
      rpcSystem.setTraceEncoder([](const kj::Exception&) { return kj::String(); });
      return;
    }
    rpcSystem.setTraceEncoder([this](const kj::Exception& e) { return encodeTrace(e); });
  }

  void applyTraceEncoderToAllActiveConnections() {
    for (auto& entry : clients_) {
      applyTraceEncoder(*entry.second->rpcSystem);
    }
    for (auto& entry : networkClientPeers_) {
      applyTraceEncoder(*entry.second->rpcSystem);
    }
    for (auto& loopback : loopbackPeers_) {
      applyTraceEncoder(*loopback.second->serverRpcSystem);
    }
    for (auto& peer : networkServerPeers_) {
      applyTraceEncoder(*peer->rpcSystem);
    }
    for (auto& server : servers_) {
      for (auto& peer : server.second->peers) {
        applyTraceEncoder(*peer->rpcSystem);
      }
    }
  }

  void clearTraceEncoderHandler() {
    if (traceEncoderHandler_ != nullptr) {
      lean_dec(traceEncoderHandler_);
      traceEncoderHandler_ = nullptr;
    }
  }

  void setTraceEncoderEnabled(bool enabled) {
    clearTraceEncoderHandler();
    traceEncoderEnabled_ = enabled;
    applyTraceEncoderToAllActiveConnections();
  }

  void setTraceEncoderFromLean(lean_object* encoder) {
    clearTraceEncoderHandler();
    traceEncoderEnabled_ = false;
    traceEncoderHandler_ = encoder;
    applyTraceEncoderToAllActiveConnections();
  }

  kj::Promise<RawCallResult> processRawCall(uint32_t target, uint64_t interfaceId,
                                            uint16_t methodId,
                                            const std::vector<uint8_t>& request,
                                            const std::vector<uint32_t>& requestCaps) {
    auto targetIt = targets_.find(target);
    if (targetIt == targets_.end()) {
      throw std::runtime_error("unknown RPC target capability id: " + std::to_string(target));
    }

    debugLog(
        "rawcall.start",
        "target=" + std::to_string(target) + " interfaceId=" + std::to_string(interfaceId) +
            " methodId=" + std::to_string(methodId));

    auto requestBuilder = targetIt->second.typelessRequest(interfaceId, methodId, kj::none, {});
    setRequestPayload(requestBuilder, request, requestCaps);
    return requestBuilder.send().then([this, target](capnp::Response<capnp::AnyPointer>&& response) {
      debugLog("rawcall.done", "target=" + std::to_string(target));
      return kj::evalNow([this, &response]() { return serializeResponse(response); });
    });
  }

  RawCallResult processCppCallWithAccept(
      kj::AsyncIoProvider& ioProvider, kj::WaitScope& waitScope, uint32_t serverId,
      uint32_t listenerId, const std::string& address, uint32_t portHint,
      uint64_t interfaceId, uint16_t methodId, const std::vector<uint8_t>& request,
      const std::vector<uint32_t>& requestCaps) {
    auto serverIt = servers_.find(serverId);
    if (serverIt == servers_.end()) {
      throw std::runtime_error("unknown RPC server id: " + std::to_string(serverId));
    }
    auto listenerIt = listeners_.find(listenerId);
    if (listenerIt == listeners_.end()) {
      throw std::runtime_error("unknown RPC listener id: " + std::to_string(listenerId));
    }

    auto addr = ioProvider.getNetwork().parseAddress(address.c_str(), portHint).wait(waitScope);
    auto connectPromise = addr->connect();
    auto acceptPromise = listenerIt->second->accept();
    auto stream = connectPromise.wait(waitScope).downcast<kj::AsyncCapabilityStream>();
    auto connection = acceptPromise.wait(waitScope).downcast<kj::AsyncCapabilityStream>();

    // Accept the incoming client connection and keep the server peer alive in runtime-owned state.
    networkServerPeers_.add(makeRuntimeServerPeerWithFds(*serverIt->second, kj::mv(connection)));
    std::vector<uint8_t> responseCopy;
    std::vector<uint8_t> responseCaps;
    {
      auto network = kj::heap<capnp::TwoPartyVatNetwork>(
          *stream, maxFdsPerMessage_, capnp::rpc::twoparty::Side::CLIENT);
      auto rpcSystem = kj::heap<TwoPartyRpcSystem>(capnp::makeRpcClient(*network));
      applyTraceEncoder(*rpcSystem);

      capnp::word scratch[4];
      memset(&scratch, 0, sizeof(scratch));
      capnp::MallocMessageBuilder message(scratch);
      auto vatId = message.getRoot<capnp::rpc::twoparty::VatId>();
      vatId.setSide(network->getSide() == capnp::rpc::twoparty::Side::CLIENT
                        ? capnp::rpc::twoparty::Side::SERVER
                        : capnp::rpc::twoparty::Side::CLIENT);
      auto target = rpcSystem->bootstrap(vatId);

      auto requestBuilder = target.typelessRequest(interfaceId, methodId, kj::none, {});
      setRequestPayload(requestBuilder, request, requestCaps);

      auto response = requestBuilder.send().wait(waitScope);
      capnp::MallocMessageBuilder responseMessage;
      capnp::BuilderCapabilityTable responseCapTable;
      responseCapTable
          .imbue(responseMessage.getRoot<capnp::AnyPointer>())
          .setAs<capnp::AnyPointer>(response.getAs<capnp::AnyPointer>());

      auto responseWords = capnp::messageToFlatArray(responseMessage);
      auto responseBytes = responseWords.asBytes();
      responseCopy.assign(responseBytes.begin(), responseBytes.end());

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
    }
    return RawCallResult{std::move(responseCopy), std::move(responseCaps)};
  }

  RawCallResult processCppPipelinedCallWithAccept(
      kj::AsyncIoProvider& ioProvider, kj::WaitScope& waitScope, uint32_t serverId,
      uint32_t listenerId, const std::string& address, uint32_t portHint,
      uint64_t interfaceId, uint16_t methodId, const std::vector<uint8_t>& request,
      const std::vector<uint32_t>& requestCaps, const std::vector<uint8_t>& pipelinedRequest,
      const std::vector<uint32_t>& pipelinedRequestCaps) {
    auto serverIt = servers_.find(serverId);
    if (serverIt == servers_.end()) {
      throw std::runtime_error("unknown RPC server id: " + std::to_string(serverId));
    }
    auto listenerIt = listeners_.find(listenerId);
    if (listenerIt == listeners_.end()) {
      throw std::runtime_error("unknown RPC listener id: " + std::to_string(listenerId));
    }

    auto addr = ioProvider.getNetwork().parseAddress(address.c_str(), portHint).wait(waitScope);
    auto connectPromise = addr->connect();
    auto acceptPromise = listenerIt->second->accept();
    auto stream = connectPromise.wait(waitScope).downcast<kj::AsyncCapabilityStream>();
    auto connection = acceptPromise.wait(waitScope).downcast<kj::AsyncCapabilityStream>();

    // Accept the incoming client connection and keep the server peer alive in runtime-owned state.
    networkServerPeers_.add(makeRuntimeServerPeerWithFds(*serverIt->second, kj::mv(connection)));

    auto network = kj::heap<capnp::TwoPartyVatNetwork>(
        *stream, maxFdsPerMessage_, capnp::rpc::twoparty::Side::CLIENT);
    auto rpcSystem = kj::heap<TwoPartyRpcSystem>(capnp::makeRpcClient(*network));
    applyTraceEncoder(*rpcSystem);

    capnp::word scratch[4];
    memset(&scratch, 0, sizeof(scratch));
    capnp::MallocMessageBuilder message(scratch);
    auto vatId = message.getRoot<capnp::rpc::twoparty::VatId>();
    vatId.setSide(network->getSide() == capnp::rpc::twoparty::Side::CLIENT
                      ? capnp::rpc::twoparty::Side::SERVER
                      : capnp::rpc::twoparty::Side::CLIENT);
    auto target = rpcSystem->bootstrap(vatId);

    auto firstRequest = target.typelessRequest(interfaceId, methodId, kj::none, {});
    setRequestPayload(firstRequest, request, requestCaps);
    auto firstPromise = firstRequest.send();

    auto pipelinedTarget = capnp::Capability::Client(firstPromise.noop().asCap());
    auto pipelinedRequestBuilder =
        pipelinedTarget.typelessRequest(interfaceId, methodId, kj::none, {});
    setRequestPayload(pipelinedRequestBuilder, pipelinedRequest, pipelinedRequestCaps);
    auto pipelinedResponse = pipelinedRequestBuilder.send().wait(waitScope);
    return serializeResponse(pipelinedResponse);
  }

  bool dropTargetIfPresent(uint32_t target) {
    auto erased = targets_.erase(target);
    releasePeerOwnership(target, loopbackPeers_, loopbackPeerOwnerByTarget_,
                         loopbackPeerOwnerRefCount_);
    releasePeerOwnership(target, networkClientPeers_, networkPeerOwnerByTarget_,
                         networkPeerOwnerRefCount_);
    return erased > 0;
  }

  uint32_t retainTarget(uint32_t target) {
    auto targetIt = targets_.find(target);
    if (targetIt == targets_.end()) {
      throw std::runtime_error("unknown RPC target capability id: " + std::to_string(target));
    }
    auto retainedTarget = addTarget(targetIt->second);
    retainPeerOwnership(target, retainedTarget, loopbackPeerOwnerByTarget_,
                        loopbackPeerOwnerRefCount_);
    retainPeerOwnership(target, retainedTarget, networkPeerOwnerByTarget_,
                        networkPeerOwnerRefCount_);
    return retainedTarget;
  }

  void releaseTarget(uint32_t target) {
    if (!dropTargetIfPresent(target)) {
      throw std::runtime_error("unknown RPC target capability id: " + std::to_string(target));
    }
  }

  void releaseTargets(const std::vector<uint32_t>& targets) {
    for (auto target : targets) {
      if (target == 0) {
        continue;
      }
      if (!dropTargetIfPresent(target)) {
        throw std::runtime_error("unknown RPC target capability id: " + std::to_string(target));
      }
    }
  }

  std::pair<uint32_t, uint32_t> newPromiseCapability() {
    auto paf = kj::newPromiseAndFulfiller<kj::Own<capnp::ClientHook>>();
    auto promiseHook = capnp::newLocalPromiseClient(kj::mv(paf.promise));
    auto promiseTarget = capnp::Capability::Client(kj::mv(promiseHook));
    auto targetId = addTarget(kj::mv(promiseTarget));
    auto fulfillerId = addPromiseCapabilityFulfiller(
        PendingPromiseCapability(kj::mv(paf.fulfiller), targetId));
    return {targetId, fulfillerId};
  }

  static kj::Exception::Type decodeRemoteExceptionType(uint8_t typeTag) {
    switch (typeTag) {
      case 0:
        return kj::Exception::Type::FAILED;
      case 1:
        return kj::Exception::Type::OVERLOADED;
      case 2:
        return kj::Exception::Type::DISCONNECTED;
      case 3:
        return kj::Exception::Type::UNIMPLEMENTED;
      default:
        return kj::Exception::Type::FAILED;
    }
  }

  void promiseCapabilityFulfill(uint32_t fulfillerId, uint32_t target) {
    auto promiseIt = promiseCapabilityFulfillers_.find(fulfillerId);
    if (promiseIt == promiseCapabilityFulfillers_.end()) {
      throw std::runtime_error("unknown RPC promise capability fulfiller id: " +
                               std::to_string(fulfillerId));
    }
    if (target == 0) {
      throw std::runtime_error("cannot fulfill promise capability with null target");
    }
    if (target == promiseIt->second.promiseTarget) {
      throw std::runtime_error("cannot fulfill promise capability with itself");
    }

    auto targetIt = targets_.find(target);
    if (targetIt == targets_.end()) {
      throw std::runtime_error("unknown RPC target capability id: " + std::to_string(target));
    }
    capnp::Capability::Client cap = targetIt->second;
    promiseIt->second.fulfiller->fulfill(capnp::ClientHook::from(kj::mv(cap)));
    promiseCapabilityFulfillers_.erase(promiseIt);
  }

  void promiseCapabilityReject(uint32_t fulfillerId, uint8_t exceptionTypeTag,
                               const std::string& message,
                               const std::vector<uint8_t>& detailBytes) {
    auto promiseIt = promiseCapabilityFulfillers_.find(fulfillerId);
    if (promiseIt == promiseCapabilityFulfillers_.end()) {
      throw std::runtime_error("unknown RPC promise capability fulfiller id: " +
                               std::to_string(fulfillerId));
    }
    auto type = decodeRemoteExceptionType(exceptionTypeTag);
    auto ex = kj::Exception(type, __FILE__, __LINE__, kj::str(message.c_str()));
    if (!detailBytes.empty()) {
      auto copy = kj::heapArray<kj::byte>(detailBytes.size());
      std::memcpy(copy.begin(), detailBytes.data(), detailBytes.size());
      ex.setDetail(1, kj::mv(copy));
    }
    promiseIt->second.fulfiller->reject(kj::mv(ex));
    promiseCapabilityFulfillers_.erase(promiseIt);
  }

  void promiseCapabilityRelease(uint32_t fulfillerId) {
    auto promiseIt = promiseCapabilityFulfillers_.find(fulfillerId);
    if (promiseIt == promiseCapabilityFulfillers_.end()) {
      return;
    }
    auto ex = kj::Exception(kj::Exception::Type::DISCONNECTED, __FILE__, __LINE__,
                            kj::str("promise capability fulfiller released"));
    promiseIt->second.fulfiller->reject(kj::mv(ex));
    promiseCapabilityFulfillers_.erase(promiseIt);
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

  uint32_t addTransport(kj::Own<kj::AsyncCapabilityStream>&& stream) {
    uint32_t transportId = nextTransportId_++;
    while (transports_.find(transportId) != transports_.end()) {
      transportId = nextTransportId_++;
    }
    transports_.emplace(transportId, kj::mv(stream));
    return transportId;
  }

  kj::Own<kj::AsyncCapabilityStream> takeTransport(uint32_t transportId) {
    auto transportIt = transports_.find(transportId);
    if (transportIt == transports_.end()) {
      throw std::runtime_error("unknown RPC transport id: " + std::to_string(transportId));
    }
    auto stream = kj::mv(transportIt->second);
    transports_.erase(transportIt);
    return stream;
  }

  std::pair<uint32_t, uint32_t> newTransportPipe(kj::AsyncIoProvider& ioProvider) {
    auto pipe = ioProvider.newCapabilityPipe();
    auto firstId = addTransport(kj::mv(pipe.ends[0]));
    auto secondId = addTransport(kj::mv(pipe.ends[1]));
    return {firstId, secondId};
  }

  uint32_t newTransportFromFd(kj::LowLevelAsyncIoProvider& lowLevelProvider, uint32_t fd) {
#if defined(_WIN32)
    (void)lowLevelProvider;
    (void)fd;
    throw std::runtime_error("newTransportFromFd is not supported on Windows");
#else
    constexpr uint32_t maxInt = static_cast<uint32_t>(std::numeric_limits<int>::max());
    if (fd > maxInt) {
      throw std::runtime_error("fd exceeds platform int range");
    }
    int fdCopy = dup(static_cast<int>(fd));
    if (fdCopy < 0) {
      throw std::runtime_error("dup() failed while creating RPC transport from fd");
    }
    auto stream = lowLevelProvider.wrapUnixSocketFd(kj::LowLevelAsyncIoProvider::OwnFd{fdCopy});
    return addTransport(kj::mv(stream));
#endif
  }

  uint32_t newTransportFromFdTake(kj::LowLevelAsyncIoProvider& lowLevelProvider, uint32_t fd) {
#if defined(_WIN32)
    (void)lowLevelProvider;
    (void)fd;
    throw std::runtime_error("newTransportFromFdTake is not supported on Windows");
#else
    constexpr uint32_t maxInt = static_cast<uint32_t>(std::numeric_limits<int>::max());
    if (fd > maxInt) {
      throw std::runtime_error("fd exceeds platform int range");
    }
    auto stream = lowLevelProvider.wrapUnixSocketFd(
        kj::LowLevelAsyncIoProvider::OwnFd{static_cast<int>(fd)});
    return addTransport(kj::mv(stream));
#endif
  }

  void releaseTransport(uint32_t transportId) {
    auto erased = transports_.erase(transportId);
    if (erased == 0) {
      throw std::runtime_error("unknown RPC transport id: " + std::to_string(transportId));
    }
  }

  int64_t transportGetFd(kj::WaitScope& waitScope, uint32_t transportId) {
    (void)waitScope;
    auto transportIt = transports_.find(transportId);
    if (transportIt == transports_.end()) {
      throw std::runtime_error("unknown RPC transport id: " + std::to_string(transportId));
    }
    auto fdMaybe = transportIt->second->getFd();
    if (fdMaybe == kj::none) {
      return -1;
    }
    return static_cast<int64_t>(KJ_ASSERT_NONNULL(fdMaybe));
  }

  uint32_t connectTargetPeer(kj::Own<NetworkClientPeer>&& peer) {
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
    networkPeerOwnerByTarget_.emplace(targetId, targetId);
    networkPeerOwnerRefCount_.emplace(targetId, 1);
    return targetId;
  }

  kj::Own<NetworkClientPeer> connectPeer(kj::AsyncIoProvider& ioProvider, kj::WaitScope& waitScope,
                                         const std::string& address, uint32_t portHint) {
    auto addr = ioProvider.getNetwork().parseAddress(address.c_str(), portHint).wait(waitScope);
    auto stream = addr->connect().wait(waitScope).downcast<kj::AsyncCapabilityStream>();
    auto network = kj::heap<capnp::TwoPartyVatNetwork>(
        *stream, maxFdsPerMessage_, capnp::rpc::twoparty::Side::CLIENT);
    auto rpcSystem = kj::heap<TwoPartyRpcSystem>(capnp::makeRpcClient(*network));
    applyTraceEncoder(*rpcSystem);

    auto peer = kj::heap<NetworkClientPeer>();
    peer->stream = kj::mv(stream);
    peer->network = kj::mv(network);
    peer->rpcSystem = kj::mv(rpcSystem);
    return peer;
  }

  kj::Promise<kj::Own<NetworkClientPeer>> connectPeerStart(kj::AsyncIoProvider& ioProvider,
                                                            std::string address,
                                                            uint32_t portHint) {
    return ioProvider.getNetwork()
        .parseAddress(address.c_str(), portHint)
        .then([](kj::Own<kj::NetworkAddress>&& addr) { return addr->connect(); })
        .then([this](kj::Own<kj::AsyncIoStream>&& stream) {
          auto capStream = stream.downcast<kj::AsyncCapabilityStream>();
          auto network = kj::heap<capnp::TwoPartyVatNetwork>(
              *capStream, maxFdsPerMessage_, capnp::rpc::twoparty::Side::CLIENT);
          auto rpcSystem = kj::heap<TwoPartyRpcSystem>(capnp::makeRpcClient(*network));
          applyTraceEncoder(*rpcSystem);

          auto peer = kj::heap<NetworkClientPeer>();
          peer->stream = kj::mv(capStream);
          peer->network = kj::mv(network);
          peer->rpcSystem = kj::mv(rpcSystem);
          return peer;
        });
  }

  uint32_t connectTarget(kj::AsyncIoProvider& ioProvider, kj::WaitScope& waitScope,
                         const std::string& address, uint32_t portHint) {
    auto peer = connectPeer(ioProvider, waitScope, address, portHint);
    return connectTargetPeer(kj::mv(peer));
  }

  uint32_t connectTargetStart(kj::AsyncIoProvider& ioProvider, std::string address,
                              uint32_t portHint) {
    auto canceler = kj::heap<kj::Canceler>();
    auto promise = canceler->wrap(
        connectPeerStart(ioProvider, std::move(address), portHint)
            .then([this](kj::Own<NetworkClientPeer>&& peer) {
              return connectTargetPeer(kj::mv(peer));
            }));
    return addRegisterPromise(PendingRegisterPromise(kj::mv(promise), kj::mv(canceler)));
  }

  uint32_t connectTargetFd(kj::LowLevelAsyncIoProvider& lowLevelProvider, uint32_t fd) {
#if defined(_WIN32)
    (void)lowLevelProvider;
    (void)fd;
    throw std::runtime_error("connectFd is not supported on Windows");
#else
    constexpr uint32_t maxInt = static_cast<uint32_t>(std::numeric_limits<int>::max());
    if (fd > maxInt) {
      throw std::runtime_error("fd exceeds platform int range");
    }
    int fdCopy = dup(static_cast<int>(fd));
    if (fdCopy < 0) {
      throw std::runtime_error("dup() failed while connecting RPC target fd");
    }
    auto stream = lowLevelProvider.wrapUnixSocketFd(
        kj::LowLevelAsyncIoProvider::OwnFd{fdCopy});
    auto network = kj::heap<capnp::TwoPartyVatNetwork>(
        *stream, maxFdsPerMessage_, capnp::rpc::twoparty::Side::CLIENT);
    auto rpcSystem = kj::heap<TwoPartyRpcSystem>(capnp::makeRpcClient(*network));
    applyTraceEncoder(*rpcSystem);

    auto peer = kj::heap<NetworkClientPeer>();
    peer->stream = kj::mv(stream);
    peer->network = kj::mv(network);
    peer->rpcSystem = kj::mv(rpcSystem);
    return connectTargetPeer(kj::mv(peer));
#endif
  }

  uint32_t connectTargetTransport(uint32_t transportId) {
    auto stream = takeTransport(transportId);
    auto network = kj::heap<capnp::TwoPartyVatNetwork>(
        *stream, maxFdsPerMessage_, capnp::rpc::twoparty::Side::CLIENT);
    auto rpcSystem = kj::heap<TwoPartyRpcSystem>(capnp::makeRpcClient(*network));
    applyTraceEncoder(*rpcSystem);

    auto peer = kj::heap<NetworkClientPeer>();
    peer->stream = kj::mv(stream);
    peer->network = kj::mv(network);
    peer->rpcSystem = kj::mv(rpcSystem);
    return connectTargetPeer(kj::mv(peer));
  }

  uint32_t newClient(kj::AsyncIoProvider& ioProvider, kj::WaitScope& waitScope,
                     const std::string& address, uint32_t portHint) {
    auto peer = connectPeer(ioProvider, waitScope, address, portHint);
    return addClient(kj::mv(peer));
  }

  uint32_t newClientStart(kj::AsyncIoProvider& ioProvider, std::string address,
                          uint32_t portHint) {
    auto canceler = kj::heap<kj::Canceler>();
    auto promise = canceler->wrap(
        connectPeerStart(ioProvider, std::move(address), portHint)
            .then([this](kj::Own<NetworkClientPeer>&& peer) { return addClient(kj::mv(peer)); }));
    return addRegisterPromise(PendingRegisterPromise(kj::mv(promise), kj::mv(canceler)));
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

  uint32_t clientOnDisconnectStart(uint32_t clientId) {
    auto clientIt = clients_.find(clientId);
    if (clientIt == clients_.end()) {
      throw std::runtime_error("unknown RPC client id: " + std::to_string(clientId));
    }
    auto canceler = kj::heap<kj::Canceler>();
    auto promise = canceler->wrap(clientIt->second->network->onDisconnect());
    return addUnitPromise(PendingUnitPromise(kj::mv(promise), kj::mv(canceler)));
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

  uint64_t targetCount() const {
    return static_cast<uint64_t>(targets_.size());
  }

  uint64_t listenerCount() const {
    return static_cast<uint64_t>(listeners_.size());
  }

  uint64_t clientCount() const {
    return static_cast<uint64_t>(clients_.size());
  }

  uint64_t serverCount() const {
    return static_cast<uint64_t>(servers_.size());
  }

  uint64_t pendingCallCount() const {
    return static_cast<uint64_t>(pendingCalls_.size());
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

  kj::Own<NetworkServerPeer> makeServerPeer(capnp::Capability::Client bootstrap,
                                            kj::Own<kj::AsyncIoStream>&& connection) {
    auto peer = kj::heap<NetworkServerPeer>();
    peer->ioConnection = kj::mv(connection);
    auto& ioConnection = KJ_ASSERT_NONNULL(peer->ioConnection);
    peer->network = kj::heap<capnp::TwoPartyVatNetwork>(
        *ioConnection, capnp::rpc::twoparty::Side::SERVER);
    peer->rpcSystem = kj::heap<TwoPartyRpcSystem>(
        capnp::makeRpcServer(*peer->network, kj::mv(bootstrap)));
    applyTraceEncoder(*peer->rpcSystem);
    return peer;
  }

  kj::Own<NetworkServerPeer> makeServerPeer(
      capnp::BootstrapFactory<capnp::rpc::twoparty::VatId>& bootstrapFactory,
      kj::Own<kj::AsyncIoStream>&& connection) {
    auto peer = kj::heap<NetworkServerPeer>();
    peer->ioConnection = kj::mv(connection);
    auto& ioConnection = KJ_ASSERT_NONNULL(peer->ioConnection);
    peer->network = kj::heap<capnp::TwoPartyVatNetwork>(
        *ioConnection, capnp::rpc::twoparty::Side::SERVER);
    peer->rpcSystem =
        kj::heap<TwoPartyRpcSystem>(capnp::makeRpcServer(*peer->network, bootstrapFactory));
    applyTraceEncoder(*peer->rpcSystem);
    return peer;
  }

  kj::Own<NetworkServerPeer> makeServerPeerWithFds(capnp::Capability::Client bootstrap,
                                                    kj::Own<kj::AsyncCapabilityStream>&& connection) {
    auto peer = kj::heap<NetworkServerPeer>();
    peer->capConnection = kj::mv(connection);
    auto& capConnection = KJ_ASSERT_NONNULL(peer->capConnection);
    peer->network = kj::heap<capnp::TwoPartyVatNetwork>(
        *capConnection, maxFdsPerMessage_, capnp::rpc::twoparty::Side::SERVER);
    peer->rpcSystem = kj::heap<TwoPartyRpcSystem>(
        capnp::makeRpcServer(*peer->network, kj::mv(bootstrap)));
    applyTraceEncoder(*peer->rpcSystem);
    return peer;
  }

  kj::Own<NetworkServerPeer> makeServerPeerWithFds(
      capnp::BootstrapFactory<capnp::rpc::twoparty::VatId>& bootstrapFactory,
      kj::Own<kj::AsyncCapabilityStream>&& connection) {
    auto peer = kj::heap<NetworkServerPeer>();
    peer->capConnection = kj::mv(connection);
    auto& capConnection = KJ_ASSERT_NONNULL(peer->capConnection);
    peer->network = kj::heap<capnp::TwoPartyVatNetwork>(
        *capConnection, maxFdsPerMessage_, capnp::rpc::twoparty::Side::SERVER);
    peer->rpcSystem =
        kj::heap<TwoPartyRpcSystem>(capnp::makeRpcServer(*peer->network, bootstrapFactory));
    applyTraceEncoder(*peer->rpcSystem);
    return peer;
  }

  kj::Own<NetworkServerPeer> makeRuntimeServerPeerWithFds(
      RuntimeServer& server, kj::Own<kj::AsyncCapabilityStream>&& connection) {
    KJ_IF_SOME(bootstrapFactory, server.bootstrapFactory) {
      return makeServerPeerWithFds(*bootstrapFactory, kj::mv(connection));
    }
    auto bootstrap = KJ_ASSERT_NONNULL(server.bootstrap);
    return makeServerPeerWithFds(bootstrap, kj::mv(connection));
  }

  uint32_t listenLoopback(kj::AsyncIoProvider& ioProvider, kj::WaitScope& waitScope,
                      const std::string& address, uint32_t portHint) {
    auto addr = ioProvider.getNetwork().parseAddress(address.c_str(), portHint).wait(waitScope);
    auto listener = addr->listen();
    return addListener(kj::mv(listener));
  }

  void acceptLoopback(kj::WaitScope& waitScope, uint32_t listenerId) {
    auto listenerIt = listeners_.find(listenerId);
    if (listenerIt == listeners_.end()) {
      throw std::runtime_error("unknown RPC listener id: " + std::to_string(listenerId));
    }
    auto connection = listenerIt->second->accept().wait(waitScope);
    networkServerPeers_.add(makeServerPeer(
        capnp::Capability::Client(kj::heap<LoopbackCapabilityServer>()), kj::mv(connection)));
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

  uint32_t newServerWithBootstrapFactory(lean_object* bootstrapFactory) {
    auto factory = kj::heap<LeanBootstrapFactory>(*this, bootstrapFactory);
    auto server = kj::heap<RuntimeServer>(kj::mv(factory));
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

    auto connection = listenerIt->second->accept().wait(waitScope).downcast<kj::AsyncCapabilityStream>();
    auto peer = makeRuntimeServerPeerWithFds(*serverIt->second, kj::mv(connection));
    serverIt->second->peers.add(kj::mv(peer));
  }

  uint32_t serverAcceptStart(uint32_t serverId, uint32_t listenerId) {
    auto canceler = kj::heap<kj::Canceler>();
    kj::Promise<void> ready = kj::READY_NOW;
    auto promise = canceler->wrap(kj::mv(ready)
                                      .then([this, listenerId]() {
                                        auto listenerIt = listeners_.find(listenerId);
                                        if (listenerIt == listeners_.end()) {
                                          throw std::runtime_error("unknown RPC listener id: " +
                                                                   std::to_string(listenerId));
                                        }
                                        return listenerIt->second->accept();
                                      })
                                      .then([this, serverId](kj::Own<kj::AsyncIoStream>&& conn) {
                                        auto serverIt = servers_.find(serverId);
                                        if (serverIt == servers_.end()) {
                                          throw std::runtime_error("unknown RPC server id: " +
                                                                   std::to_string(serverId));
                                        }
                                        auto connection =
                                            conn.downcast<kj::AsyncCapabilityStream>();
                                        auto peer = makeRuntimeServerPeerWithFds(
                                            *serverIt->second, kj::mv(connection));
                                        serverIt->second->peers.add(kj::mv(peer));
                                      }));
    return addUnitPromise(PendingUnitPromise(kj::mv(promise), kj::mv(canceler)));
  }

  void serverAcceptFd(kj::LowLevelAsyncIoProvider& lowLevelProvider, uint32_t serverId,
                      uint32_t fd) {
#if defined(_WIN32)
    (void)lowLevelProvider;
    (void)serverId;
    (void)fd;
    throw std::runtime_error("serverAcceptFd is not supported on Windows");
#else
    auto serverIt = servers_.find(serverId);
    if (serverIt == servers_.end()) {
      throw std::runtime_error("unknown RPC server id: " + std::to_string(serverId));
    }

    constexpr uint32_t maxInt = static_cast<uint32_t>(std::numeric_limits<int>::max());
    if (fd > maxInt) {
      throw std::runtime_error("fd exceeds platform int range");
    }
    int fdCopy = dup(static_cast<int>(fd));
    if (fdCopy < 0) {
      throw std::runtime_error("dup() failed while accepting RPC server fd");
    }
    auto connection = lowLevelProvider.wrapUnixSocketFd(
        kj::LowLevelAsyncIoProvider::OwnFd{fdCopy});
    auto peer = makeRuntimeServerPeerWithFds(*serverIt->second, kj::mv(connection));
    serverIt->second->peers.add(kj::mv(peer));
#endif
  }

  void serverAcceptTransport(uint32_t serverId, uint32_t transportId) {
    auto serverIt = servers_.find(serverId);
    if (serverIt == servers_.end()) {
      throw std::runtime_error("unknown RPC server id: " + std::to_string(serverId));
    }
    auto connection = takeTransport(transportId);
    auto peer = makeRuntimeServerPeerWithFds(*serverIt->second, kj::mv(connection));
    serverIt->second->peers.add(kj::mv(peer));
  }

  void serverDrain(kj::WaitScope& waitScope, uint32_t serverId) {
    auto serverIt = servers_.find(serverId);
    if (serverIt == servers_.end()) {
      throw std::runtime_error("unknown RPC server id: " + std::to_string(serverId));
    }
    for (auto& peer : serverIt->second->peers) {
      peer->network->onDisconnect().wait(waitScope);
    }
  }

  uint32_t serverDrainStart(uint32_t serverId) {
    auto serverIt = servers_.find(serverId);
    if (serverIt == servers_.end()) {
      throw std::runtime_error("unknown RPC server id: " + std::to_string(serverId));
    }
    auto canceler = kj::heap<kj::Canceler>();
    auto promises = kj::heapArrayBuilder<kj::Promise<void>>(serverIt->second->peers.size());
    for (auto& peer : serverIt->second->peers) {
      promises.add(peer->network->onDisconnect());
    }
    auto promise = canceler->wrap(kj::joinPromises(promises.finish()));
    return addUnitPromise(PendingUnitPromise(kj::mv(promise), kj::mv(canceler)));
  }

  uint32_t registerLoopbackTarget(kj::AsyncIoProvider& ioProvider,
                                  capnp::Capability::Client bootstrap) {
    auto pipe = ioProvider.newCapabilityPipe();
    auto serverStream = kj::mv(pipe.ends[0]);
    auto serverNetwork = kj::heap<capnp::TwoPartyVatNetwork>(
        *serverStream, 2, capnp::rpc::twoparty::Side::SERVER);
    auto serverRpcSystem =
        kj::heap<TwoPartyRpcSystem>(capnp::makeRpcServer(*serverNetwork, kj::mv(bootstrap)));
    applyTraceEncoder(*serverRpcSystem);

    auto client = kj::heap<capnp::TwoPartyClient>(*pipe.ends[1], 2);
    auto cap = client->bootstrap();

    auto peer = kj::heap<LoopbackPeer>();
    peer->clientStream = kj::mv(pipe.ends[1]);
    peer->serverStream = kj::mv(serverStream);
    peer->serverNetwork = kj::mv(serverNetwork);
    peer->serverRpcSystem = kj::mv(serverRpcSystem);
    peer->client = kj::mv(client);

    auto targetId = addTarget(kj::mv(cap));
    loopbackPeers_.emplace(targetId, kj::mv(peer));
    loopbackPeerOwnerByTarget_.emplace(targetId, targetId);
    loopbackPeerOwnerRefCount_.emplace(targetId, 1);
    return targetId;
  }

  uint32_t registerLoopbackTarget(kj::AsyncIoProvider& ioProvider) {
    return registerLoopbackTarget(
        ioProvider, capnp::Capability::Client(kj::heap<LoopbackCapabilityServer>()));
  }

  uint32_t registerLoopbackTarget(kj::AsyncIoProvider& ioProvider, uint32_t bootstrapTarget) {
    auto targetIt = targets_.find(bootstrapTarget);
    if (targetIt == targets_.end()) {
      throw std::runtime_error("unknown RPC target capability id: " +
                               std::to_string(bootstrapTarget));
    }
    return registerLoopbackTarget(ioProvider, targetIt->second);
  }

  void run() {
    try {
      auto io = kj::setupAsyncIo();
#if _WIN32
      eventPort_.store(&io.win32EventPort, std::memory_order_release);
#else
      eventPort_.store(&io.unixEventPort, std::memory_order_release);
#endif
      {
        std::lock_guard<std::mutex> lock(startupMutex_);
        startupComplete_ = true;
      }
      startupCv_.notify_all();

      class RuntimeTaskErrorHandler final : public kj::TaskSet::ErrorHandler {
       public:
        void taskFailed(kj::Exception&& exception) override {
          debugLog("runtime.task_failed", describeKjException(exception));
        }
      };

      RuntimeTaskErrorHandler taskErrorHandler;
      kj::TaskSet tasks(taskErrorHandler);

      auto scheduleRawCallCompletion =
          [&tasks](kj::Promise<RawCallResult>&& promise,
                   const std::shared_ptr<RawCallCompletion>& completion) {
            tasks.add(kj::mv(promise).then(
                [completion](RawCallResult&& result) mutable {
                  completeSuccess(completion, kj::mv(result));
                },
                [completion](kj::Exception&& e) mutable {
                  completeFailureKj(completion, e);
                }));
          };

      while (true) {
        QueuedOperation op;
        bool haveOp = false;
        {
          std::lock_guard<std::mutex> lock(queueMutex_);
          if (!queue_.empty()) {
            op = std::move(queue_.front());
            queue_.pop_front();
            haveOp = true;
          } else if (stopping_) {
            if (tasks.isEmpty()) {
              break;
            }
          }
        }

        if (!haveOp) {
          // No queued work. Pump KJ once (non-blocking) and then *block via KJ* for a short
          // duration.
          //
          // We intentionally avoid calling EventPort::wait() directly: on some platforms we've
          // observed that wake() does not reliably interrupt a naked EventPort wait, causing the
          // runtime queue to deadlock. Waiting on a short timer promise guarantees forward
          // progress, while still letting the event loop process async I/O immediately.
          if (io.waitScope.poll(1) > 0) {
            continue;
          }
          io.provider->getTimer().afterDelay(1 * kj::MILLISECONDS).wait(io.waitScope);
          continue;
        }

        if (std::holds_alternative<QueuedRawCall>(op)) {
          auto call = std::get<QueuedRawCall>(std::move(op));
          try {
            auto promise =
                processRawCall(call.target, call.interfaceId, call.methodId, call.request,
                               call.requestCaps);
            scheduleRawCallCompletion(kj::mv(promise), call.completion);
          } catch (const kj::Exception& e) {
            completeFailureKj(call.completion, e);
          } catch (const std::exception& e) {
            completeFailure(call.completion, e.what());
          } catch (...) {
            completeFailure(call.completion, "unknown exception in capnp_lean_rpc_raw_call");
          }
        } else if (std::holds_alternative<QueuedStartPendingCall>(op)) {
          auto call = std::get<QueuedStartPendingCall>(std::move(op));
          try {
            auto pendingCallId =
                startPendingCall(call.target, call.interfaceId, call.methodId, call.request,
                                 call.requestCaps);
            completeRegisterSuccess(call.completion, pendingCallId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(call.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(call.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                call.completion,
                "unknown exception in capnp_lean_rpc_runtime_start_call_with_caps");
          }
        } else if (std::holds_alternative<QueuedAwaitPendingCall>(op)) {
          auto call = std::get<QueuedAwaitPendingCall>(std::move(op));
          try {
            auto promise = awaitPendingCall(call.pendingCallId);
            scheduleRawCallCompletion(kj::mv(promise), call.completion);
          } catch (const kj::Exception& e) {
            completeFailureKj(call.completion, e);
          } catch (const std::exception& e) {
            completeFailure(call.completion, e.what());
          } catch (...) {
            completeFailure(
                call.completion,
                "unknown exception in capnp_lean_rpc_runtime_pending_call_await");
          }
        } else if (std::holds_alternative<QueuedReleasePendingCall>(op)) {
          auto release = std::get<QueuedReleasePendingCall>(std::move(op));
          try {
            releasePendingCall(release.pendingCallId);
            completeUnitSuccess(release.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(release.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(release.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                release.completion,
                "unknown exception in capnp_lean_rpc_runtime_pending_call_release");
          }
        } else if (std::holds_alternative<QueuedGetPipelinedCap>(op)) {
          auto call = std::get<QueuedGetPipelinedCap>(std::move(op));
          try {
            auto targetId = getPipelinedCap(call.pendingCallId, call.pointerPath);
            completeRegisterSuccess(call.completion, targetId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(call.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(call.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                call.completion,
                "unknown exception in capnp_lean_rpc_runtime_pending_call_get_pipelined_cap");
          }
        } else if (std::holds_alternative<QueuedStreamingCall>(op)) {
          auto call = std::get<QueuedStreamingCall>(std::move(op));
          try {
            processStreamingCall(call.target, call.interfaceId, call.methodId, call.request,
                                 call.requestCaps, io.waitScope);
            completeUnitSuccess(call.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(call.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(call.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                call.completion,
                "unknown exception in capnp_lean_rpc_runtime_streaming_call_with_caps");
          }
        } else if (std::holds_alternative<QueuedTargetGetFd>(op)) {
          auto call = std::get<QueuedTargetGetFd>(std::move(op));
          try {
            completeInt64Success(call.completion, targetGetFd(io.waitScope, call.target));
          } catch (const kj::Exception& e) {
            completeInt64Failure(call.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeInt64Failure(call.completion, e.what());
          } catch (...) {
            completeInt64Failure(call.completion,
                                 "unknown exception in capnp_lean_rpc_runtime_target_get_fd");
          }
        } else if (std::holds_alternative<QueuedTargetWhenResolved>(op)) {
          auto call = std::get<QueuedTargetWhenResolved>(std::move(op));
          try {
            targetWhenResolved(io.waitScope, call.target);
            completeUnitSuccess(call.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(call.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(call.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                call.completion,
                "unknown exception in capnp_lean_rpc_runtime_target_when_resolved");
          }
        } else if (std::holds_alternative<QueuedTargetWhenResolvedStart>(op)) {
          auto call = std::get<QueuedTargetWhenResolvedStart>(std::move(op));
          try {
            auto promiseId = targetWhenResolvedStart(call.target);
            completeRegisterSuccess(call.completion, promiseId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(call.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(call.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                call.completion,
                "unknown exception in capnp_lean_rpc_runtime_target_when_resolved_start");
          }
        } else if (std::holds_alternative<QueuedEnableTraceEncoder>(op)) {
          auto enable = std::get<QueuedEnableTraceEncoder>(std::move(op));
          try {
            setTraceEncoderEnabled(true);
            completeUnitSuccess(enable.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(enable.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(enable.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                enable.completion,
                "unknown exception in capnp_lean_rpc_runtime_enable_trace_encoder");
          }
        } else if (std::holds_alternative<QueuedDisableTraceEncoder>(op)) {
          auto disable = std::get<QueuedDisableTraceEncoder>(std::move(op));
          try {
            setTraceEncoderEnabled(false);
            completeUnitSuccess(disable.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(disable.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(disable.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                disable.completion,
                "unknown exception in capnp_lean_rpc_runtime_disable_trace_encoder");
          }
        } else if (std::holds_alternative<QueuedSetTraceEncoder>(op)) {
          auto setTrace = std::get<QueuedSetTraceEncoder>(std::move(op));
          try {
            setTraceEncoderFromLean(setTrace.encoder);
            completeUnitSuccess(setTrace.completion);
          } catch (const kj::Exception& e) {
            lean_dec(setTrace.encoder);
            completeUnitFailure(setTrace.completion, describeKjException(e));
          } catch (const std::exception& e) {
            lean_dec(setTrace.encoder);
            completeUnitFailure(setTrace.completion, e.what());
          } catch (...) {
            lean_dec(setTrace.encoder);
            completeUnitFailure(
                setTrace.completion,
                "unknown exception in capnp_lean_rpc_runtime_set_trace_encoder");
          }
        } else if (std::holds_alternative<QueuedCppCallWithAccept>(op)) {
          auto call = std::get<QueuedCppCallWithAccept>(std::move(op));
          try {
            auto promise = kj::evalNow([&]() {
              return processCppCallWithAccept(
                  *io.provider, io.waitScope, call.serverId, call.listenerId, call.address,
                  call.portHint, call.interfaceId, call.methodId, call.request, call.requestCaps);
            });
            completeSuccess(call.completion, promise.wait(io.waitScope));
          } catch (const kj::Exception& e) {
            completeFailureKj(call.completion, e);
          } catch (const std::exception& e) {
            completeFailure(call.completion, e.what());
          } catch (...) {
            completeFailure(call.completion,
                            "unknown exception in capnp_lean_rpc_runtime_cpp_call_with_accept");
          }
        } else if (std::holds_alternative<QueuedCppCallPipelinedWithAccept>(op)) {
          auto call = std::get<QueuedCppCallPipelinedWithAccept>(std::move(op));
          try {
            auto promise = kj::evalNow([&]() {
              return processCppPipelinedCallWithAccept(
                  *io.provider, io.waitScope, call.serverId, call.listenerId, call.address,
                  call.portHint, call.interfaceId, call.methodId, call.request, call.requestCaps,
                  call.pipelinedRequest, call.pipelinedRequestCaps);
            });
            completeSuccess(call.completion, promise.wait(io.waitScope));
          } catch (const kj::Exception& e) {
            completeFailureKj(call.completion, e);
          } catch (const std::exception& e) {
            completeFailure(call.completion, e.what());
          } catch (...) {
            completeFailure(call.completion,
                            "unknown exception in "
                            "capnp_lean_rpc_runtime_cpp_call_pipelined_with_accept");
          }
        } else if (std::holds_alternative<QueuedRegisterLoopbackTarget>(op)) {
          auto registration = std::get<QueuedRegisterLoopbackTarget>(std::move(op));
          try {
            completeRegisterSuccess(registration.completion, registerLoopbackTarget(*io.provider));
          } catch (const kj::Exception& e) {
            completeRegisterFailure(registration.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(registration.completion, e.what());
          } catch (...) {
            completeRegisterFailure(registration.completion,
                                    "unknown exception in capnp_lean_rpc_runtime_register_echo_target");
          }
        } else if (std::holds_alternative<QueuedRegisterLoopbackBootstrapTarget>(op)) {
          auto registration = std::get<QueuedRegisterLoopbackBootstrapTarget>(std::move(op));
          try {
            auto targetId = registerLoopbackTarget(*io.provider, registration.bootstrapTarget);
            completeRegisterSuccess(registration.completion, targetId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(registration.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(registration.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                registration.completion,
                "unknown exception in capnp_lean_rpc_runtime_register_loopback_target");
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
        } else if (std::holds_alternative<QueuedRegisterAdvancedHandlerTarget>(op)) {
          auto registration = std::get<QueuedRegisterAdvancedHandlerTarget>(std::move(op));
          try {
            auto targetId = registerAdvancedHandlerTarget(registration.handler);
            completeRegisterSuccess(registration.completion, targetId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(registration.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(registration.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                registration.completion,
                "unknown exception in capnp_lean_rpc_runtime_register_advanced_handler_target");
          }
          lean_dec(registration.handler);
        } else if (std::holds_alternative<QueuedRegisterTailCallHandlerTarget>(op)) {
          auto registration = std::get<QueuedRegisterTailCallHandlerTarget>(std::move(op));
          try {
            auto targetId = registerTailCallHandlerTarget(registration.handler);
            completeRegisterSuccess(registration.completion, targetId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(registration.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(registration.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                registration.completion,
                "unknown exception in capnp_lean_rpc_runtime_register_tailcall_handler_target");
          }
          lean_dec(registration.handler);
        } else if (std::holds_alternative<QueuedRegisterTailCallTarget>(op)) {
          auto registration = std::get<QueuedRegisterTailCallTarget>(std::move(op));
          try {
            auto targetId = registerTailCallTarget(registration.target);
            completeRegisterSuccess(registration.completion, targetId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(registration.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(registration.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                registration.completion,
                "unknown exception in capnp_lean_rpc_runtime_register_tailcall_target");
          }
        } else if (std::holds_alternative<QueuedRegisterFdTarget>(op)) {
          auto registration = std::get<QueuedRegisterFdTarget>(std::move(op));
          try {
            auto targetId = registerFdTarget(registration.fd);
            completeRegisterSuccess(registration.completion, targetId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(registration.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(registration.completion, e.what());
          } catch (...) {
            completeRegisterFailure(registration.completion,
                                    "unknown exception in capnp_lean_rpc_runtime_register_fd_target");
          }
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
        } else if (std::holds_alternative<QueuedReleaseTargets>(op)) {
          auto release = std::get<QueuedReleaseTargets>(std::move(op));
          try {
            releaseTargets(release.targets);
            completeUnitSuccess(release.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(release.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(release.completion, e.what());
          } catch (...) {
            completeUnitFailure(release.completion,
                                "unknown exception in capnp_lean_rpc_runtime_release_targets");
          }
        } else if (std::holds_alternative<QueuedRetainTarget>(op)) {
          auto retain = std::get<QueuedRetainTarget>(std::move(op));
          try {
            auto retained = retainTarget(retain.target);
            completeRegisterSuccess(retain.completion, retained);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(retain.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(retain.completion, e.what());
          } catch (...) {
            completeRegisterFailure(retain.completion,
                                    "unknown exception in capnp_lean_rpc_runtime_retain_target");
          }
        } else if (std::holds_alternative<QueuedNewPromiseCapability>(op)) {
          auto request = std::get<QueuedNewPromiseCapability>(std::move(op));
          try {
            auto ids = newPromiseCapability();
            completeRegisterPairSuccess(request.completion, ids.first, ids.second);
          } catch (const kj::Exception& e) {
            completeRegisterPairFailure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterPairFailure(request.completion, e.what());
          } catch (...) {
            completeRegisterPairFailure(
                request.completion,
                "unknown exception in capnp_lean_rpc_runtime_new_promise_capability");
          }
        } else if (std::holds_alternative<QueuedPromiseCapabilityFulfill>(op)) {
          auto request = std::get<QueuedPromiseCapabilityFulfill>(std::move(op));
          try {
            promiseCapabilityFulfill(request.fulfillerId, request.target);
            completeUnitSuccess(request.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(request.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                request.completion,
                "unknown exception in capnp_lean_rpc_runtime_promise_capability_fulfill");
          }
        } else if (std::holds_alternative<QueuedPromiseCapabilityReject>(op)) {
          auto request = std::get<QueuedPromiseCapabilityReject>(std::move(op));
          try {
            promiseCapabilityReject(request.fulfillerId, request.exceptionTypeTag,
                                   request.message, request.detailBytes);
            completeUnitSuccess(request.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(request.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                request.completion,
                "unknown exception in capnp_lean_rpc_runtime_promise_capability_reject");
          }
        } else if (std::holds_alternative<QueuedPromiseCapabilityRelease>(op)) {
          auto request = std::get<QueuedPromiseCapabilityRelease>(std::move(op));
          try {
            promiseCapabilityRelease(request.fulfillerId);
            completeUnitSuccess(request.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(request.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                request.completion,
                "unknown exception in capnp_lean_rpc_runtime_promise_capability_release");
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
        } else if (std::holds_alternative<QueuedConnectTargetStart>(op)) {
          auto connect = std::get<QueuedConnectTargetStart>(std::move(op));
          try {
            auto promiseId = connectTargetStart(*io.provider, std::move(connect.address),
                                                connect.portHint);
            completeRegisterSuccess(connect.completion, promiseId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(connect.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(connect.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                connect.completion,
                "unknown exception in capnp_lean_rpc_runtime_connect_start");
          }
        } else if (std::holds_alternative<QueuedConnectTargetFd>(op)) {
          auto connect = std::get<QueuedConnectTargetFd>(std::move(op));
          try {
            auto targetId = connectTargetFd(*io.lowLevelProvider, connect.fd);
            completeRegisterSuccess(connect.completion, targetId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(connect.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(connect.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                connect.completion,
                "unknown exception in capnp_lean_rpc_runtime_connect_fd");
          }
        } else if (std::holds_alternative<QueuedNewTransportPipe>(op)) {
          auto pipe = std::get<QueuedNewTransportPipe>(std::move(op));
          try {
            auto ids = newTransportPipe(*io.provider);
            completeRegisterPairSuccess(pipe.completion, ids.first, ids.second);
          } catch (const kj::Exception& e) {
            completeRegisterPairFailure(pipe.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterPairFailure(pipe.completion, e.what());
          } catch (...) {
            completeRegisterPairFailure(pipe.completion,
                                        "unknown exception in capnp_lean_rpc_runtime_new_transport_pipe");
          }
        } else if (std::holds_alternative<QueuedNewTransportFromFd>(op)) {
          auto request = std::get<QueuedNewTransportFromFd>(std::move(op));
          try {
            auto transportId = newTransportFromFd(*io.lowLevelProvider, request.fd);
            completeRegisterSuccess(request.completion, transportId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(request.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                request.completion,
                "unknown exception in capnp_lean_rpc_runtime_new_transport_from_fd");
          }
        } else if (std::holds_alternative<QueuedNewTransportFromFdTake>(op)) {
          auto request = std::get<QueuedNewTransportFromFdTake>(std::move(op));
          try {
            auto transportId = newTransportFromFdTake(*io.lowLevelProvider, request.fd);
            completeRegisterSuccess(request.completion, transportId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(request.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                request.completion,
                "unknown exception in capnp_lean_rpc_runtime_new_transport_from_fd_take");
          }
        } else if (std::holds_alternative<QueuedReleaseTransport>(op)) {
          auto release = std::get<QueuedReleaseTransport>(std::move(op));
          try {
            releaseTransport(release.transportId);
            completeUnitSuccess(release.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(release.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(release.completion, e.what());
          } catch (...) {
            completeUnitFailure(release.completion,
                                "unknown exception in capnp_lean_rpc_runtime_release_transport");
          }
        } else if (std::holds_alternative<QueuedTransportGetFd>(op)) {
          auto request = std::get<QueuedTransportGetFd>(std::move(op));
          try {
            completeInt64Success(request.completion, transportGetFd(io.waitScope, request.transportId));
          } catch (const kj::Exception& e) {
            completeInt64Failure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeInt64Failure(request.completion, e.what());
          } catch (...) {
            completeInt64Failure(
                request.completion,
                "unknown exception in capnp_lean_rpc_runtime_transport_get_fd");
          }
        } else if (std::holds_alternative<QueuedConnectTargetTransport>(op)) {
          auto connect = std::get<QueuedConnectTargetTransport>(std::move(op));
          try {
            auto targetId = connectTargetTransport(connect.transportId);
            completeRegisterSuccess(connect.completion, targetId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(connect.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(connect.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                connect.completion,
                "unknown exception in capnp_lean_rpc_runtime_connect_transport");
          }
        } else if (std::holds_alternative<QueuedListenLoopback>(op)) {
          auto listen = std::get<QueuedListenLoopback>(std::move(op));
          try {
            auto listenerId = listenLoopback(*io.provider, io.waitScope, listen.address, listen.portHint);
            completeRegisterSuccess(listen.completion, listenerId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(listen.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(listen.completion, e.what());
          } catch (...) {
            completeRegisterFailure(listen.completion,
                                    "unknown exception in capnp_lean_rpc_runtime_listen_echo");
          }
        } else if (std::holds_alternative<QueuedAcceptLoopback>(op)) {
          auto accept = std::get<QueuedAcceptLoopback>(std::move(op));
          try {
            acceptLoopback(io.waitScope, accept.listenerId);
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
        } else if (std::holds_alternative<QueuedNewClientStart>(op)) {
          auto newClientReq = std::get<QueuedNewClientStart>(std::move(op));
          try {
            auto promiseId =
                newClientStart(*io.provider, std::move(newClientReq.address), newClientReq.portHint);
            completeRegisterSuccess(newClientReq.completion, promiseId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(newClientReq.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(newClientReq.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                newClientReq.completion,
                "unknown exception in capnp_lean_rpc_runtime_new_client_start");
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
        } else if (std::holds_alternative<QueuedClientOnDisconnectStart>(op)) {
          auto disconnect = std::get<QueuedClientOnDisconnectStart>(std::move(op));
          try {
            auto promiseId = clientOnDisconnectStart(disconnect.clientId);
            completeRegisterSuccess(disconnect.completion, promiseId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(disconnect.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(disconnect.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                disconnect.completion,
                "unknown exception in capnp_lean_rpc_runtime_client_on_disconnect_start");
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
        } else if (std::holds_alternative<QueuedTargetCount>(op)) {
          auto metric = std::get<QueuedTargetCount>(std::move(op));
          try {
            completeUInt64Success(metric.completion, targetCount());
          } catch (const kj::Exception& e) {
            completeUInt64Failure(metric.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUInt64Failure(metric.completion, e.what());
          } catch (...) {
            completeUInt64Failure(metric.completion,
                                  "unknown exception in capnp_lean_rpc_runtime_target_count");
          }
        } else if (std::holds_alternative<QueuedListenerCount>(op)) {
          auto metric = std::get<QueuedListenerCount>(std::move(op));
          try {
            completeUInt64Success(metric.completion, listenerCount());
          } catch (const kj::Exception& e) {
            completeUInt64Failure(metric.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUInt64Failure(metric.completion, e.what());
          } catch (...) {
            completeUInt64Failure(metric.completion,
                                  "unknown exception in capnp_lean_rpc_runtime_listener_count");
          }
        } else if (std::holds_alternative<QueuedClientCount>(op)) {
          auto metric = std::get<QueuedClientCount>(std::move(op));
          try {
            completeUInt64Success(metric.completion, clientCount());
          } catch (const kj::Exception& e) {
            completeUInt64Failure(metric.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUInt64Failure(metric.completion, e.what());
          } catch (...) {
            completeUInt64Failure(metric.completion,
                                  "unknown exception in capnp_lean_rpc_runtime_client_count");
          }
        } else if (std::holds_alternative<QueuedServerCount>(op)) {
          auto metric = std::get<QueuedServerCount>(std::move(op));
          try {
            completeUInt64Success(metric.completion, serverCount());
          } catch (const kj::Exception& e) {
            completeUInt64Failure(metric.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUInt64Failure(metric.completion, e.what());
          } catch (...) {
            completeUInt64Failure(metric.completion,
                                  "unknown exception in capnp_lean_rpc_runtime_server_count");
          }
        } else if (std::holds_alternative<QueuedPendingCallCount>(op)) {
          auto metric = std::get<QueuedPendingCallCount>(std::move(op));
          try {
            completeUInt64Success(metric.completion, pendingCallCount());
          } catch (const kj::Exception& e) {
            completeUInt64Failure(metric.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUInt64Failure(metric.completion, e.what());
          } catch (...) {
            completeUInt64Failure(
                metric.completion,
                "unknown exception in capnp_lean_rpc_runtime_pending_call_count");
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
        } else if (std::holds_alternative<QueuedNewServerWithBootstrapFactory>(op)) {
          auto newServerReq = std::get<QueuedNewServerWithBootstrapFactory>(std::move(op));
          try {
            auto serverId = newServerWithBootstrapFactory(newServerReq.bootstrapFactory);
            completeRegisterSuccess(newServerReq.completion, serverId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(newServerReq.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(newServerReq.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                newServerReq.completion,
                "unknown exception in capnp_lean_rpc_runtime_new_server_with_bootstrap_factory");
          }
          lean_dec(newServerReq.bootstrapFactory);
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
        } else if (std::holds_alternative<QueuedServerAcceptStart>(op)) {
          auto accept = std::get<QueuedServerAcceptStart>(std::move(op));
          try {
            auto promiseId = serverAcceptStart(accept.serverId, accept.listenerId);
            completeRegisterSuccess(accept.completion, promiseId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(accept.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(accept.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                accept.completion,
                "unknown exception in capnp_lean_rpc_runtime_server_accept_start");
          }
        } else if (std::holds_alternative<QueuedServerAcceptFd>(op)) {
          auto accept = std::get<QueuedServerAcceptFd>(std::move(op));
          try {
            serverAcceptFd(*io.lowLevelProvider, accept.serverId, accept.fd);
            completeUnitSuccess(accept.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(accept.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(accept.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                accept.completion,
                "unknown exception in capnp_lean_rpc_runtime_server_accept_fd");
          }
        } else if (std::holds_alternative<QueuedServerAcceptTransport>(op)) {
          auto accept = std::get<QueuedServerAcceptTransport>(std::move(op));
          try {
            serverAcceptTransport(accept.serverId, accept.transportId);
            completeUnitSuccess(accept.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(accept.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(accept.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                accept.completion,
                "unknown exception in capnp_lean_rpc_runtime_server_accept_transport");
          }
        } else if (std::holds_alternative<QueuedServerDrain>(op)) {
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
        } else if (std::holds_alternative<QueuedServerDrainStart>(op)) {
          auto drain = std::get<QueuedServerDrainStart>(std::move(op));
          try {
            auto promiseId = serverDrainStart(drain.serverId);
            completeRegisterSuccess(drain.completion, promiseId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(drain.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(drain.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                drain.completion,
                "unknown exception in capnp_lean_rpc_runtime_server_drain_start");
          }
        } else if (std::holds_alternative<QueuedAwaitRegisterPromise>(op)) {
          auto promise = std::get<QueuedAwaitRegisterPromise>(std::move(op));
          try {
            auto id = awaitRegisterPromise(io.waitScope, promise.promiseId);
            completeRegisterSuccess(promise.completion, id);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(promise.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(promise.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                promise.completion,
                "unknown exception in capnp_lean_rpc_runtime_register_promise_await");
          }
        } else if (std::holds_alternative<QueuedCancelRegisterPromise>(op)) {
          auto promise = std::get<QueuedCancelRegisterPromise>(std::move(op));
          try {
            cancelRegisterPromise(promise.promiseId);
            completeUnitSuccess(promise.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(promise.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(promise.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                promise.completion,
                "unknown exception in capnp_lean_rpc_runtime_register_promise_cancel");
          }
        } else if (std::holds_alternative<QueuedReleaseRegisterPromise>(op)) {
          auto promise = std::get<QueuedReleaseRegisterPromise>(std::move(op));
          try {
            releaseRegisterPromise(promise.promiseId);
            completeUnitSuccess(promise.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(promise.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(promise.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                promise.completion,
                "unknown exception in capnp_lean_rpc_runtime_register_promise_release");
          }
        } else if (std::holds_alternative<QueuedAwaitUnitPromise>(op)) {
          auto promise = std::get<QueuedAwaitUnitPromise>(std::move(op));
          try {
            awaitUnitPromise(io.waitScope, promise.promiseId);
            completeUnitSuccess(promise.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(promise.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(promise.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                promise.completion,
                "unknown exception in capnp_lean_rpc_runtime_unit_promise_await");
          }
        } else if (std::holds_alternative<QueuedCancelUnitPromise>(op)) {
          auto promise = std::get<QueuedCancelUnitPromise>(std::move(op));
          try {
            cancelUnitPromise(promise.promiseId);
            completeUnitSuccess(promise.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(promise.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(promise.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                promise.completion,
                "unknown exception in capnp_lean_rpc_runtime_unit_promise_cancel");
          }
        } else if (std::holds_alternative<QueuedReleaseUnitPromise>(op)) {
          auto promise = std::get<QueuedReleaseUnitPromise>(std::move(op));
          try {
            releaseUnitPromise(promise.promiseId);
            completeUnitSuccess(promise.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(promise.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(promise.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                promise.completion,
                "unknown exception in capnp_lean_rpc_runtime_unit_promise_release");
          }
        } else if (std::holds_alternative<QueuedMultiVatNewClient>(op)) {
          auto request = std::get<QueuedMultiVatNewClient>(std::move(op));
          try {
            auto peerId = newMultiVatClient(request.name);
            completeRegisterSuccess(request.completion, peerId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(request.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                request.completion,
                "unknown exception in capnp_lean_rpc_runtime_multivat_new_client");
          }
        } else if (std::holds_alternative<QueuedMultiVatNewServer>(op)) {
          auto request = std::get<QueuedMultiVatNewServer>(std::move(op));
          try {
            auto peerId = newMultiVatServer(request.name, request.bootstrapTarget);
            completeRegisterSuccess(request.completion, peerId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(request.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                request.completion,
                "unknown exception in capnp_lean_rpc_runtime_multivat_new_server");
          }
        } else if (std::holds_alternative<QueuedMultiVatNewServerWithBootstrapFactory>(op)) {
          auto request = std::get<QueuedMultiVatNewServerWithBootstrapFactory>(std::move(op));
          try {
            auto peerId = newMultiVatServerWithBootstrapFactory(request.name, request.bootstrapFactory);
            completeRegisterSuccess(request.completion, peerId);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(request.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                request.completion,
                "unknown exception in capnp_lean_rpc_runtime_multivat_new_server_with_bootstrap_factory");
          }
          lean_dec(request.bootstrapFactory);
        } else if (std::holds_alternative<QueuedMultiVatReleasePeer>(op)) {
          auto request = std::get<QueuedMultiVatReleasePeer>(std::move(op));
          try {
            releaseMultiVatPeer(request.peerId);
            completeUnitSuccess(request.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(request.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                request.completion,
                "unknown exception in capnp_lean_rpc_runtime_multivat_release_peer");
          }
        } else if (std::holds_alternative<QueuedMultiVatBootstrap>(op)) {
          auto request = std::get<QueuedMultiVatBootstrap>(std::move(op));
          try {
            auto target = multiVatBootstrap(request.sourcePeerId, LeanVatId{request.host, request.unique});
            completeRegisterSuccess(request.completion, target);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(request.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                request.completion,
                "unknown exception in capnp_lean_rpc_runtime_multivat_bootstrap");
          }
        } else if (std::holds_alternative<QueuedMultiVatBootstrapPeer>(op)) {
          auto request = std::get<QueuedMultiVatBootstrapPeer>(std::move(op));
          try {
            auto target = multiVatBootstrapPeer(
                request.sourcePeerId, request.targetPeerId, request.unique);
            completeRegisterSuccess(request.completion, target);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(request.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                request.completion,
                "unknown exception in capnp_lean_rpc_runtime_multivat_bootstrap_peer");
          }
        } else if (std::holds_alternative<QueuedMultiVatSetForwardingEnabled>(op)) {
          auto request = std::get<QueuedMultiVatSetForwardingEnabled>(std::move(op));
          try {
            multiVatSetForwardingEnabled(request.enabled);
            completeUnitSuccess(request.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(request.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                request.completion,
                "unknown exception in capnp_lean_rpc_runtime_multivat_set_forwarding_enabled");
          }
        } else if (std::holds_alternative<QueuedMultiVatResetForwardingStats>(op)) {
          auto request = std::get<QueuedMultiVatResetForwardingStats>(std::move(op));
          try {
            multiVatResetForwardingStats();
            completeUnitSuccess(request.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(request.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                request.completion,
                "unknown exception in capnp_lean_rpc_runtime_multivat_reset_forwarding_stats");
          }
        } else if (std::holds_alternative<QueuedMultiVatForwardCount>(op)) {
          auto request = std::get<QueuedMultiVatForwardCount>(std::move(op));
          try {
            completeUInt64Success(request.completion, multiVatForwardCount());
          } catch (const kj::Exception& e) {
            completeUInt64Failure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUInt64Failure(request.completion, e.what());
          } catch (...) {
            completeUInt64Failure(
                request.completion,
                "unknown exception in capnp_lean_rpc_runtime_multivat_forward_count");
          }
        } else if (std::holds_alternative<QueuedMultiVatThirdPartyTokenCount>(op)) {
          auto request = std::get<QueuedMultiVatThirdPartyTokenCount>(std::move(op));
          try {
            completeUInt64Success(request.completion, multiVatThirdPartyTokenCount());
          } catch (const kj::Exception& e) {
            completeUInt64Failure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUInt64Failure(request.completion, e.what());
          } catch (...) {
            completeUInt64Failure(
                request.completion,
                "unknown exception in capnp_lean_rpc_runtime_multivat_third_party_token_count");
          }
        } else if (std::holds_alternative<QueuedMultiVatDeniedForwardCount>(op)) {
          auto request = std::get<QueuedMultiVatDeniedForwardCount>(std::move(op));
          try {
            completeUInt64Success(request.completion, multiVatDeniedForwardCount());
          } catch (const kj::Exception& e) {
            completeUInt64Failure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUInt64Failure(request.completion, e.what());
          } catch (...) {
            completeUInt64Failure(
                request.completion,
                "unknown exception in capnp_lean_rpc_runtime_multivat_denied_forward_count");
          }
        } else if (std::holds_alternative<QueuedMultiVatHasConnection>(op)) {
          auto request = std::get<QueuedMultiVatHasConnection>(std::move(op));
          try {
            completeUInt64Success(
                request.completion,
                multiVatHasConnection(request.fromPeerId, request.toPeerId) ? 1 : 0);
          } catch (const kj::Exception& e) {
            completeUInt64Failure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUInt64Failure(request.completion, e.what());
          } catch (...) {
            completeUInt64Failure(
                request.completion,
                "unknown exception in capnp_lean_rpc_runtime_multivat_has_connection");
          }
        } else if (std::holds_alternative<QueuedMultiVatSetRestorer>(op)) {
          auto request = std::get<QueuedMultiVatSetRestorer>(std::move(op));
          try {
            multiVatSetRestorer(request.peerId, request.restorer);
            completeUnitSuccess(request.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(request.completion, describeKjException(e));
            lean_dec(request.restorer);
          } catch (const std::exception& e) {
            completeUnitFailure(request.completion, e.what());
            lean_dec(request.restorer);
          } catch (...) {
            completeUnitFailure(
                request.completion,
                "unknown exception in capnp_lean_rpc_runtime_multivat_set_restorer");
            lean_dec(request.restorer);
          }
        } else if (std::holds_alternative<QueuedMultiVatClearRestorer>(op)) {
          auto request = std::get<QueuedMultiVatClearRestorer>(std::move(op));
          try {
            multiVatClearRestorer(request.peerId);
            completeUnitSuccess(request.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(request.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                request.completion,
                "unknown exception in capnp_lean_rpc_runtime_multivat_clear_restorer");
          }
        } else if (std::holds_alternative<QueuedMultiVatPublishSturdyRef>(op)) {
          auto request = std::get<QueuedMultiVatPublishSturdyRef>(std::move(op));
          try {
            multiVatPublishSturdyRef(request.hostPeerId, request.objectId, request.targetId);
            completeUnitSuccess(request.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(request.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                request.completion,
                "unknown exception in capnp_lean_rpc_runtime_multivat_publish_sturdy_ref");
          }
        } else if (std::holds_alternative<QueuedMultiVatRestoreSturdyRef>(op)) {
          auto request = std::get<QueuedMultiVatRestoreSturdyRef>(std::move(op));
          try {
            auto target = multiVatRestoreSturdyRef(
                request.sourcePeerId, LeanVatId{request.host, request.unique}, request.objectId);
            completeRegisterSuccess(request.completion, target);
          } catch (const kj::Exception& e) {
            completeRegisterFailure(request.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeRegisterFailure(request.completion, e.what());
          } catch (...) {
            completeRegisterFailure(
                request.completion,
                "unknown exception in capnp_lean_rpc_runtime_multivat_restore_sturdy_ref");
          }
        } else {
          auto pump = std::get<QueuedPump>(std::move(op));
          try {
            io.waitScope.poll(1);
            completeUnitSuccess(pump.completion);
          } catch (const kj::Exception& e) {
            completeUnitFailure(pump.completion, describeKjException(e));
          } catch (const std::exception& e) {
            completeUnitFailure(pump.completion, e.what());
          } catch (...) {
            completeUnitFailure(
                pump.completion,
                "unknown exception in capnp_lean_rpc_runtime_pump");
          }
        }

        // Keep KJ progressing even if the runtime queue stays busy.
        io.waitScope.poll(1);
      }

      // Tear down RPC clients/servers on the runtime thread while async I/O is still valid.
      targets_.clear();
      listeners_.clear();
      loopbackPeers_.clear();
      networkClientPeers_.clear();
      transports_.clear();
      loopbackPeerOwnerByTarget_.clear();
      loopbackPeerOwnerRefCount_.clear();
      networkPeerOwnerByTarget_.clear();
      networkPeerOwnerRefCount_.clear();
      networkServerPeers_.clear();
      pendingCalls_.clear();
      activeCancelableDeferredTasks_.clear();
      pendingDeferredCancelRequests_ = 0;
      registerPromises_.clear();
      unitPromises_.clear();
      promiseCapabilityFulfillers_.clear();
      clients_.clear();
      servers_.clear();
      sturdyRefs_.clear();
      multiVatPeerIdsByName_.clear();
      multiVatPeers_.clear();
      genericBootstrapFactories_.clear();
      clearTraceEncoderHandler();
      traceEncoderEnabled_ = false;

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
      } else if (std::holds_alternative<QueuedStartPendingCall>(op)) {
        completeRegisterFailure(std::get<QueuedStartPendingCall>(op).completion, message);
      } else if (std::holds_alternative<QueuedAwaitPendingCall>(op)) {
        completeFailure(std::get<QueuedAwaitPendingCall>(op).completion, message);
      } else if (std::holds_alternative<QueuedReleasePendingCall>(op)) {
        completeUnitFailure(std::get<QueuedReleasePendingCall>(op).completion, message);
      } else if (std::holds_alternative<QueuedGetPipelinedCap>(op)) {
        completeRegisterFailure(std::get<QueuedGetPipelinedCap>(op).completion, message);
      } else if (std::holds_alternative<QueuedStreamingCall>(op)) {
        completeUnitFailure(std::get<QueuedStreamingCall>(op).completion, message);
      } else if (std::holds_alternative<QueuedTargetGetFd>(op)) {
        completeInt64Failure(std::get<QueuedTargetGetFd>(op).completion, message);
      } else if (std::holds_alternative<QueuedTargetWhenResolved>(op)) {
        completeUnitFailure(std::get<QueuedTargetWhenResolved>(op).completion, message);
      } else if (std::holds_alternative<QueuedTargetWhenResolvedStart>(op)) {
        completeRegisterFailure(std::get<QueuedTargetWhenResolvedStart>(op).completion, message);
      } else if (std::holds_alternative<QueuedEnableTraceEncoder>(op)) {
        completeUnitFailure(std::get<QueuedEnableTraceEncoder>(op).completion, message);
      } else if (std::holds_alternative<QueuedDisableTraceEncoder>(op)) {
        completeUnitFailure(std::get<QueuedDisableTraceEncoder>(op).completion, message);
      } else if (std::holds_alternative<QueuedSetTraceEncoder>(op)) {
        auto& setTrace = std::get<QueuedSetTraceEncoder>(op);
        lean_dec(setTrace.encoder);
        completeUnitFailure(setTrace.completion, message);
      } else if (std::holds_alternative<QueuedCppCallWithAccept>(op)) {
        completeFailure(std::get<QueuedCppCallWithAccept>(op).completion, message);
      } else if (std::holds_alternative<QueuedCppCallPipelinedWithAccept>(op)) {
        completeFailure(std::get<QueuedCppCallPipelinedWithAccept>(op).completion, message);
      } else if (std::holds_alternative<QueuedRegisterLoopbackTarget>(op)) {
        completeRegisterFailure(std::get<QueuedRegisterLoopbackTarget>(op).completion, message);
      } else if (std::holds_alternative<QueuedRegisterLoopbackBootstrapTarget>(op)) {
        completeRegisterFailure(
            std::get<QueuedRegisterLoopbackBootstrapTarget>(op).completion, message);
      } else if (std::holds_alternative<QueuedRegisterHandlerTarget>(op)) {
        auto& registration = std::get<QueuedRegisterHandlerTarget>(op);
        lean_dec(registration.handler);
        completeRegisterFailure(registration.completion, message);
      } else if (std::holds_alternative<QueuedRegisterAdvancedHandlerTarget>(op)) {
        auto& registration = std::get<QueuedRegisterAdvancedHandlerTarget>(op);
        lean_dec(registration.handler);
        completeRegisterFailure(registration.completion, message);
      } else if (std::holds_alternative<QueuedRegisterTailCallHandlerTarget>(op)) {
        auto& registration = std::get<QueuedRegisterTailCallHandlerTarget>(op);
        lean_dec(registration.handler);
        completeRegisterFailure(registration.completion, message);
      } else if (std::holds_alternative<QueuedRegisterTailCallTarget>(op)) {
        completeRegisterFailure(std::get<QueuedRegisterTailCallTarget>(op).completion, message);
      } else if (std::holds_alternative<QueuedRegisterFdTarget>(op)) {
        completeRegisterFailure(std::get<QueuedRegisterFdTarget>(op).completion, message);
      } else if (std::holds_alternative<QueuedReleaseTarget>(op)) {
        completeUnitFailure(std::get<QueuedReleaseTarget>(op).completion, message);
      } else if (std::holds_alternative<QueuedReleaseTargets>(op)) {
        completeUnitFailure(std::get<QueuedReleaseTargets>(op).completion, message);
      } else if (std::holds_alternative<QueuedRetainTarget>(op)) {
        completeRegisterFailure(std::get<QueuedRetainTarget>(op).completion, message);
      } else if (std::holds_alternative<QueuedNewPromiseCapability>(op)) {
        completeRegisterPairFailure(std::get<QueuedNewPromiseCapability>(op).completion, message);
      } else if (std::holds_alternative<QueuedPromiseCapabilityFulfill>(op)) {
        completeUnitFailure(std::get<QueuedPromiseCapabilityFulfill>(op).completion, message);
      } else if (std::holds_alternative<QueuedPromiseCapabilityReject>(op)) {
        completeUnitFailure(std::get<QueuedPromiseCapabilityReject>(op).completion, message);
      } else if (std::holds_alternative<QueuedPromiseCapabilityRelease>(op)) {
        completeUnitFailure(std::get<QueuedPromiseCapabilityRelease>(op).completion, message);
      } else if (std::holds_alternative<QueuedConnectTarget>(op)) {
        completeRegisterFailure(std::get<QueuedConnectTarget>(op).completion, message);
      } else if (std::holds_alternative<QueuedConnectTargetStart>(op)) {
        completeRegisterFailure(std::get<QueuedConnectTargetStart>(op).completion, message);
      } else if (std::holds_alternative<QueuedConnectTargetFd>(op)) {
        completeRegisterFailure(std::get<QueuedConnectTargetFd>(op).completion, message);
      } else if (std::holds_alternative<QueuedNewTransportPipe>(op)) {
        completeRegisterPairFailure(std::get<QueuedNewTransportPipe>(op).completion, message);
      } else if (std::holds_alternative<QueuedNewTransportFromFd>(op)) {
        completeRegisterFailure(std::get<QueuedNewTransportFromFd>(op).completion, message);
      } else if (std::holds_alternative<QueuedNewTransportFromFdTake>(op)) {
        completeRegisterFailure(std::get<QueuedNewTransportFromFdTake>(op).completion, message);
      } else if (std::holds_alternative<QueuedReleaseTransport>(op)) {
        completeUnitFailure(std::get<QueuedReleaseTransport>(op).completion, message);
      } else if (std::holds_alternative<QueuedTransportGetFd>(op)) {
        completeInt64Failure(std::get<QueuedTransportGetFd>(op).completion, message);
      } else if (std::holds_alternative<QueuedConnectTargetTransport>(op)) {
        completeRegisterFailure(std::get<QueuedConnectTargetTransport>(op).completion, message);
      } else if (std::holds_alternative<QueuedListenLoopback>(op)) {
        completeRegisterFailure(std::get<QueuedListenLoopback>(op).completion, message);
      } else if (std::holds_alternative<QueuedAcceptLoopback>(op)) {
        completeUnitFailure(std::get<QueuedAcceptLoopback>(op).completion, message);
      } else if (std::holds_alternative<QueuedReleaseListener>(op)) {
        completeUnitFailure(std::get<QueuedReleaseListener>(op).completion, message);
      } else if (std::holds_alternative<QueuedNewClient>(op)) {
        completeRegisterFailure(std::get<QueuedNewClient>(op).completion, message);
      } else if (std::holds_alternative<QueuedNewClientStart>(op)) {
        completeRegisterFailure(std::get<QueuedNewClientStart>(op).completion, message);
      } else if (std::holds_alternative<QueuedReleaseClient>(op)) {
        completeUnitFailure(std::get<QueuedReleaseClient>(op).completion, message);
      } else if (std::holds_alternative<QueuedClientBootstrap>(op)) {
        completeRegisterFailure(std::get<QueuedClientBootstrap>(op).completion, message);
      } else if (std::holds_alternative<QueuedClientOnDisconnect>(op)) {
        completeUnitFailure(std::get<QueuedClientOnDisconnect>(op).completion, message);
      } else if (std::holds_alternative<QueuedClientOnDisconnectStart>(op)) {
        completeRegisterFailure(std::get<QueuedClientOnDisconnectStart>(op).completion, message);
      } else if (std::holds_alternative<QueuedClientSetFlowLimit>(op)) {
        completeUnitFailure(std::get<QueuedClientSetFlowLimit>(op).completion, message);
      } else if (std::holds_alternative<QueuedNewServer>(op)) {
        completeRegisterFailure(std::get<QueuedNewServer>(op).completion, message);
      } else if (std::holds_alternative<QueuedNewServerWithBootstrapFactory>(op)) {
        auto& newServer = std::get<QueuedNewServerWithBootstrapFactory>(op);
        lean_dec(newServer.bootstrapFactory);
        completeRegisterFailure(newServer.completion, message);
      } else if (std::holds_alternative<QueuedReleaseServer>(op)) {
        completeUnitFailure(std::get<QueuedReleaseServer>(op).completion, message);
      } else if (std::holds_alternative<QueuedServerListen>(op)) {
        completeRegisterFailure(std::get<QueuedServerListen>(op).completion, message);
      } else if (std::holds_alternative<QueuedServerAccept>(op)) {
        completeUnitFailure(std::get<QueuedServerAccept>(op).completion, message);
      } else if (std::holds_alternative<QueuedServerAcceptStart>(op)) {
        completeRegisterFailure(std::get<QueuedServerAcceptStart>(op).completion, message);
      } else if (std::holds_alternative<QueuedServerAcceptFd>(op)) {
        completeUnitFailure(std::get<QueuedServerAcceptFd>(op).completion, message);
      } else if (std::holds_alternative<QueuedServerAcceptTransport>(op)) {
        completeUnitFailure(std::get<QueuedServerAcceptTransport>(op).completion, message);
      } else if (std::holds_alternative<QueuedServerDrain>(op)) {
        completeUnitFailure(std::get<QueuedServerDrain>(op).completion, message);
      } else if (std::holds_alternative<QueuedServerDrainStart>(op)) {
        completeRegisterFailure(std::get<QueuedServerDrainStart>(op).completion, message);
      } else if (std::holds_alternative<QueuedClientQueueSize>(op)) {
        completeUInt64Failure(std::get<QueuedClientQueueSize>(op).completion, message);
      } else if (std::holds_alternative<QueuedClientQueueCount>(op)) {
        completeUInt64Failure(std::get<QueuedClientQueueCount>(op).completion, message);
      } else if (std::holds_alternative<QueuedClientOutgoingWaitNanos>(op)) {
        completeUInt64Failure(std::get<QueuedClientOutgoingWaitNanos>(op).completion, message);
      } else if (std::holds_alternative<QueuedTargetCount>(op)) {
        completeUInt64Failure(std::get<QueuedTargetCount>(op).completion, message);
      } else if (std::holds_alternative<QueuedListenerCount>(op)) {
        completeUInt64Failure(std::get<QueuedListenerCount>(op).completion, message);
      } else if (std::holds_alternative<QueuedClientCount>(op)) {
        completeUInt64Failure(std::get<QueuedClientCount>(op).completion, message);
      } else if (std::holds_alternative<QueuedServerCount>(op)) {
        completeUInt64Failure(std::get<QueuedServerCount>(op).completion, message);
      } else if (std::holds_alternative<QueuedPendingCallCount>(op)) {
        completeUInt64Failure(std::get<QueuedPendingCallCount>(op).completion, message);
      } else if (std::holds_alternative<QueuedAwaitRegisterPromise>(op)) {
        completeRegisterFailure(std::get<QueuedAwaitRegisterPromise>(op).completion, message);
      } else if (std::holds_alternative<QueuedCancelRegisterPromise>(op)) {
        completeUnitFailure(std::get<QueuedCancelRegisterPromise>(op).completion, message);
      } else if (std::holds_alternative<QueuedReleaseRegisterPromise>(op)) {
        completeUnitFailure(std::get<QueuedReleaseRegisterPromise>(op).completion, message);
      } else if (std::holds_alternative<QueuedAwaitUnitPromise>(op)) {
        completeUnitFailure(std::get<QueuedAwaitUnitPromise>(op).completion, message);
      } else if (std::holds_alternative<QueuedCancelUnitPromise>(op)) {
        completeUnitFailure(std::get<QueuedCancelUnitPromise>(op).completion, message);
      } else if (std::holds_alternative<QueuedReleaseUnitPromise>(op)) {
        completeUnitFailure(std::get<QueuedReleaseUnitPromise>(op).completion, message);
      } else if (std::holds_alternative<QueuedMultiVatNewClient>(op)) {
        completeRegisterFailure(std::get<QueuedMultiVatNewClient>(op).completion, message);
      } else if (std::holds_alternative<QueuedMultiVatNewServer>(op)) {
        completeRegisterFailure(std::get<QueuedMultiVatNewServer>(op).completion, message);
      } else if (std::holds_alternative<QueuedMultiVatNewServerWithBootstrapFactory>(op)) {
        auto& request = std::get<QueuedMultiVatNewServerWithBootstrapFactory>(op);
        lean_dec(request.bootstrapFactory);
        completeRegisterFailure(request.completion, message);
      } else if (std::holds_alternative<QueuedMultiVatReleasePeer>(op)) {
        completeUnitFailure(std::get<QueuedMultiVatReleasePeer>(op).completion, message);
      } else if (std::holds_alternative<QueuedMultiVatBootstrap>(op)) {
        completeRegisterFailure(std::get<QueuedMultiVatBootstrap>(op).completion, message);
      } else if (std::holds_alternative<QueuedMultiVatBootstrapPeer>(op)) {
        completeRegisterFailure(std::get<QueuedMultiVatBootstrapPeer>(op).completion, message);
      } else if (std::holds_alternative<QueuedMultiVatSetForwardingEnabled>(op)) {
        completeUnitFailure(std::get<QueuedMultiVatSetForwardingEnabled>(op).completion, message);
      } else if (std::holds_alternative<QueuedMultiVatResetForwardingStats>(op)) {
        completeUnitFailure(std::get<QueuedMultiVatResetForwardingStats>(op).completion, message);
      } else if (std::holds_alternative<QueuedMultiVatForwardCount>(op)) {
        completeUInt64Failure(std::get<QueuedMultiVatForwardCount>(op).completion, message);
      } else if (std::holds_alternative<QueuedMultiVatThirdPartyTokenCount>(op)) {
        completeUInt64Failure(std::get<QueuedMultiVatThirdPartyTokenCount>(op).completion, message);
      } else if (std::holds_alternative<QueuedMultiVatDeniedForwardCount>(op)) {
        completeUInt64Failure(std::get<QueuedMultiVatDeniedForwardCount>(op).completion, message);
      } else if (std::holds_alternative<QueuedMultiVatHasConnection>(op)) {
        completeUInt64Failure(std::get<QueuedMultiVatHasConnection>(op).completion, message);
      } else if (std::holds_alternative<QueuedMultiVatSetRestorer>(op)) {
        auto& request = std::get<QueuedMultiVatSetRestorer>(op);
        lean_dec(request.restorer);
        completeUnitFailure(request.completion, message);
      } else if (std::holds_alternative<QueuedMultiVatClearRestorer>(op)) {
        completeUnitFailure(std::get<QueuedMultiVatClearRestorer>(op).completion, message);
      } else if (std::holds_alternative<QueuedMultiVatPublishSturdyRef>(op)) {
        completeUnitFailure(std::get<QueuedMultiVatPublishSturdyRef>(op).completion, message);
      } else if (std::holds_alternative<QueuedMultiVatRestoreSturdyRef>(op)) {
        completeRegisterFailure(std::get<QueuedMultiVatRestoreSturdyRef>(op).completion, message);
      } else {
        completeUnitFailure(std::get<QueuedPump>(op).completion, message);
      }
    }
  }

  uint maxFdsPerMessage_;
  std::thread worker_;

  std::mutex startupMutex_;
  std::condition_variable startupCv_;
  bool startupComplete_ = false;
  std::string startupError_;

  std::mutex queueMutex_;
  std::condition_variable queueCv_;
  std::atomic<kj::EventPort*> eventPort_{nullptr};
  bool stopping_ = false;
  std::deque<QueuedOperation> queue_;

  std::unordered_map<uint32_t, capnp::Capability::Client> targets_;
  std::unordered_map<uint32_t, kj::Own<kj::ConnectionReceiver>> listeners_;
  std::unordered_map<uint32_t, kj::Own<LoopbackPeer>> loopbackPeers_;
  std::unordered_map<uint32_t, kj::Own<NetworkClientPeer>> networkClientPeers_;
  std::unordered_map<uint32_t, kj::Own<kj::AsyncCapabilityStream>> transports_;
  std::unordered_map<uint32_t, uint32_t> loopbackPeerOwnerByTarget_;
  std::unordered_map<uint32_t, uint32_t> loopbackPeerOwnerRefCount_;
  std::unordered_map<uint32_t, uint32_t> networkPeerOwnerByTarget_;
  std::unordered_map<uint32_t, uint32_t> networkPeerOwnerRefCount_;
  kj::Vector<kj::Own<NetworkServerPeer>> networkServerPeers_;
  std::unordered_map<uint32_t, PendingCall> pendingCalls_;
  std::deque<std::weak_ptr<DeferredLeanTaskState>> activeCancelableDeferredTasks_;
  uint64_t pendingDeferredCancelRequests_ = 0;
  std::unordered_map<uint32_t, PendingRegisterPromise> registerPromises_;
  std::unordered_map<uint32_t, PendingUnitPromise> unitPromises_;
  std::unordered_map<uint32_t, PendingPromiseCapability> promiseCapabilityFulfillers_;
  std::unordered_map<uint32_t, kj::Own<NetworkClientPeer>> clients_;
  std::unordered_map<uint32_t, kj::Own<RuntimeServer>> servers_;
  GenericVatNetwork genericVatNetwork_;
  std::unordered_map<uint32_t, kj::Own<MultiVatPeer>> multiVatPeers_;
  std::unordered_map<std::string, uint32_t> multiVatPeerIdsByName_;
  std::unordered_map<uint32_t, std::unordered_map<std::string, capnp::Capability::Client>>
      sturdyRefs_;
  kj::Vector<kj::Own<LeanGenericBootstrapFactory>> genericBootstrapFactories_;
  bool traceEncoderEnabled_ = false;
  lean_object* traceEncoderHandler_ = nullptr;
  uint32_t nextTargetId_ = 1;
  uint32_t nextListenerId_ = 1;
  uint32_t nextTransportId_ = 1;
  uint32_t nextClientId_ = 1;
  uint32_t nextServerId_ = 1;
  uint32_t nextMultiVatPeerId_ = 1;
  uint32_t nextPendingCallId_ = 1;
  uint32_t nextRegisterPromiseId_ = 1;
  uint32_t nextUnitPromiseId_ = 1;
  uint32_t nextPromiseCapabilityFulfillerId_ = 1;
};

std::mutex gRuntimeRegistryMutex;
kj::HashMap<uint64_t, std::shared_ptr<RuntimeLoop>> gRuntimes;
std::atomic<uint64_t> gNextRuntimeId{1};

uint64_t allocateRuntimeIdLocked() {
  while (true) {
    uint64_t id = gNextRuntimeId.fetch_add(1, std::memory_order_relaxed);
    if (id == 0) {
      continue;
    }
    if (gRuntimes.find(id) == kj::none) {
      return id;
    }
  }
}

std::shared_ptr<RuntimeLoop> getRuntime(uint64_t runtimeId) {
  std::lock_guard<std::mutex> lock(gRuntimeRegistryMutex);
  KJ_IF_SOME(runtime, gRuntimes.find(runtimeId)) {
    return runtime;
  }
  return nullptr;
}

bool isRuntimeAlive(uint64_t runtimeId) {
  std::lock_guard<std::mutex> lock(gRuntimeRegistryMutex);
  return gRuntimes.find(runtimeId) != kj::none;
}

std::shared_ptr<RuntimeLoop> unregisterRuntime(uint64_t runtimeId) {
  std::lock_guard<std::mutex> lock(gRuntimeRegistryMutex);
  KJ_IF_SOME(runtime, gRuntimes.find(runtimeId)) {
    auto out = runtime;
    gRuntimes.erase(runtimeId);
    return out;
  }
  return nullptr;
}


uint64_t createRuntime(uint32_t maxFdsPerMessage) {
  auto runtime = std::make_shared<RuntimeLoop>(maxFdsPerMessage);
  uint64_t runtimeId;
  {
    std::lock_guard<std::mutex> lock(gRuntimeRegistryMutex);
    runtimeId = allocateRuntimeIdLocked();
    gRuntimes.insert(runtimeId, runtime);
  }
  return runtimeId;
}

void shutdown(RuntimeLoop& runtime) { runtime.shutdown(); }

bool isWorkerThread(const RuntimeLoop& runtime) { return runtime.isWorkerThread(); }

uint32_t retainTargetInline(RuntimeLoop& runtime, uint32_t target) {
  return runtime.retainTargetInline(target);
}

void releaseTargetInline(RuntimeLoop& runtime, uint32_t target) { runtime.releaseTargetInline(target); }

void releaseTargetsInline(RuntimeLoop& runtime, const std::vector<uint32_t>& targets) {
  runtime.releaseTargetsInline(targets);
}

std::pair<uint32_t, uint32_t> newPromiseCapabilityInline(RuntimeLoop& runtime) {
  return runtime.newPromiseCapabilityInline();
}

void promiseCapabilityFulfillInline(RuntimeLoop& runtime, uint32_t fulfillerId, uint32_t target) {
  runtime.promiseCapabilityFulfillInline(fulfillerId, target);
}

void promiseCapabilityRejectInline(RuntimeLoop& runtime, uint32_t fulfillerId,
                                   uint8_t exceptionTypeTag, std::string message,
                                   std::vector<uint8_t> detailBytes) {
  runtime.promiseCapabilityRejectInline(fulfillerId, exceptionTypeTag, std::move(message),
                                        std::move(detailBytes));
}

void promiseCapabilityReleaseInline(RuntimeLoop& runtime, uint32_t fulfillerId) {
  runtime.promiseCapabilityReleaseInline(fulfillerId);
}

std::shared_ptr<RawCallCompletion> enqueueRawCall(
    RuntimeLoop& runtime, uint32_t target, uint64_t interfaceId, uint16_t methodId,
    std::vector<uint8_t> request, std::vector<uint32_t> requestCaps) {
  return runtime.enqueueRawCall(target, interfaceId, methodId, std::move(request),
                                std::move(requestCaps));
}

std::shared_ptr<RegisterTargetCompletion> enqueueStartPendingCall(
    RuntimeLoop& runtime, uint32_t target, uint64_t interfaceId, uint16_t methodId,
    std::vector<uint8_t> request, std::vector<uint32_t> requestCaps) {
  return runtime.enqueueStartPendingCall(target, interfaceId, methodId, std::move(request),
                                         std::move(requestCaps));
}

std::shared_ptr<RawCallCompletion> enqueueAwaitPendingCall(RuntimeLoop& runtime,
                                                           uint32_t pendingCallId) {
  return runtime.enqueueAwaitPendingCall(pendingCallId);
}

std::shared_ptr<UnitCompletion> enqueueReleasePendingCall(RuntimeLoop& runtime,
                                                          uint32_t pendingCallId) {
  return runtime.enqueueReleasePendingCall(pendingCallId);
}

std::shared_ptr<RegisterTargetCompletion> enqueueGetPipelinedCap(
    RuntimeLoop& runtime, uint32_t pendingCallId, std::vector<uint16_t> pointerPath) {
  return runtime.enqueueGetPipelinedCap(pendingCallId, std::move(pointerPath));
}

std::shared_ptr<UnitCompletion> enqueueStreamingCall(
    RuntimeLoop& runtime, uint32_t target, uint64_t interfaceId, uint16_t methodId,
    std::vector<uint8_t> request, std::vector<uint32_t> requestCaps) {
  return runtime.enqueueStreamingCall(target, interfaceId, methodId, std::move(request),
                                      std::move(requestCaps));
}

std::shared_ptr<Int64Completion> enqueueTargetGetFd(RuntimeLoop& runtime, uint32_t target) {
  return runtime.enqueueTargetGetFd(target);
}

std::shared_ptr<UnitCompletion> enqueueTargetWhenResolved(RuntimeLoop& runtime, uint32_t target) {
  return runtime.enqueueTargetWhenResolved(target);
}

std::shared_ptr<RegisterTargetCompletion> enqueueTargetWhenResolvedStart(RuntimeLoop& runtime,
                                                                         uint32_t target) {
  return runtime.enqueueTargetWhenResolvedStart(target);
}

std::shared_ptr<UnitCompletion> enqueueEnableTraceEncoder(RuntimeLoop& runtime) {
  return runtime.enqueueEnableTraceEncoder();
}

std::shared_ptr<UnitCompletion> enqueueDisableTraceEncoder(RuntimeLoop& runtime) {
  return runtime.enqueueDisableTraceEncoder();
}

std::shared_ptr<UnitCompletion> enqueueSetTraceEncoder(RuntimeLoop& runtime,
                                                       b_lean_obj_arg encoder) {
  return runtime.enqueueSetTraceEncoder(encoder);
}

std::shared_ptr<UnitCompletion> enqueueReleaseTarget(RuntimeLoop& runtime, uint32_t target) {
  return runtime.enqueueReleaseTarget(target);
}

std::shared_ptr<UnitCompletion> enqueueReleaseTargets(RuntimeLoop& runtime,
                                                      std::vector<uint32_t> targets) {
  return runtime.enqueueReleaseTargets(std::move(targets));
}

std::shared_ptr<RegisterTargetCompletion> enqueueRetainTarget(RuntimeLoop& runtime,
                                                              uint32_t target) {
  return runtime.enqueueRetainTarget(target);
}

std::shared_ptr<RegisterPairCompletion> enqueueNewPromiseCapability(RuntimeLoop& runtime) {
  return runtime.enqueueNewPromiseCapability();
}

std::shared_ptr<UnitCompletion> enqueuePromiseCapabilityFulfill(RuntimeLoop& runtime,
                                                                uint32_t fulfillerId,
                                                                uint32_t target) {
  return runtime.enqueuePromiseCapabilityFulfill(fulfillerId, target);
}

std::shared_ptr<UnitCompletion> enqueuePromiseCapabilityReject(RuntimeLoop& runtime,
                                                               uint32_t fulfillerId,
                                                               uint8_t exceptionTypeTag,
                                                               std::string message,
                                                               std::vector<uint8_t> detailBytes) {
  return runtime.enqueuePromiseCapabilityReject(fulfillerId, exceptionTypeTag, std::move(message),
                                                std::move(detailBytes));
}

std::shared_ptr<UnitCompletion> enqueuePromiseCapabilityRelease(RuntimeLoop& runtime,
                                                                uint32_t fulfillerId) {
  return runtime.enqueuePromiseCapabilityRelease(fulfillerId);
}

std::shared_ptr<RegisterTargetCompletion> enqueueConnectTarget(RuntimeLoop& runtime,
                                                               std::string address,
                                                               uint32_t portHint) {
  return runtime.enqueueConnectTarget(std::move(address), portHint);
}

std::shared_ptr<RegisterTargetCompletion> enqueueConnectTargetStart(RuntimeLoop& runtime,
                                                                    std::string address,
                                                                    uint32_t portHint) {
  return runtime.enqueueConnectTargetStart(std::move(address), portHint);
}

std::shared_ptr<RegisterTargetCompletion> enqueueConnectTargetFd(RuntimeLoop& runtime,
                                                                 uint32_t fd) {
  return runtime.enqueueConnectTargetFd(fd);
}

std::shared_ptr<RegisterPairCompletion> enqueueNewTransportPipe(RuntimeLoop& runtime) {
  return runtime.enqueueNewTransportPipe();
}

std::shared_ptr<RegisterTargetCompletion> enqueueNewTransportFromFd(RuntimeLoop& runtime,
                                                                    uint32_t fd) {
  return runtime.enqueueNewTransportFromFd(fd);
}

std::shared_ptr<RegisterTargetCompletion> enqueueNewTransportFromFdTake(RuntimeLoop& runtime,
                                                                        uint32_t fd) {
  return runtime.enqueueNewTransportFromFdTake(fd);
}

std::shared_ptr<UnitCompletion> enqueueReleaseTransport(RuntimeLoop& runtime,
                                                        uint32_t transportId) {
  return runtime.enqueueReleaseTransport(transportId);
}

std::shared_ptr<Int64Completion> enqueueTransportGetFd(RuntimeLoop& runtime,
                                                       uint32_t transportId) {
  return runtime.enqueueTransportGetFd(transportId);
}

std::shared_ptr<RegisterTargetCompletion> enqueueConnectTargetTransport(RuntimeLoop& runtime,
                                                                        uint32_t transportId) {
  return runtime.enqueueConnectTargetTransport(transportId);
}

std::shared_ptr<RegisterTargetCompletion> enqueueListenLoopback(RuntimeLoop& runtime,
                                                                std::string address,
                                                                uint32_t portHint) {
  return runtime.enqueueListenLoopback(std::move(address), portHint);
}

std::shared_ptr<UnitCompletion> enqueueAcceptLoopback(RuntimeLoop& runtime, uint32_t listenerId) {
  return runtime.enqueueAcceptLoopback(listenerId);
}

std::shared_ptr<UnitCompletion> enqueueReleaseListener(RuntimeLoop& runtime, uint32_t listenerId) {
  return runtime.enqueueReleaseListener(listenerId);
}

std::shared_ptr<RegisterTargetCompletion> enqueueNewClient(RuntimeLoop& runtime,
                                                           std::string address,
                                                           uint32_t portHint) {
  return runtime.enqueueNewClient(std::move(address), portHint);
}

std::shared_ptr<RegisterTargetCompletion> enqueueNewClientStart(RuntimeLoop& runtime,
                                                                std::string address,
                                                                uint32_t portHint) {
  return runtime.enqueueNewClientStart(std::move(address), portHint);
}

std::shared_ptr<UnitCompletion> enqueueReleaseClient(RuntimeLoop& runtime, uint32_t clientId) {
  return runtime.enqueueReleaseClient(clientId);
}

std::shared_ptr<RegisterTargetCompletion> enqueueClientBootstrap(RuntimeLoop& runtime,
                                                                 uint32_t clientId) {
  return runtime.enqueueClientBootstrap(clientId);
}

std::shared_ptr<UnitCompletion> enqueueClientOnDisconnect(RuntimeLoop& runtime, uint32_t clientId) {
  return runtime.enqueueClientOnDisconnect(clientId);
}

std::shared_ptr<RegisterTargetCompletion> enqueueClientOnDisconnectStart(RuntimeLoop& runtime,
                                                                         uint32_t clientId) {
  return runtime.enqueueClientOnDisconnectStart(clientId);
}

std::shared_ptr<UnitCompletion> enqueueClientSetFlowLimit(RuntimeLoop& runtime, uint32_t clientId,
                                                          uint64_t words) {
  return runtime.enqueueClientSetFlowLimit(clientId, words);
}

std::shared_ptr<UInt64Completion> enqueueClientQueueSize(RuntimeLoop& runtime, uint32_t clientId) {
  return runtime.enqueueClientQueueSize(clientId);
}

std::shared_ptr<UInt64Completion> enqueueClientQueueCount(RuntimeLoop& runtime, uint32_t clientId) {
  return runtime.enqueueClientQueueCount(clientId);
}

std::shared_ptr<UInt64Completion> enqueueClientOutgoingWaitNanos(RuntimeLoop& runtime,
                                                                 uint32_t clientId) {
  return runtime.enqueueClientOutgoingWaitNanos(clientId);
}

std::shared_ptr<UInt64Completion> enqueueTargetCount(RuntimeLoop& runtime) {
  return runtime.enqueueTargetCount();
}

std::shared_ptr<UInt64Completion> enqueueListenerCount(RuntimeLoop& runtime) {
  return runtime.enqueueListenerCount();
}

std::shared_ptr<UInt64Completion> enqueueClientCount(RuntimeLoop& runtime) {
  return runtime.enqueueClientCount();
}

std::shared_ptr<UInt64Completion> enqueueServerCount(RuntimeLoop& runtime) {
  return runtime.enqueueServerCount();
}

std::shared_ptr<UInt64Completion> enqueuePendingCallCount(RuntimeLoop& runtime) {
  return runtime.enqueuePendingCallCount();
}

std::shared_ptr<RegisterTargetCompletion> enqueueNewServer(RuntimeLoop& runtime,
                                                           uint32_t bootstrapTarget) {
  return runtime.enqueueNewServer(bootstrapTarget);
}

std::shared_ptr<RegisterTargetCompletion> enqueueNewServerWithBootstrapFactory(
    RuntimeLoop& runtime, b_lean_obj_arg bootstrapFactory) {
  return runtime.enqueueNewServerWithBootstrapFactory(bootstrapFactory);
}

std::shared_ptr<UnitCompletion> enqueueReleaseServer(RuntimeLoop& runtime, uint32_t serverId) {
  return runtime.enqueueReleaseServer(serverId);
}

std::shared_ptr<RegisterTargetCompletion> enqueueServerListen(RuntimeLoop& runtime, uint32_t serverId,
                                                              std::string address,
                                                              uint32_t portHint) {
  return runtime.enqueueServerListen(serverId, std::move(address), portHint);
}

std::shared_ptr<UnitCompletion> enqueueServerAccept(RuntimeLoop& runtime, uint32_t serverId,
                                                    uint32_t listenerId) {
  return runtime.enqueueServerAccept(serverId, listenerId);
}

std::shared_ptr<RegisterTargetCompletion> enqueueServerAcceptStart(RuntimeLoop& runtime,
                                                                   uint32_t serverId,
                                                                   uint32_t listenerId) {
  return runtime.enqueueServerAcceptStart(serverId, listenerId);
}

std::shared_ptr<UnitCompletion> enqueueServerAcceptFd(RuntimeLoop& runtime, uint32_t serverId,
                                                      uint32_t fd) {
  return runtime.enqueueServerAcceptFd(serverId, fd);
}

std::shared_ptr<UnitCompletion> enqueueServerAcceptTransport(RuntimeLoop& runtime,
                                                             uint32_t serverId,
                                                             uint32_t transportId) {
  return runtime.enqueueServerAcceptTransport(serverId, transportId);
}

std::shared_ptr<UnitCompletion> enqueueServerDrain(RuntimeLoop& runtime, uint32_t serverId) {
  return runtime.enqueueServerDrain(serverId);
}

std::shared_ptr<RegisterTargetCompletion> enqueueServerDrainStart(RuntimeLoop& runtime,
                                                                  uint32_t serverId) {
  return runtime.enqueueServerDrainStart(serverId);
}

std::shared_ptr<RegisterTargetCompletion> enqueueRegisterLoopbackTarget(RuntimeLoop& runtime) {
  return runtime.enqueueRegisterLoopbackTarget();
}

std::shared_ptr<RegisterTargetCompletion> enqueueRegisterLoopbackTarget(RuntimeLoop& runtime,
                                                                        uint32_t bootstrapTarget) {
  return runtime.enqueueRegisterLoopbackTarget(bootstrapTarget);
}

std::shared_ptr<RegisterTargetCompletion> enqueueRegisterHandlerTarget(RuntimeLoop& runtime,
                                                                       b_lean_obj_arg handler) {
  return runtime.enqueueRegisterHandlerTarget(handler);
}

std::shared_ptr<RegisterTargetCompletion> enqueueRegisterAdvancedHandlerTarget(RuntimeLoop& runtime,
                                                                               b_lean_obj_arg handler) {
  return runtime.enqueueRegisterAdvancedHandlerTarget(handler);
}

std::shared_ptr<RegisterTargetCompletion> enqueueRegisterTailCallHandlerTarget(RuntimeLoop& runtime,
                                                                               b_lean_obj_arg handler) {
  return runtime.enqueueRegisterTailCallHandlerTarget(handler);
}

std::shared_ptr<RegisterTargetCompletion> enqueueRegisterTailCallTarget(RuntimeLoop& runtime,
                                                                        uint32_t target) {
  return runtime.enqueueRegisterTailCallTarget(target);
}

std::shared_ptr<RegisterTargetCompletion> enqueueRegisterFdTarget(RuntimeLoop& runtime, uint32_t fd) {
  return runtime.enqueueRegisterFdTarget(fd);
}

std::shared_ptr<RawCallCompletion> enqueueCppCallWithAccept(
    RuntimeLoop& runtime, uint32_t serverId, uint32_t listenerId, std::string address,
    uint32_t portHint, uint64_t interfaceId, uint16_t methodId, std::vector<uint8_t> request,
    std::vector<uint32_t> requestCaps) {
  return runtime.enqueueCppCallWithAccept(serverId, listenerId, std::move(address), portHint,
                                          interfaceId, methodId, std::move(request),
                                          std::move(requestCaps));
}

std::shared_ptr<RawCallCompletion> enqueueCppCallPipelinedWithAccept(
    RuntimeLoop& runtime, uint32_t serverId, uint32_t listenerId, std::string address,
    uint32_t portHint, uint64_t interfaceId, uint16_t methodId, std::vector<uint8_t> request,
    std::vector<uint32_t> requestCaps, std::vector<uint8_t> pipelinedRequest,
    std::vector<uint32_t> pipelinedRequestCaps) {
  return runtime.enqueueCppCallPipelinedWithAccept(
      serverId, listenerId, std::move(address), portHint, interfaceId, methodId,
      std::move(request), std::move(requestCaps), std::move(pipelinedRequest),
      std::move(pipelinedRequestCaps));
}

std::shared_ptr<RegisterTargetCompletion> enqueueAwaitRegisterPromise(RuntimeLoop& runtime,
                                                                      uint32_t promiseId) {
  return runtime.enqueueAwaitRegisterPromise(promiseId);
}

std::shared_ptr<UnitCompletion> enqueueCancelRegisterPromise(RuntimeLoop& runtime,
                                                             uint32_t promiseId) {
  return runtime.enqueueCancelRegisterPromise(promiseId);
}

std::shared_ptr<UnitCompletion> enqueueReleaseRegisterPromise(RuntimeLoop& runtime,
                                                              uint32_t promiseId) {
  return runtime.enqueueReleaseRegisterPromise(promiseId);
}

std::shared_ptr<UnitCompletion> enqueueAwaitUnitPromise(RuntimeLoop& runtime, uint32_t promiseId) {
  return runtime.enqueueAwaitUnitPromise(promiseId);
}

std::shared_ptr<UnitCompletion> enqueueCancelUnitPromise(RuntimeLoop& runtime, uint32_t promiseId) {
  return runtime.enqueueCancelUnitPromise(promiseId);
}

std::shared_ptr<UnitCompletion> enqueueReleaseUnitPromise(RuntimeLoop& runtime,
                                                          uint32_t promiseId) {
  return runtime.enqueueReleaseUnitPromise(promiseId);
}

std::shared_ptr<UnitCompletion> enqueuePump(RuntimeLoop& runtime) { return runtime.enqueuePump(); }

std::shared_ptr<RegisterTargetCompletion> enqueueMultiVatNewClient(RuntimeLoop& runtime,
                                                                   std::string name) {
  return runtime.enqueueMultiVatNewClient(std::move(name));
}

std::shared_ptr<RegisterTargetCompletion> enqueueMultiVatNewServer(RuntimeLoop& runtime,
                                                                   std::string name,
                                                                   uint32_t bootstrapTarget) {
  return runtime.enqueueMultiVatNewServer(std::move(name), bootstrapTarget);
}

std::shared_ptr<RegisterTargetCompletion> enqueueMultiVatNewServerWithBootstrapFactory(
    RuntimeLoop& runtime, std::string name, b_lean_obj_arg bootstrapFactory) {
  return runtime.enqueueMultiVatNewServerWithBootstrapFactory(std::move(name), bootstrapFactory);
}

std::shared_ptr<UnitCompletion> enqueueMultiVatReleasePeer(RuntimeLoop& runtime, uint32_t peerId) {
  return runtime.enqueueMultiVatReleasePeer(peerId);
}

std::shared_ptr<RegisterTargetCompletion> enqueueMultiVatBootstrap(RuntimeLoop& runtime,
                                                                   uint32_t sourcePeerId,
                                                                   std::string host,
                                                                   bool unique) {
  return runtime.enqueueMultiVatBootstrap(sourcePeerId, std::move(host), unique);
}

std::shared_ptr<RegisterTargetCompletion> enqueueMultiVatBootstrapPeer(RuntimeLoop& runtime,
                                                                       uint32_t sourcePeerId,
                                                                       uint32_t peerId,
                                                                       bool unique) {
  return runtime.enqueueMultiVatBootstrapPeer(sourcePeerId, peerId, unique);
}

std::shared_ptr<UnitCompletion> enqueueMultiVatSetForwardingEnabled(RuntimeLoop& runtime,
                                                                    bool enabled) {
  return runtime.enqueueMultiVatSetForwardingEnabled(enabled);
}

std::shared_ptr<UnitCompletion> enqueueMultiVatResetForwardingStats(RuntimeLoop& runtime) {
  return runtime.enqueueMultiVatResetForwardingStats();
}

std::shared_ptr<UInt64Completion> enqueueMultiVatForwardCount(RuntimeLoop& runtime) {
  return runtime.enqueueMultiVatForwardCount();
}

std::shared_ptr<UInt64Completion> enqueueMultiVatThirdPartyTokenCount(RuntimeLoop& runtime) {
  return runtime.enqueueMultiVatThirdPartyTokenCount();
}

std::shared_ptr<UInt64Completion> enqueueMultiVatDeniedForwardCount(RuntimeLoop& runtime) {
  return runtime.enqueueMultiVatDeniedForwardCount();
}

std::shared_ptr<UInt64Completion> enqueueMultiVatHasConnection(RuntimeLoop& runtime,
                                                               uint32_t fromPeerId,
                                                               uint32_t toPeerId) {
  return runtime.enqueueMultiVatHasConnection(fromPeerId, toPeerId);
}

std::shared_ptr<UnitCompletion> enqueueMultiVatSetRestorer(RuntimeLoop& runtime, uint32_t peerId,
                                                           b_lean_obj_arg restorer) {
  return runtime.enqueueMultiVatSetRestorer(peerId, restorer);
}

std::shared_ptr<UnitCompletion> enqueueMultiVatClearRestorer(RuntimeLoop& runtime, uint32_t peerId) {
  return runtime.enqueueMultiVatClearRestorer(peerId);
}

std::shared_ptr<UnitCompletion> enqueueMultiVatPublishSturdyRef(RuntimeLoop& runtime,
                                                                uint32_t hostPeerId,
                                                                std::vector<uint8_t> objectId,
                                                                uint32_t targetId) {
  return runtime.enqueueMultiVatPublishSturdyRef(hostPeerId, std::move(objectId), targetId);
}

std::shared_ptr<RegisterTargetCompletion> enqueueMultiVatRestoreSturdyRef(
    RuntimeLoop& runtime, uint32_t sourcePeerId, std::string host, bool unique,
    std::vector<uint8_t> objectId) {
  return runtime.enqueueMultiVatRestoreSturdyRef(sourcePeerId, std::move(host), unique,
                                                 std::move(objectId));
}

}  // namespace capnp_lean_rpc
