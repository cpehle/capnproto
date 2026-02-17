#pragma once

#include <lean/lean.h>

#include <atomic>
#include <kj/exception.h>

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

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

lean_obj_res mkByteArrayCopy(const uint8_t* data, size_t size);
lean_obj_res mkIoUserError(const std::string& message);
void mkIoOkUnit(lean_obj_res& out);

const char* kjExceptionTypeName(kj::Exception::Type type);
std::string describeKjException(const kj::Exception& e);

std::vector<uint32_t> decodeCapTable(const uint8_t* data, size_t size);
std::vector<uint32_t> decodeCapTable(b_lean_obj_arg bytes);

std::vector<uint16_t> decodePipelineOps(const uint8_t* data, size_t size);
std::vector<uint16_t> decodePipelineOps(b_lean_obj_arg bytes);

std::vector<uint8_t> copyByteArray(b_lean_obj_arg bytes);

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
  uint8_t exceptionTypeTag = 0;  // kj::Exception::Type as uint8_t
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

uint64_t createRuntime(uint32_t maxFdsPerMessage);
std::shared_ptr<RuntimeLoop> getRuntime(uint64_t runtimeId);
bool isRuntimeAlive(uint64_t runtimeId);
std::shared_ptr<RuntimeLoop> unregisterRuntime(uint64_t runtimeId);
void shutdown(RuntimeLoop& runtime);

bool isWorkerThread(const RuntimeLoop& runtime);
uint32_t retainTargetInline(RuntimeLoop& runtime, uint32_t target);
void releaseTargetInline(RuntimeLoop& runtime, uint32_t target);
void releaseTargetsInline(RuntimeLoop& runtime, const std::vector<uint32_t>& targets);

std::pair<uint32_t, uint32_t> newPromiseCapabilityInline(RuntimeLoop& runtime);
void promiseCapabilityFulfillInline(RuntimeLoop& runtime, uint32_t fulfillerId, uint32_t target);
void promiseCapabilityRejectInline(RuntimeLoop& runtime, uint32_t fulfillerId,
                                   uint8_t exceptionTypeTag, std::string message,
                                   std::vector<uint8_t> detailBytes);
void promiseCapabilityReleaseInline(RuntimeLoop& runtime, uint32_t fulfillerId);

std::shared_ptr<KjPromiseIdCompletion> enqueueKjAsyncSleepNanosStart(
    RuntimeLoop& runtime, uint64_t delayNanos);
std::shared_ptr<UnitCompletion> enqueueKjAsyncPromiseAwait(RuntimeLoop& runtime,
                                                           uint32_t promiseId);
std::shared_ptr<UnitCompletion> enqueueKjAsyncPromiseCancel(RuntimeLoop& runtime,
                                                            uint32_t promiseId);
std::shared_ptr<UnitCompletion> enqueueKjAsyncPromiseRelease(RuntimeLoop& runtime,
                                                             uint32_t promiseId);
std::shared_ptr<KjPromiseIdCompletion> enqueueKjAsyncPromiseThenStart(
    RuntimeLoop& runtime, uint32_t firstPromiseId, uint32_t secondPromiseId);
std::shared_ptr<KjPromiseIdCompletion> enqueueKjAsyncPromiseCatchStart(
    RuntimeLoop& runtime, uint32_t promiseId, uint32_t fallbackPromiseId);
std::shared_ptr<KjPromiseIdCompletion> enqueueKjAsyncPromiseAllStart(
    RuntimeLoop& runtime, std::vector<uint32_t> promiseIds);
std::shared_ptr<KjPromiseIdCompletion> enqueueKjAsyncPromiseRaceStart(
    RuntimeLoop& runtime, std::vector<uint32_t> promiseIds);

uint32_t kjAsyncSleepNanosStartInline(RuntimeLoop& runtime, uint64_t delayNanos);
void kjAsyncPromiseCancelInline(RuntimeLoop& runtime, uint32_t promiseId);
void kjAsyncPromiseReleaseInline(RuntimeLoop& runtime, uint32_t promiseId);
uint32_t kjAsyncPromiseThenStartInline(RuntimeLoop& runtime, uint32_t firstPromiseId,
                                       uint32_t secondPromiseId);
uint32_t kjAsyncPromiseCatchStartInline(RuntimeLoop& runtime, uint32_t promiseId,
                                        uint32_t fallbackPromiseId);
uint32_t kjAsyncPromiseAllStartInline(RuntimeLoop& runtime,
                                      std::vector<uint32_t> promiseIds);
uint32_t kjAsyncPromiseRaceStartInline(RuntimeLoop& runtime,
                                       std::vector<uint32_t> promiseIds);

std::shared_ptr<RawCallCompletion> enqueueRawCall(
    RuntimeLoop& runtime, uint32_t target, uint64_t interfaceId, uint16_t methodId,
    std::vector<uint8_t> request, std::vector<uint32_t> requestCaps);
std::shared_ptr<RegisterTargetCompletion> enqueueStartPendingCall(
    RuntimeLoop& runtime, uint32_t target, uint64_t interfaceId, uint16_t methodId,
    std::vector<uint8_t> request, std::vector<uint32_t> requestCaps);
std::shared_ptr<RawCallCompletion> enqueueAwaitPendingCall(RuntimeLoop& runtime,
                                                           uint32_t pendingCallId);
std::shared_ptr<UnitCompletion> enqueueReleasePendingCall(RuntimeLoop& runtime,
                                                          uint32_t pendingCallId);
std::shared_ptr<RegisterTargetCompletion> enqueueGetPipelinedCap(
    RuntimeLoop& runtime, uint32_t pendingCallId, std::vector<uint16_t> pointerPath);
std::shared_ptr<UnitCompletion> enqueueStreamingCall(
    RuntimeLoop& runtime, uint32_t target, uint64_t interfaceId, uint16_t methodId,
    std::vector<uint8_t> request, std::vector<uint32_t> requestCaps);
std::shared_ptr<Int64Completion> enqueueTargetGetFd(RuntimeLoop& runtime, uint32_t target);
std::shared_ptr<UnitCompletion> enqueueTargetWhenResolved(RuntimeLoop& runtime, uint32_t target);
std::shared_ptr<RegisterTargetCompletion> enqueueTargetWhenResolvedStart(RuntimeLoop& runtime,
                                                                         uint32_t target);
std::shared_ptr<UnitCompletion> enqueueEnableTraceEncoder(RuntimeLoop& runtime);
std::shared_ptr<UnitCompletion> enqueueDisableTraceEncoder(RuntimeLoop& runtime);
std::shared_ptr<UnitCompletion> enqueueSetTraceEncoder(RuntimeLoop& runtime, b_lean_obj_arg encoder);

std::shared_ptr<UnitCompletion> enqueueReleaseTarget(RuntimeLoop& runtime, uint32_t target);
std::shared_ptr<UnitCompletion> enqueueReleaseTargets(RuntimeLoop& runtime,
                                                      std::vector<uint32_t> targets);
std::shared_ptr<RegisterTargetCompletion> enqueueRetainTarget(RuntimeLoop& runtime, uint32_t target);

std::shared_ptr<RegisterPairCompletion> enqueueNewPromiseCapability(RuntimeLoop& runtime);
std::shared_ptr<UnitCompletion> enqueuePromiseCapabilityFulfill(RuntimeLoop& runtime,
                                                                uint32_t fulfillerId,
                                                                uint32_t target);
std::shared_ptr<UnitCompletion> enqueuePromiseCapabilityReject(RuntimeLoop& runtime,
                                                               uint32_t fulfillerId,
                                                               uint8_t exceptionTypeTag,
                                                               std::string message,
                                                               std::vector<uint8_t> detailBytes);
std::shared_ptr<UnitCompletion> enqueuePromiseCapabilityRelease(RuntimeLoop& runtime,
                                                                uint32_t fulfillerId);

std::shared_ptr<RegisterTargetCompletion> enqueueConnectTarget(RuntimeLoop& runtime,
                                                               std::string address,
                                                               uint32_t portHint);
std::shared_ptr<RegisterTargetCompletion> enqueueConnectTargetStart(RuntimeLoop& runtime,
                                                                    std::string address,
                                                                    uint32_t portHint);
std::shared_ptr<RegisterTargetCompletion> enqueueConnectTargetFd(RuntimeLoop& runtime, uint32_t fd);

std::shared_ptr<RegisterPairCompletion> enqueueNewTransportPipe(RuntimeLoop& runtime);
std::shared_ptr<RegisterTargetCompletion> enqueueNewTransportFromFd(RuntimeLoop& runtime, uint32_t fd);
std::shared_ptr<RegisterTargetCompletion> enqueueNewTransportFromFdTake(RuntimeLoop& runtime, uint32_t fd);
std::shared_ptr<UnitCompletion> enqueueReleaseTransport(RuntimeLoop& runtime, uint32_t transportId);
std::shared_ptr<Int64Completion> enqueueTransportGetFd(RuntimeLoop& runtime, uint32_t transportId);
std::shared_ptr<RegisterTargetCompletion> enqueueConnectTargetTransport(RuntimeLoop& runtime,
                                                                        uint32_t transportId);

std::shared_ptr<RegisterTargetCompletion> enqueueListenLoopback(RuntimeLoop& runtime,
                                                                std::string address,
                                                                uint32_t portHint);
std::shared_ptr<UnitCompletion> enqueueAcceptLoopback(RuntimeLoop& runtime, uint32_t listenerId);
std::shared_ptr<UnitCompletion> enqueueReleaseListener(RuntimeLoop& runtime, uint32_t listenerId);

std::shared_ptr<RegisterTargetCompletion> enqueueNewClient(RuntimeLoop& runtime, std::string address,
                                                           uint32_t portHint);
std::shared_ptr<RegisterTargetCompletion> enqueueNewClientStart(RuntimeLoop& runtime,
                                                                std::string address,
                                                                uint32_t portHint);
std::shared_ptr<UnitCompletion> enqueueReleaseClient(RuntimeLoop& runtime, uint32_t clientId);
std::shared_ptr<RegisterTargetCompletion> enqueueClientBootstrap(RuntimeLoop& runtime, uint32_t clientId);
std::shared_ptr<UnitCompletion> enqueueClientOnDisconnect(RuntimeLoop& runtime, uint32_t clientId);
std::shared_ptr<RegisterTargetCompletion> enqueueClientOnDisconnectStart(RuntimeLoop& runtime,
                                                                         uint32_t clientId);
std::shared_ptr<UnitCompletion> enqueueClientSetFlowLimit(RuntimeLoop& runtime, uint32_t clientId,
                                                          uint64_t words);
std::shared_ptr<UInt64Completion> enqueueClientQueueSize(RuntimeLoop& runtime, uint32_t clientId);
std::shared_ptr<UInt64Completion> enqueueClientQueueCount(RuntimeLoop& runtime, uint32_t clientId);
std::shared_ptr<UInt64Completion> enqueueClientOutgoingWaitNanos(RuntimeLoop& runtime,
                                                                 uint32_t clientId);

std::shared_ptr<UInt64Completion> enqueueTargetCount(RuntimeLoop& runtime);
std::shared_ptr<UInt64Completion> enqueueListenerCount(RuntimeLoop& runtime);
std::shared_ptr<UInt64Completion> enqueueClientCount(RuntimeLoop& runtime);
std::shared_ptr<UInt64Completion> enqueueServerCount(RuntimeLoop& runtime);
std::shared_ptr<UInt64Completion> enqueuePendingCallCount(RuntimeLoop& runtime);

std::shared_ptr<RegisterTargetCompletion> enqueueNewServer(RuntimeLoop& runtime, uint32_t bootstrapTarget);
std::shared_ptr<RegisterTargetCompletion> enqueueNewServerWithBootstrapFactory(RuntimeLoop& runtime,
                                                                               b_lean_obj_arg bootstrapFactory);
std::shared_ptr<UnitCompletion> enqueueReleaseServer(RuntimeLoop& runtime, uint32_t serverId);
std::shared_ptr<RegisterTargetCompletion> enqueueServerListen(RuntimeLoop& runtime, uint32_t serverId,
                                                              std::string address,
                                                              uint32_t portHint);
std::shared_ptr<UnitCompletion> enqueueServerAccept(RuntimeLoop& runtime, uint32_t serverId,
                                                    uint32_t listenerId);
std::shared_ptr<RegisterTargetCompletion> enqueueServerAcceptStart(RuntimeLoop& runtime,
                                                                   uint32_t serverId,
                                                                   uint32_t listenerId);
std::shared_ptr<UnitCompletion> enqueueServerAcceptFd(RuntimeLoop& runtime, uint32_t serverId,
                                                      uint32_t fd);
std::shared_ptr<UnitCompletion> enqueueServerAcceptTransport(RuntimeLoop& runtime, uint32_t serverId,
                                                             uint32_t transportId);
std::shared_ptr<UnitCompletion> enqueueServerDrain(RuntimeLoop& runtime, uint32_t serverId);
std::shared_ptr<RegisterTargetCompletion> enqueueServerDrainStart(RuntimeLoop& runtime,
                                                                  uint32_t serverId);

std::shared_ptr<RegisterTargetCompletion> enqueueRegisterLoopbackTarget(RuntimeLoop& runtime);
std::shared_ptr<RegisterTargetCompletion> enqueueRegisterLoopbackTarget(RuntimeLoop& runtime,
                                                                        uint32_t bootstrapTarget);
std::shared_ptr<RegisterTargetCompletion> enqueueRegisterHandlerTarget(RuntimeLoop& runtime,
                                                                       b_lean_obj_arg handler);
std::shared_ptr<RegisterTargetCompletion> enqueueRegisterAdvancedHandlerTarget(RuntimeLoop& runtime,
                                                                               b_lean_obj_arg handler);
std::shared_ptr<RegisterTargetCompletion> enqueueRegisterTailCallHandlerTarget(RuntimeLoop& runtime,
                                                                               b_lean_obj_arg handler);
std::shared_ptr<RegisterTargetCompletion> enqueueRegisterTailCallTarget(RuntimeLoop& runtime,
                                                                        uint32_t target);
std::shared_ptr<RegisterTargetCompletion> enqueueRegisterFdTarget(RuntimeLoop& runtime, uint32_t fd);

std::shared_ptr<RawCallCompletion> enqueueCppCallWithAccept(
    RuntimeLoop& runtime, uint32_t serverId, uint32_t listenerId, std::string address,
    uint32_t portHint, uint64_t interfaceId, uint16_t methodId, std::vector<uint8_t> request,
    std::vector<uint32_t> requestCaps);
std::shared_ptr<RawCallCompletion> enqueueCppCallPipelinedWithAccept(
    RuntimeLoop& runtime, uint32_t serverId, uint32_t listenerId, std::string address,
    uint32_t portHint, uint64_t interfaceId, uint16_t methodId, std::vector<uint8_t> request,
    std::vector<uint32_t> requestCaps, std::vector<uint8_t> pipelinedRequest,
    std::vector<uint32_t> pipelinedRequestCaps);

std::shared_ptr<RegisterTargetCompletion> enqueueAwaitRegisterPromise(RuntimeLoop& runtime,
                                                                      uint32_t promiseId);
std::shared_ptr<UnitCompletion> enqueueCancelRegisterPromise(RuntimeLoop& runtime, uint32_t promiseId);
std::shared_ptr<UnitCompletion> enqueueReleaseRegisterPromise(RuntimeLoop& runtime, uint32_t promiseId);

std::shared_ptr<UnitCompletion> enqueueAwaitUnitPromise(RuntimeLoop& runtime, uint32_t promiseId);
std::shared_ptr<UnitCompletion> enqueueCancelUnitPromise(RuntimeLoop& runtime, uint32_t promiseId);
std::shared_ptr<UnitCompletion> enqueueReleaseUnitPromise(RuntimeLoop& runtime, uint32_t promiseId);

std::shared_ptr<UnitCompletion> enqueuePump(RuntimeLoop& runtime);

std::shared_ptr<RegisterTargetCompletion> enqueueMultiVatNewClient(RuntimeLoop& runtime,
                                                                   std::string name);
std::shared_ptr<RegisterTargetCompletion> enqueueMultiVatNewServer(RuntimeLoop& runtime,
                                                                   std::string name,
                                                                   uint32_t bootstrapTarget);
std::shared_ptr<RegisterTargetCompletion> enqueueMultiVatNewServerWithBootstrapFactory(
    RuntimeLoop& runtime, std::string name, b_lean_obj_arg bootstrapFactory);
std::shared_ptr<UnitCompletion> enqueueMultiVatReleasePeer(RuntimeLoop& runtime, uint32_t peerId);
std::shared_ptr<RegisterTargetCompletion> enqueueMultiVatBootstrap(RuntimeLoop& runtime,
                                                                   uint32_t sourcePeerId,
                                                                   std::string host,
                                                                   bool unique);
std::shared_ptr<RegisterTargetCompletion> enqueueMultiVatBootstrapPeer(RuntimeLoop& runtime,
                                                                       uint32_t sourcePeerId,
                                                                       uint32_t peerId,
                                                                       bool unique);
std::shared_ptr<UnitCompletion> enqueueMultiVatSetForwardingEnabled(RuntimeLoop& runtime, bool enabled);
std::shared_ptr<UnitCompletion> enqueueMultiVatResetForwardingStats(RuntimeLoop& runtime);
std::shared_ptr<UInt64Completion> enqueueMultiVatForwardCount(RuntimeLoop& runtime);
std::shared_ptr<UInt64Completion> enqueueMultiVatThirdPartyTokenCount(RuntimeLoop& runtime);
std::shared_ptr<UInt64Completion> enqueueMultiVatDeniedForwardCount(RuntimeLoop& runtime);
std::shared_ptr<UInt64Completion> enqueueMultiVatHasConnection(RuntimeLoop& runtime,
                                                               uint32_t fromPeerId,
                                                               uint32_t toPeerId);
std::shared_ptr<UnitCompletion> enqueueMultiVatSetRestorer(RuntimeLoop& runtime, uint32_t peerId,
                                                           b_lean_obj_arg restorer);
std::shared_ptr<UnitCompletion> enqueueMultiVatClearRestorer(RuntimeLoop& runtime, uint32_t peerId);
std::shared_ptr<UnitCompletion> enqueueMultiVatPublishSturdyRef(RuntimeLoop& runtime, uint32_t hostPeerId,
                                                                std::vector<uint8_t> objectId,
                                                                uint32_t targetId);
std::shared_ptr<RegisterTargetCompletion> enqueueMultiVatRestoreSturdyRef(
    RuntimeLoop& runtime, uint32_t sourcePeerId, std::string host, bool unique,
    std::vector<uint8_t> objectId);

RawCallResult cppCallOneShot(const std::string& address, uint32_t portHint, uint64_t interfaceId,
                             uint16_t methodId, const std::vector<uint8_t>& requestBytes,
                             const std::vector<uint32_t>& requestCapIds);
RawCallResult cppCallPipelinedCapOneShot(
    const std::string& address, uint32_t portHint, uint64_t interfaceId, uint16_t methodId,
    const std::vector<uint8_t>& requestBytes, const std::vector<uint32_t>& requestCapIds,
    const std::vector<uint8_t>& pipelinedRequestBytes,
    const std::vector<uint32_t>& pipelinedRequestCapIds);
RawCallResult cppServeOneShotEx(const std::string& address, uint32_t portHint, uint64_t interfaceId,
                                uint16_t methodId, uint32_t delayMillis, bool throwException,
                                bool throwWithDetail, bool waitForDisconnect);
RawCallResult cppServeOneShot(const std::string& address, uint32_t portHint, uint64_t interfaceId,
                              uint16_t methodId);

}  // namespace capnp_lean_rpc
