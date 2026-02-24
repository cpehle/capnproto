#include "rpc_bridge_payload_ref.h"

#include <atomic>
#include <mutex>
#include <stdexcept>
#include <unordered_map>

namespace capnp_lean_rpc_payload_ref {

namespace {

struct RuntimePayloadRefEntry {
  uint64_t runtimeId;
  lean_object* messageBytes;
  lean_object* capBytes;
};

std::mutex gRuntimePayloadRefsMutex;
std::unordered_map<uint32_t, RuntimePayloadRefEntry> gRuntimePayloadRefs;
std::atomic<uint32_t> gNextRuntimePayloadRefId{1};

uint32_t allocateRuntimePayloadRefIdLocked() {
  auto id = gNextRuntimePayloadRefId.fetch_add(1, std::memory_order_relaxed);
  while (id == 0 || gRuntimePayloadRefs.find(id) != gRuntimePayloadRefs.end()) {
    id = gNextRuntimePayloadRefId.fetch_add(1, std::memory_order_relaxed);
  }
  return id;
}

}  // namespace

uint32_t registerRuntimePayloadRef(uint64_t runtimeId, lean_object* messageBytes,
                                   lean_object* capBytes, bool retainInputs) {
  if (messageBytes == nullptr || capBytes == nullptr) {
    throw std::runtime_error("runtime payload ref requires message and cap byte arrays");
  }
  lean_mark_mt(messageBytes);
  lean_mark_mt(capBytes);
  if (retainInputs) {
    lean_inc(messageBytes);
    lean_inc(capBytes);
  }

  std::lock_guard<std::mutex> lock(gRuntimePayloadRefsMutex);
  uint32_t id = allocateRuntimePayloadRefIdLocked();
  gRuntimePayloadRefs.emplace(id, RuntimePayloadRefEntry{runtimeId, messageBytes, capBytes});
  return id;
}

uint32_t registerRuntimePayloadRefFromRawCallResult(
    uint64_t runtimeId, const capnp_lean_rpc::RawCallResult& result) {
  auto messageBytes = capnp_lean_rpc::mkByteArrayCopy(result.responseData(), result.responseSize());
  auto capBytes =
      capnp_lean_rpc::mkByteArrayCopy(result.responseCaps.data(), result.responseCaps.size());
  return registerRuntimePayloadRef(runtimeId, messageBytes, capBytes, false);
}

kj::Maybe<RetainedRuntimePayloadRef> retainRuntimePayloadRef(uint32_t payloadRefId) {
  std::lock_guard<std::mutex> lock(gRuntimePayloadRefsMutex);
  auto it = gRuntimePayloadRefs.find(payloadRefId);
  if (it == gRuntimePayloadRefs.end()) {
    return kj::none;
  }
  lean_inc(it->second.messageBytes);
  lean_inc(it->second.capBytes);
  return RetainedRuntimePayloadRef{it->second.runtimeId, it->second.messageBytes,
                                   it->second.capBytes};
}

bool releaseRuntimePayloadRef(uint64_t runtimeId, uint32_t payloadRefId, std::string& errorOut) {
  std::lock_guard<std::mutex> lock(gRuntimePayloadRefsMutex);
  auto it = gRuntimePayloadRefs.find(payloadRefId);
  if (it == gRuntimePayloadRefs.end()) {
    errorOut = "unknown runtime payload ref id";
    return false;
  }
  if (it->second.runtimeId != runtimeId) {
    errorOut = "runtime payload ref belongs to a different Capnp.Rpc runtime";
    return false;
  }
  lean_dec(it->second.messageBytes);
  lean_dec(it->second.capBytes);
  gRuntimePayloadRefs.erase(it);
  return true;
}

uint64_t releaseRuntimePayloadRefsForRuntime(uint64_t runtimeId) {
  std::lock_guard<std::mutex> lock(gRuntimePayloadRefsMutex);
  uint64_t released = 0;
  for (auto it = gRuntimePayloadRefs.begin(); it != gRuntimePayloadRefs.end();) {
    if (it->second.runtimeId == runtimeId) {
      lean_dec(it->second.messageBytes);
      lean_dec(it->second.capBytes);
      it = gRuntimePayloadRefs.erase(it);
      released += 1;
    } else {
      ++it;
    }
  }
  return released;
}

}  // namespace capnp_lean_rpc_payload_ref
