#pragma once

#include "rpc_bridge_common.h"

#include <string>

namespace capnp_lean_rpc_payload_ref {

struct RetainedRuntimePayloadRef {
  uint64_t runtimeId = 0;
  lean_object* messageBytes = nullptr;
  lean_object* capBytes = nullptr;

  RetainedRuntimePayloadRef() = default;

  RetainedRuntimePayloadRef(uint64_t runtimeId, lean_object* messageBytes,
                            lean_object* capBytes)
      : runtimeId(runtimeId), messageBytes(messageBytes), capBytes(capBytes) {}

  RetainedRuntimePayloadRef(const RetainedRuntimePayloadRef&) = delete;
  RetainedRuntimePayloadRef& operator=(const RetainedRuntimePayloadRef&) = delete;

  RetainedRuntimePayloadRef(RetainedRuntimePayloadRef&& other) noexcept
      : runtimeId(other.runtimeId),
        messageBytes(other.messageBytes),
        capBytes(other.capBytes) {
    other.messageBytes = nullptr;
    other.capBytes = nullptr;
  }

  RetainedRuntimePayloadRef& operator=(RetainedRuntimePayloadRef&& other) noexcept {
    if (this != &other) {
      reset();
      runtimeId = other.runtimeId;
      messageBytes = other.messageBytes;
      capBytes = other.capBytes;
      other.messageBytes = nullptr;
      other.capBytes = nullptr;
    }
    return *this;
  }

  ~RetainedRuntimePayloadRef() { reset(); }

  void reset() {
    if (messageBytes != nullptr) {
      lean_dec(messageBytes);
      messageBytes = nullptr;
    }
    if (capBytes != nullptr) {
      lean_dec(capBytes);
      capBytes = nullptr;
    }
  }

  lean_object* takeMessageBytes() {
    auto* out = messageBytes;
    messageBytes = nullptr;
    return out;
  }

  lean_object* takeCapBytes() {
    auto* out = capBytes;
    capBytes = nullptr;
    return out;
  }
};

uint32_t registerRuntimePayloadRef(uint64_t runtimeId, lean_object* messageBytes,
                                   lean_object* capBytes, bool retainInputs);
uint32_t registerRuntimePayloadRefFromRawCallResult(
    uint64_t runtimeId, const capnp_lean_rpc::RawCallResult& result);
kj::Maybe<RetainedRuntimePayloadRef> retainRuntimePayloadRef(uint32_t payloadRefId);
bool releaseRuntimePayloadRef(uint64_t runtimeId, uint32_t payloadRefId,
                              std::string& errorOut);
uint64_t releaseRuntimePayloadRefsForRuntime(uint64_t runtimeId);

}  // namespace capnp_lean_rpc_payload_ref
