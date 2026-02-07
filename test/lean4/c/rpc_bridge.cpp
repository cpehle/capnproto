#include <lean/lean.h>

#include <capnp/any.h>
#include <capnp/serialize.h>
#include <kj/io.h>

#include <cstring>
#include <exception>

namespace {

lean_obj_res mkByteArrayCopy(const uint8_t* data, size_t size) {
  lean_object* out = lean_alloc_sarray(1, size, size);
  std::memcpy(lean_sarray_cptr(out), data, size);
  lean_sarray_set_size(out, size);
  return out;
}

lean_obj_res mkIoUserError(const char* message) {
  lean_object* msg = lean_mk_string(message);
  lean_object* err = lean_mk_io_user_error(msg);
  return lean_io_result_mk_error(err);
}

}  // namespace

extern "C" LEAN_EXPORT lean_obj_res capnp_lean_rpc_raw_call(
    uint32_t target, uint64_t interfaceId, uint16_t methodId, b_lean_obj_arg request) {
  (void)target;
  (void)interfaceId;
  (void)methodId;

  try {
    const auto size = lean_sarray_size(request);
    const auto* reqData = lean_sarray_cptr(const_cast<lean_object*>(request));

    if (size == 0) {
      return lean_io_result_mk_ok(mkByteArrayCopy(reqData, size));
    }

    // Parse as a standard (unpacked) Cap'n Proto stream message to validate wire format.
    kj::ArrayPtr<const kj::byte> reqBytes(reinterpret_cast<const kj::byte*>(reqData), size);
    kj::ArrayInputStream input(reqBytes);
    capnp::ReaderOptions options;
    options.traversalLimitInWords = 1ull << 30;
    capnp::InputStreamMessageReader reader(input, options);
    auto root = reader.getRoot<capnp::AnyPointer>();
    (void)root;

    // Current bridge behavior is pass-through: return the original bytes after validation.
    return lean_io_result_mk_ok(mkByteArrayCopy(reqData, size));
  } catch (const kj::Exception& e) {
    return mkIoUserError(e.getDescription().cStr());
  } catch (const std::exception& e) {
    return mkIoUserError(e.what());
  } catch (...) {
    return mkIoUserError("unknown exception in capnp_lean_rpc_raw_call");
  }
}
