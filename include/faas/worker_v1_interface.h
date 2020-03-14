#ifndef _FAAS_WORKER_V1_INTERFACE_H_
#define _FAAS_WORKER_V1_INTERFACE_H_

#include <stddef.h>

#ifdef __FAAS_SRC
    #define API_EXPORT
#else
    #define API_EXPORT __attribute__ ((visibility ("default")))
#endif

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

// ================== INTERFACE START ==================

typedef void (*faas_append_output_fn_t)(
    void* caller_context, const char* data, size_t length);

// Return 0 on success.
typedef int (*faas_invoke_func_fn_t)(
    void* caller_context, const char* func_name,
    const char* input_data, size_t input_length,
    const char** output_data, size_t* output_length);

// Below are APIs that function library must implement.
// For all APIs, return 0 on success.

// Initialize function library, will be called once after loading
// the dynamic library.
API_EXPORT int faas_init();
// Create a new function worker.
// When calling `invoke_func_fn` and `append_output_fn`, caller_context
// received in `faas_create_func_worker` should be passed unchanged.
API_EXPORT int faas_create_func_worker(
    void* caller_context,
    faas_invoke_func_fn_t invoke_func_fn,
    faas_append_output_fn_t append_output_fn,
    void** worker_handle);
// Destroy a function worker.
API_EXPORT int faas_destroy_func_worker(void* worker_handle);
// Execute the function. `append_output_fn` can be called multiple
// times to append new data to the output buffer. invoke_func_fn
// can be used to invoke other functions in the system.
// For the same worker_handle, faas_func_call will never be called
// concurrently from different threads, i.e. the implementation
// does not need to be thread-safe for a single function worker.
API_EXPORT int faas_func_call(
    void* worker_handle,
    const char* input, size_t input_length);

// =================== INTERFACE END ===================

#ifdef __cplusplus
}
#endif  // __cplusplus

#ifdef __FAAS_SRC
#ifdef __cplusplus

typedef decltype(faas_init)*                 faas_init_fn_t;
typedef decltype(faas_create_func_worker)*   faas_create_func_worker_fn_t;
typedef decltype(faas_destroy_func_worker)*  faas_destroy_func_worker_fn_t;
typedef decltype(faas_func_call)*            faas_func_call_fn_t;

#endif  // __cplusplus
#endif  // __FAAS_SRC

#undef API_EXPORT

#endif  // _FAAS_WORKER_V1_INTERFACE_H_
