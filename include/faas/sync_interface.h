#ifndef _FAAS_SYNC_INTERFACE_H_
#define _FAAS_SYNC_INTERFACE_H_

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

typedef void (*fass_append_output_fn)(const char* data, size_t length);

// Below are APIs that function library must implement.
// For all APIs, return 0 on success.

// Initialize function library, will be called once after loading
// the dynamic library.
API_EXPORT int faas_init();
// Create a new function worker.
API_EXPORT int faas_create_func_worker(void** worker_handle);
// Destroy a function worker.
API_EXPORT int faas_destroy_func_worker(void* worker_handle);
// Execute the function. append_output_fn can be called multiple
// times to append new data to the output buffer.
// For the same worker_handle, fass_func_call will never be called
// concurrently from different threads, i.e. the implementation
// does not need to be thread-safe for a single function worker.
API_EXPORT int fass_func_call(
    void* worker_handle,
    const char* input, size_t input_length,
    fass_append_output_fn append_output_fn);

// ================== INTERFACE END ==================

#ifdef __cplusplus
}
#endif  // __cplusplus

#ifdef __FAAS_SRC
#ifdef __cplusplus

typedef decltype(faas_init)*                 faas_init_fn_t;
typedef decltype(faas_create_func_worker)*   faas_create_func_worker_fn_t;
typedef decltype(faas_destroy_func_worker)*  faas_destroy_func_worker_fn_t;
typedef decltype(fass_func_call)*            fass_func_call_fn_t;

#endif  // __cplusplus
#endif  // __FAAS_SRC

#undef API_EXPORT

#endif  // _FAAS_SYNC_INTERFACE_H_
