#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include <faas/worker_v1_interface.h>

struct worker_context {
    void* caller_context;
    faas_invoke_func_fn_t invoke_func_fn;
    faas_append_output_fn_t append_output_fn;
};

int faas_init() {
    return 0;
}

int faas_create_func_worker(void* caller_context, faas_invoke_func_fn_t invoke_func_fn,
                            faas_append_output_fn_t append_output_fn, void** worker_handle) {
    struct worker_context* context = (struct worker_context*)malloc(sizeof(struct worker_context*));
    context->caller_context = caller_context;
    context->invoke_func_fn = invoke_func_fn;
    context->append_output_fn = append_output_fn;
    *worker_handle = context;
    return 0;
}

int faas_destroy_func_worker(void* worker_handle) {
    struct worker_context* context = (struct worker_context*)worker_handle;
    free(context);
    return 0;
}

static const char* kOutputPrefix = "From function Bar: ";

int faas_func_call(void* worker_handle, const char* input, size_t input_length) {
    struct worker_context* context = (struct worker_context*)worker_handle;
    const char* bar_output;
    size_t bar_output_length;
    int ret = context->invoke_func_fn(
        context->caller_context, "Bar", input, input_length,
        &bar_output, &bar_output_length);
    if (ret != 0) {
        return -1;
    }
    char* output_buffer = (char*)malloc(strlen(kOutputPrefix) + bar_output_length);
    memcpy(output_buffer, kOutputPrefix, strlen(kOutputPrefix));
    memcpy(output_buffer + strlen(kOutputPrefix), bar_output, bar_output_length);
    context->append_output_fn(context->caller_context,
                              output_buffer, strlen(kOutputPrefix) + bar_output_length);
    free(output_buffer);
    return 0;
}
