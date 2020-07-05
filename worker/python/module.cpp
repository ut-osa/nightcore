#define __FAAS_PYTHON_BINDING_SRC
#include "base/logging.h"
#include "utils/env_variables.h"
#include "worker/event_driven_worker.h"

#include <pybind11/pybind11.h>
namespace py = pybind11;

namespace faas {
namespace python {

namespace {
static std::span<const char> py_bytes_to_span(py::bytes obj) {
    DCHECK(PyBytes_CheckExact(obj.ptr()));
    return std::span<const char>(
        PyBytes_AsString(obj.ptr()), PyBytes_Size(obj.ptr()));
}

static py::bytes span_to_py_bytes(std::span<const char> s) {
    return py::bytes(s.data(), s.size());
}

static py::str string_view_to_py_str(std::string_view s) {
    return py::str(s.data(), s.size());
}
}

void InitModule(py::module& m) {
    logging::Init(utils::GetEnvVariableAsInt("FAAS_VLOG_LEVEL", 0));

    auto clz = py::class_<worker_lib::EventDrivenWorker>(m, "Worker");

    clz.def(py::init([] () {
        return std::make_unique<worker_lib::EventDrivenWorker>();
    }));
    clz.def("start", [] (worker_lib::EventDrivenWorker* self) {
        self->Start();
    });

    clz.def_property_readonly("is_grpc_service", [] (worker_lib::EventDrivenWorker* self) {
        return self->is_grpc_service();
    });
    clz.def_property_readonly("func_name", [] (worker_lib::EventDrivenWorker* self) {
        return self->func_name();
    });
    clz.def_property_readonly("grpc_service_name", [] (worker_lib::EventDrivenWorker* self) {
        return self->grpc_service_name();
    });

    clz.def("set_watch_fd_readable_callback", [] (worker_lib::EventDrivenWorker* self,
                                                  py::function callback) {
        self->SetWatchFdReadableCallback([callback] (int fd) {
            callback(py::int_(fd));
        });
    });
    clz.def("set_stop_watch_fd_callback", [] (worker_lib::EventDrivenWorker* self,
                                              py::function callback) {
        self->SetStopWatchFdCallback([callback] (int fd) {
            callback(py::int_(fd));
        });
    });
    clz.def("set_incoming_func_call_callback", [] (worker_lib::EventDrivenWorker* self,
                                                   py::function callback) {
        self->SetIncomingFuncCallCallback([callback] (int64_t handle, std::string_view method,
                                                      std::span<const char> input) {
            callback(py::int_(handle), string_view_to_py_str(method), span_to_py_bytes(input));
        });
    });
    clz.def("set_outgoing_func_call_complete_callback", [] (worker_lib::EventDrivenWorker* self,
                                                            py::function callback) {
        self->SetOutgoingFuncCallCompleteCallback([callback] (int64_t handle, bool success,
                                                              std::span<const char> output) {
            callback(py::int_(handle), py::bool_(success), span_to_py_bytes(output));
        });
    });

    clz.def("on_fd_readable", [] (worker_lib::EventDrivenWorker* self, int fd) {
        self->OnFdReadable(fd);
    });

    clz.def("on_func_execution_finished", [] (worker_lib::EventDrivenWorker* self, int64_t handle,
                                              bool success, py::bytes output) {
        self->OnFuncExecutionFinished(handle, success, py_bytes_to_span(output));
    });

    clz.def("new_outgoing_func_call", [] (worker_lib::EventDrivenWorker* self, int64_t parent_handle,
                                          std::string func_name, py::bytes input) -> py::object {
        int64_t handle;
        bool ret = self->NewOutgoingFuncCall(parent_handle, func_name,
                                             py_bytes_to_span(input), &handle);
        if (ret) {
            return py::int_(handle);
        } else {
            return py::none();
        }
    });

    clz.def("new_outgoing_grpc_call", [] (worker_lib::EventDrivenWorker* self, int64_t parent_handle,
                                          std::string service, std::string method,
                                          py::bytes request) -> py::object {
        int64_t handle;
        bool ret = self->NewOutgoingGrpcCall(parent_handle, service, method,
                                             py_bytes_to_span(request), &handle);
        if (ret) {
            return py::int_(handle);
        } else {
            return py::none();
        }
    });
}

}  // namespace python
}  // namespace faas

PYBIND11_MODULE(_faas_native, m) {
    faas::python::InitModule(m);
}
