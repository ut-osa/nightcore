#define __FAAS_PYTHON_BINDING_SRC
#include "worker/lib/manager.h"

#include <pybind11/pybind11.h>
namespace py = pybind11;

namespace faas {
namespace python {

namespace {
std::span<const char> py_bytes_to_span(py::bytes obj) {
    DCHECK(PyBytes_CheckExact(obj.ptr()));
    return std::span<const char>(
        PyBytes_AsString(obj.ptr()), PyBytes_Size(obj.ptr()));
}

py::bytes span_to_py_bytes(std::span<const char> s) {
    return py::bytes(s.data(), s.size());
}

py::str string_view_to_py_str(std::string_view s) {
    return py::str(s.data(), s.size());
}
}

void InitModule(py::module& m) {
    auto clz = py::class_<worker_lib::Manager>(m, "WorkerManager");

    clz.def(py::init([] () { return std::make_unique<worker_lib::Manager>(); }));
    clz.def("start", [] (worker_lib::Manager* self) {
        self->Start();
    });
    clz.def("on_gateway_io_error", [] (worker_lib::Manager* self, py::object obj) {
        if (PyLong_CheckExact(obj.ptr())) {
            self->OnGatewayIOError(obj.cast<int>());
        } else if (PyUnicode_CheckExact(obj.ptr())) {
            self->OnGatewayIOError(obj.cast<std::string>());
        } else {
            throw py::type_error(
                "on_gateway_io_error only accepts 'int' or 'str' input");
        }
    });
    clz.def("on_watchdog_io_error", [] (worker_lib::Manager* self, py::object obj){
        if (PyLong_CheckExact(obj.ptr())) {
            self->OnWatchdogIOError(obj.cast<int>());
        } else if (PyUnicode_CheckExact(obj.ptr())) {
            self->OnWatchdogIOError(obj.cast<std::string>());
        } else {
            throw py::type_error(
                "on_watchdog_io_error only accepts 'int' or 'str' input");
        }
    });

    clz.def_property_readonly("is_grpc_service", [] (worker_lib::Manager* self) {
        return self->is_grpc_service();
    });
    clz.def_property_readonly("watchdog_input_pipe_fd", [] (worker_lib::Manager* self) {
        return self->watchdog_input_pipe_fd();
    });
    clz.def_property_readonly("watchdog_output_pipe_fd", [] (worker_lib::Manager* self) {
        return self->watchdog_output_pipe_fd();
    });
    clz.def_property_readonly("gateway_ipc_path", [] (worker_lib::Manager* self) {
        return self->gateway_ipc_path();
    });

    clz.def("set_send_gateway_data_callback", [] (worker_lib::Manager* self, py::function callback) {
        self->SetSendGatewayDataCallback([callback] (std::span<const char> data) {
            callback(span_to_py_bytes(data));
        });
    });
    clz.def("set_send_watchdog_data_callback", [] (worker_lib::Manager* self, py::function callback) {
        self->SetSendWatchdogDataCallback([callback] (std::span<const char> data) {
            callback(span_to_py_bytes(data));
        });
    });
    clz.def("set_incoming_func_call_callback", [] (worker_lib::Manager* self, py::function callback) {
        self->SetIncomingFuncCallCallback([callback] (uint32_t handle, std::span<const char> input) {
            callback(py::int_(handle), span_to_py_bytes(input));
        });
    });
    clz.def("set_incoming_grpc_call_callback", [] (worker_lib::Manager* self, py::function callback) {
        self->SetIncomingGrpcCallCallback([callback] (uint32_t handle, std::string_view method,
                                                      std::span<const char> request) {
            callback(py::int_(handle), string_view_to_py_str(method), span_to_py_bytes(request));
        });
    });
    clz.def("set_outcoming_func_call_complete_callback", [] (worker_lib::Manager* self,
                                                             py::function callback) {
        self->SetOutcomingFuncCallCompleteCallback([callback] (uint32_t handle, bool success,
                                                               std::span<const char> output) {
            callback(py::int_(handle), py::bool_(success), span_to_py_bytes(output));
        });
    });

    clz.def("on_recv_gateway_data", [] (worker_lib::Manager* self, py::bytes data) {
        return self->OnRecvGatewayData(py_bytes_to_span(data));
    });
    clz.def("on_recv_watchdog_data", [] (worker_lib::Manager* self, py::bytes data) {
        return self->OnRecvWatchdogData(py_bytes_to_span(data));
    });
    clz.def("on_outcoming_func_call", [] (worker_lib::Manager* self, std::string func_name,
                                          py::bytes input) -> py::object {
        uint32_t handle;
        bool ret = self->OnOutcomingFuncCall(func_name, py_bytes_to_span(input), &handle);
        if (ret) {
            return py::int_(handle);
        } else {
            return py::none();
        }
    });
    clz.def("on_outcoming_grpc_call", [] (worker_lib::Manager* self, std::string service,
                                          std::string method, py::bytes request) -> py::object {
        uint32_t handle;
        bool ret = self->OnOutcomingGrpcCall(service, method, py_bytes_to_span(request), &handle);
        if (ret) {
            return py::int_(handle);
        } else {
            return py::none();
        }
    });
    clz.def("on_incoming_func_call_complete", [] (worker_lib::Manager* self, uint32_t handle,
                                                  bool success, py::bytes output) {
        self->OnIncomingFuncCallComplete(handle, success, py_bytes_to_span(output));
    });
}

}  // namespace python
}  // namespace faas

PYBIND11_MODULE(_faas_native, m) {
    faas::python::InitModule(m);
}
