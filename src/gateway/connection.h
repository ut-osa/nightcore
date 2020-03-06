#pragma once

#include "base/common.h"

namespace faas {
namespace gateway {

class Server;
class IOWorker;

class Connection {
public:
    enum class Type { Http, Message };

    Connection(Type type, Server* server) : type_(type), server_(server) {}
    virtual ~Connection() {}

    Type type() const { return type_; }

    virtual uv_stream_t* InitUVHandle(uv_loop_t* uv_loop) = 0;
    virtual void Start(IOWorker* io_worker) = 0;
    virtual void ScheduleClose() = 0;

    // Only used for transferring connection from Server to IOWorker
    uv_write_t* uv_write_req_for_transfer() { return &uv_write_req_for_transfer_; }
    uv_write_t* uv_write_req_for_back_transfer() { return &uv_write_req_for_back_transfer_; }
    char* pipe_write_buf_for_transfer() { return pipe_write_buf_for_transfer_; }

protected:
    Type type_;
    Server* server_;

private:
    uv_write_t uv_write_req_for_transfer_;
    uv_write_t uv_write_req_for_back_transfer_;
    char pipe_write_buf_for_transfer_[sizeof(void*)];

    DISALLOW_COPY_AND_ASSIGN(Connection);
};

}
}
