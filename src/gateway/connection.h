#pragma once

#include "base/common.h"
#include "utils/uv_utils.h"
#include "utils/appendable_buffer.h"

#include <http_parser.h>

namespace faas {
namespace gateway {

class Server;
class IOWorker;
class AsyncRequestContext;

class Connection {
public:
    static constexpr const char* kDefaultContentType = "text/plain";

    Connection(Server* server, int connection_id);
    ~Connection();

    int id() const { return connection_id_; }
    uv_tcp_t* uv_tcp_handle() { return &uv_tcp_handle_; }

    void Start(IOWorker* io_worker);
    void Reset(int connection_id);

    // Must be called from uv_tcp_handle_->loop
    void ScheduleClose();

    // Only used for transferring connection from Server to IOWorker
    uv_tcp_t* uv_tcp_handle_for_transfer() { return &uv_tcp_handle_for_transfer_; }
    uv_write_t* uv_write_req_for_transfer() { return &uv_write_req_for_transfer_; }
    uv_write_t* uv_write_req_for_back_transfer() { return &uv_write_req_for_back_transfer_; }
    char* pipe_write_buf_for_transfer() { return pipe_write_buf_for_transfer_; }

private:
    enum State { kCreated, kRunning, kClosing, kClosed };

    Server* server_;
    int connection_id_;
    IOWorker* io_worker_;
    uv_tcp_t uv_tcp_handle_;
    State state_;
    int closed_uv_handles_;
    int uv_handles_is_closing_;

    uv_tcp_t uv_tcp_handle_for_transfer_;
    uv_write_t uv_write_req_for_transfer_;
    uv_write_t uv_write_req_for_back_transfer_;
    char pipe_write_buf_for_transfer_[sizeof(void*)];

    std::string log_header_;

    http_parser_settings http_parser_settings_;
    http_parser http_parser_;
    int header_field_value_flag_;

    // For request
    utils::AppendableBuffer url_buffer_;
    utils::AppendableBuffer header_field_buffer_;
    utils::AppendableBuffer header_value_buffer_;
    utils::AppendableBuffer body_buffer_;
    size_t header_value_buffer_pos_;
    absl::flat_hash_map<std::string, const char*> headers_;

    // For response
    utils::AppendableBuffer response_header_buffer_;
    utils::AppendableBuffer response_body_buffer_;
    int response_status_;
    std::string response_content_type_;
    uv_write_t response_write_req_;
    bool within_async_request_;
    std::shared_ptr<AsyncRequestContext> async_request_context_;
    uv_async_t async_request_finished_event_;

    friend class AsyncRequestContext;

    void StartRecvData();
    void StopRecvData();

    DECLARE_UV_READ_CB_FOR_CLASS(RecvData);
    DECLARE_UV_WRITE_CB_FOR_CLASS(DataWritten);
    DECLARE_UV_ALLOC_CB_FOR_CLASS(BufferAlloc);
    DECLARE_UV_ASYNC_CB_FOR_CLASS(AsyncRequestFinish);
    DECLARE_UV_CLOSE_CB_FOR_CLASS(Close);

    void HttpParserOnMessageBegin();
    void HttpParserOnUrl(const char* data, size_t length);
    void HttpParserOnHeaderField(const char* data, size_t length);
    void HttpParserOnHeaderValue(const char* data, size_t length);
    void HttpParserOnHeadersComplete();
    void HttpParserOnBody(const char* data, size_t length);
    void HttpParserOnMessageComplete();

    void HttpParserOnNewHeader();
    void ResetHttpParser();
    void OnNewHttpRequest(const std::string& method, const std::string& path,
                          const char* body, size_t body_length);
    void AsyncRequestFinish(AsyncRequestContext* context);
    void SendHttpResponse();

    static int HttpParserOnMessageBeginCallback(http_parser* http_parser);
    static int HttpParserOnUrlCallback(http_parser* http_parser, const char* data, size_t length);
    static int HttpParserOnHeaderFieldCallback(http_parser* http_parser, const char* data, size_t length);
    static int HttpParserOnHeaderValueCallback(http_parser* http_parser, const char* data, size_t length);
    static int HttpParserOnHeadersCompleteCallback(http_parser* http_parser);
    static int HttpParserOnBodyCallback(http_parser* http_parser, const char* data, size_t length);
    static int HttpParserOnMessageCompleteCallback(http_parser* http_parser);

    DISALLOW_COPY_AND_ASSIGN(Connection);
};

}  // namespace gateway
}  // namespace faas
