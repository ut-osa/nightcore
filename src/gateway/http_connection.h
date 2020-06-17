#pragma once

#include "base/common.h"
#include "common/uv.h"
#include "utils/appendable_buffer.h"
#include "gateway/connection.h"

#include <http_parser.h>

namespace faas {
namespace gateway {

class Server;
class IOWorker;
class HttpAsyncRequestContext;

class HttpConnection final : public Connection {
public:
    static constexpr const char* kDefaultContentType = "text/plain";

    HttpConnection(Server* server, int connection_id);
    ~HttpConnection();

    int id() const { return connection_id_; }

    uv_stream_t* InitUVHandle(uv_loop_t* uv_loop) override;
    void Start(IOWorker* io_worker) override;
    void ScheduleClose() override;

private:
    enum State { kCreated, kRunning, kClosing, kClosed };

    int connection_id_;
    IOWorker* io_worker_;
    uv_tcp_t uv_tcp_handle_;
    State state_;

    std::string log_header_;

    http_parser_settings http_parser_settings_;
    http_parser http_parser_;
    int header_field_value_flag_;

    // For request
    utils::AppendableBuffer url_buffer_;
    utils::AppendableBuffer header_field_buffer_;
    utils::AppendableBuffer header_value_buffer_;
    utils::AppendableBuffer body_buffer_;
    size_t header_field_buffer_pos_;
    size_t header_value_buffer_pos_;
    absl::flat_hash_map<std::string_view, std::string_view> headers_;

    // For response
    utils::AppendableBuffer response_header_buffer_;
    utils::AppendableBuffer response_body_buffer_;
    int response_status_;
    std::string response_content_type_;
    uv_write_t response_write_req_;
    bool within_async_request_;
    std::shared_ptr<HttpAsyncRequestContext> async_request_context_;

    friend class HttpAsyncRequestContext;

    void StartRecvData();
    void StopRecvData();

    DECLARE_UV_READ_CB_FOR_CLASS(RecvData);
    DECLARE_UV_WRITE_CB_FOR_CLASS(DataWritten);
    DECLARE_UV_ALLOC_CB_FOR_CLASS(BufferAlloc);
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
    void OnNewHttpRequest(std::string_view method, std::string_view path);
    void OnAsyncRequestFinish();
    void AsyncRequestFinish(HttpAsyncRequestContext* context);
    void SendHttpResponse();

    static int HttpParserOnMessageBeginCallback(http_parser* http_parser);
    static int HttpParserOnUrlCallback(http_parser* http_parser, const char* data, size_t length);
    static int HttpParserOnHeaderFieldCallback(http_parser* http_parser, const char* data, size_t length);
    static int HttpParserOnHeaderValueCallback(http_parser* http_parser, const char* data, size_t length);
    static int HttpParserOnHeadersCompleteCallback(http_parser* http_parser);
    static int HttpParserOnBodyCallback(http_parser* http_parser, const char* data, size_t length);
    static int HttpParserOnMessageCompleteCallback(http_parser* http_parser);

    DISALLOW_COPY_AND_ASSIGN(HttpConnection);
};

}  // namespace gateway
}  // namespace faas
