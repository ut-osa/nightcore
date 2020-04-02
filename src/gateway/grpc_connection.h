#pragma once

#include "base/common.h"
#include "utils/uv_utils.h"
#include "utils/appendable_buffer.h"
#include "utils/object_pool.h"
#include "gateway/connection.h"

#include <nghttp2/nghttp2.h>

namespace faas {
namespace gateway {

class Server;
class IOWorker;

class GrpcConnection final : public Connection {
public:
    static constexpr size_t kH2FrameHeaderByteSize = 9;

    GrpcConnection(Server* server, int connection_id);
    ~GrpcConnection();

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
    int closed_uv_handles_;
    int total_uv_handles_;

    std::string log_header_;

    nghttp2_session* h2_session_;
    bool uv_write_for_mem_send_ongoing_;
    uv_write_t write_req_for_mem_send_;

    class H2StreamContext;
    utils::SimpleObjectPool<H2StreamContext> h2_stream_context_pool_;

    DECLARE_UV_READ_CB_FOR_CLASS(RecvData);
    DECLARE_UV_WRITE_CB_FOR_CLASS(DataWritten);
    DECLARE_UV_ALLOC_CB_FOR_CLASS(BufferAlloc);
    DECLARE_UV_CLOSE_CB_FOR_CLASS(Close);

    void H2SendPendingDataIfNecessary();
    void H2SendSettingsFrame();
    bool ValidateAndPopulateH2Header(H2StreamContext* context, absl::string_view name, absl::string_view value);
    void OnNewGrpcRequest(H2StreamContext* context);
    

    int H2OnFrameRecv(const nghttp2_frame* frame);
    int H2OnStreamClose(int32_t stream_id, uint32_t error_code);
    int H2OnHeader(const nghttp2_frame* frame, absl::string_view name, absl::string_view value, uint8_t flags);
    int H2OnBeginHeaders(const nghttp2_frame* frame);
    int H2OnDataChunkRecv(uint8_t flags, int32_t stream_id, const uint8_t* data, size_t len);
    ssize_t H2DataSourceRead(H2StreamContext* stream_context, uint8_t* buf, size_t length, uint32_t* data_flags);
    int H2SendData(H2StreamContext* stream_context, nghttp2_frame* frame, const uint8_t* framehd, size_t length);

    static int H2OnFrameRecvCallback(nghttp2_session* session, const nghttp2_frame* frame,
                                     void* user_data);
    static int H2OnStreamCloseCallback(nghttp2_session* session, int32_t stream_id,
                                       uint32_t error_code, void* user_data);
    static int H2OnHeaderCallback(nghttp2_session* session, const nghttp2_frame* frame,
                                  const uint8_t* name, size_t namelen,
                                  const uint8_t* value, size_t valuelen,
                                  uint8_t flags, void* user_data);
    static int H2OnBeginHeadersCallback(nghttp2_session* session, const nghttp2_frame* frame,
                                        void* user_data);
    static int H2OnDataChunkRecvCallback(nghttp2_session* session, uint8_t flags, int32_t stream_id,
                                         const uint8_t* data, size_t len, void* user_data);
    static ssize_t H2DataSourceReadCallback(nghttp2_session* session, int32_t stream_id, uint8_t* buf,
                                            size_t length, uint32_t* data_flags, nghttp2_data_source* source,
                                            void* user_data);
    static int H2SendDataCallback(nghttp2_session* session, nghttp2_frame* frame,
                                  const uint8_t* framehd, size_t length, nghttp2_data_source* source,
                                  void* user_data);

    DISALLOW_COPY_AND_ASSIGN(GrpcConnection);
};

}  // namespace gateway
}  // namespace faas
