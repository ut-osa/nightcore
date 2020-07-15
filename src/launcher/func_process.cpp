#include "launcher/func_process.h"

#include "common/time.h"
#include "utils/fs.h"
#include "ipc/base.h"
#include "launcher/launcher.h"

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

namespace faas {
namespace launcher {

FuncProcess::FuncProcess(Launcher* launcher, int id, int initial_client_id)
    : state_(kCreated), launcher_(launcher), id_(id),
      initial_client_id_(initial_client_id),
      log_header_(fmt::format("FuncProcess[{}]: ", id)),
      subprocess_(launcher->fprocess()) {
    message_pipe_fd_ = subprocess_.CreateReadablePipe();
}

FuncProcess::~FuncProcess() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

bool FuncProcess::Start(uv_loop_t* uv_loop, utils::BufferPool* read_buffer_pool) {
    DCHECK(state_ == kCreated);
    uv_loop_ = uv_loop;
    read_buffer_pool_ = read_buffer_pool;
    subprocess_.AddEnvVariable("FAAS_FUNC_ID", launcher_->func_id());
    subprocess_.AddEnvVariable("FAAS_FPROCESS_ID", id_);
    if (initial_client_id_ >= 0) {
        subprocess_.AddEnvVariable("FAAS_CLIENT_ID", initial_client_id_);
    }
    subprocess_.AddEnvVariable("FAAS_MSG_PIPE_FD", message_pipe_fd_);
    subprocess_.AddEnvVariable("FAAS_ROOT_PATH_FOR_IPC", ipc::GetRootPathForIpc());
    if (launcher_->func_worker_use_engine_socket()) {
        subprocess_.AddEnvVariable("FAAS_USE_ENGINE_SOCKET", "1");
    }
    if (launcher_->engine_tcp_port() != -1) {
        subprocess_.AddEnvVariable("FAAS_ENGINE_TCP_PORT", launcher_->engine_tcp_port());
    }
    if (!launcher_->fprocess_output_dir().empty()) {
        std::string_view func_name = launcher_->func_name();
        subprocess_.SetStandardFile(
            uv::Subprocess::kStdout,
            fs_utils::JoinPath(launcher_->fprocess_output_dir(), 
                               fmt::format("{}_worker_{}.stdout", func_name, id_)));
        subprocess_.SetStandardFile(
            uv::Subprocess::kStderr,
            fs_utils::JoinPath(launcher_->fprocess_output_dir(), 
                               fmt::format("{}_worker_{}.stderr", func_name, id_)));
    }
    if (!launcher_->fprocess_working_dir().empty()) {
        subprocess_.SetWorkingDir(launcher_->fprocess_working_dir());
    }
    if (!subprocess_.Start(uv_loop, read_buffer_pool,
                           absl::bind_front(&FuncProcess::OnSubprocessExit, this))) {
        return false;
    }
    message_pipe_ = subprocess_.GetPipe(message_pipe_fd_);
    message_pipe_->data = this;
    std::string_view func_config_json = launcher_->func_config_json();
    initial_payload_size_ = gsl::narrow_cast<uint32_t>(func_config_json.size());
    uv_buf_t bufs[2];
    bufs[0] = {
        .base = reinterpret_cast<char*>(&initial_payload_size_),
        .len = sizeof(uint32_t)
    };
    bufs[1] = {
        .base = const_cast<char*>(func_config_json.data()),
        .len = func_config_json.size()
    };
    uv_write_t* write_req = launcher_->NewWriteRequest();
    write_req->data = nullptr;
    UV_DCHECK_OK(uv_write(write_req, UV_AS_STREAM(message_pipe_),
                          bufs, 2, &FuncProcess::SendMessageCallback));
    state_ = kRunning;
    return true;
}

void FuncProcess::SendMessage(const protocol::Message& message) {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_loop_);
    uv_buf_t buf;
    launcher_->NewWriteBuffer(&buf);
    DCHECK_LE(sizeof(protocol::Message), buf.len);
    memcpy(buf.base, &message, sizeof(protocol::Message));
    buf.len = sizeof(protocol::Message);
    uv_write_t* write_req = launcher_->NewWriteRequest();
    write_req->data = buf.base;
    UV_DCHECK_OK(uv_write(write_req, UV_AS_STREAM(message_pipe_),
                          &buf, 1, &FuncProcess::SendMessageCallback));
}

void FuncProcess::ScheduleClose() {
    DCHECK(state_ != kCreated);
    DCHECK_IN_EVENT_LOOP_THREAD(uv_loop_);
    if (state_ == kClosed || state_ == kClosing) {
        HLOG(WARNING) << "Already scheduled to close or has closed";
        return;
    }
    state_ = kClosing;
    subprocess_.Kill();
}

void FuncProcess::OnSubprocessExit(int exit_status, std::span<const char> stdout,
                                   std::span<const char> stderr) {
    DCHECK(state_ != kCreated);
    DCHECK_IN_EVENT_LOOP_THREAD(uv_loop_);
    if (exit_status != 0) {
        HLOG(WARNING) << "Subprocess exits abnormally with code: " << exit_status;
    }
    if (stderr.size() > 0) {
        HVLOG(1) << "Stderr: " << std::string_view(stderr.data(), stderr.size());
    }
    state_ = kClosed;
    launcher_->OnFuncProcessExit(this);
}

UV_WRITE_CB_FOR_CLASS(FuncProcess, SendMessage) {
    auto reclaim_resource = gsl::finally([this, req] {
        if (req->data != nullptr) {
            launcher_->ReturnWriteBuffer(reinterpret_cast<char*>(req->data));
        }
        launcher_->ReturnWriteRequest(req);
    });
    if (status != 0) {
        HLOG(WARNING) << "Failed to send message, will kill this func process: "
                      << uv_strerror(status);
        ScheduleClose();
    }
}

}  // namespace launcher
}  // namespace faas
