#include "launcher/func_process.h"

#include "common/time.h"
#include "utils/fs.h"
#include "ipc/base.h"
#include "launcher/launcher.h"

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

namespace faas {
namespace launcher {

FuncProcess::FuncProcess(Launcher* launcher, int id, uint16_t initial_client_id)
    : state_(kCreated), launcher_(launcher), id_(id),
      initial_client_id_(initial_client_id),
      log_header_(absl::StrFormat("FuncProcess[%d]: ", id)),
      subprocess_(launcher->fprocess()) {}

FuncProcess::~FuncProcess() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

bool FuncProcess::Start(uv_loop_t* uv_loop, utils::BufferPool* read_buffer_pool) {
    DCHECK(state_ == kCreated);
    uv_loop_ = uv_loop;
    read_buffer_pool_ = read_buffer_pool;
    subprocess_.AddEnvVariable("FAAS_FUNC_ID", launcher_->func_id());
    subprocess_.AddEnvVariable("FAAS_FPROCESS_ID", id_);
    subprocess_.AddEnvVariable("FAAS_CLIENT_ID", initial_client_id_);
    subprocess_.AddEnvVariable("FAAS_ROOT_PATH_FOR_IPC", ipc::GetRootPathForIpc());
    if (!launcher_->fprocess_output_dir().empty()) {
        std::string_view func_name = launcher_->func_name();
        subprocess_.SetStandardFile(
            uv::Subprocess::kStdout,
            fs_utils::JoinPath(launcher_->fprocess_output_dir(), 
                               absl::StrFormat("%s_worker_%d.stdout", func_name, id_)));
        subprocess_.SetStandardFile(
            uv::Subprocess::kStderr,
            fs_utils::JoinPath(launcher_->fprocess_output_dir(), 
                               absl::StrFormat("%s_worker_%d.stderr", func_name, id_)));
    }
    if (!launcher_->fprocess_working_dir().empty()) {
        subprocess_.SetWorkingDir(launcher_->fprocess_working_dir());
    }
    if (launcher_->fprocess_multi_worker_mode()) {
        HLOG(FATAL) << "Multi worker mode not implemented yet";
    }
    if (!subprocess_.Start(uv_loop, read_buffer_pool,
                           absl::bind_front(&FuncProcess::OnSubprocessExit, this))) {
        return false;
    }
    state_ = kRunning;
    return true;
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

}  // namespace launcher
}  // namespace faas
