#include "base/init.h"
#include "base/asm.h"
#include "base/common.h"
#include "common/time.h"
#include "common/stat.h"
#include "utils/perf_event.h"
#include "utils/env_variables.h"
#include "ipc/spsc_queue.h"

#include <sched.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/eventfd.h>

#include <absl/flags/flag.h>

ABSL_FLAG(int, server_cpu, -1, "Bind server process to this CPU");
ABSL_FLAG(int, client_cpu, -1, "Bind client process to this CPU");
ABSL_FLAG(size_t, server_queue_sleep_every, 0,
          "Server queue will sleep based on this interval");
ABSL_FLAG(absl::Duration, duration, absl::Seconds(30), "Duration to run");
ABSL_FLAG(absl::Duration, stat_duration, absl::Seconds(10),
          "Duration for reporting statistics");

void BindToCpu(int cpu) {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    PCHECK(sched_setaffinity(0, sizeof(set), &set) == 0);
}

std::unique_ptr<faas::utils::PerfEventGroup> SetupPerfEvents(int cpu) {
    auto perf_event_group = std::make_unique<faas::utils::PerfEventGroup>();
    if (cpu != -1) {
        perf_event_group->set_cpu(cpu);
    }
    if (faas::utils::GetEnvVariableAsInt("PERF_EVENT_KERNEL_ONLY", 0)) {
        perf_event_group->set_exclude_user(true);
    } else if (faas::utils::GetEnvVariableAsInt("PERF_EVENT_USER_ONLY", 0)) {
        perf_event_group->set_exclude_kernel(true);
    }
    CHECK(perf_event_group->AddEvent(PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES))
        << "Failed to add PERF_COUNT_HW_CPU_CYCLES event";
    CHECK(perf_event_group->AddEvent(PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS))
        << "Failed to add PERF_COUNT_HW_INSTRUCTIONS event";
    return perf_event_group;
}

void ReadPerfEventValues(std::string_view log_header,
                         faas::utils::PerfEventGroup* perf_event_group,
                         int64_t duration_in_ns) {
    std::vector<uint64_t> values = perf_event_group->ReadValues();
    LOG(INFO) << log_header << "value of PERF_COUNT_HW_CPU_CYCLES: " << values[0];
    LOG(INFO) << log_header << "value of PERF_COUNT_HW_INSTRUCTIONS: " << values[1];
    LOG(INFO) << log_header << "rate of PERF_COUNT_HW_CPU_CYCLES: "
              << (static_cast<double>(values[0]) / duration_in_ns) * 1000 << " per us";
    LOG(INFO) << log_header << "rate of PERF_COUNT_HW_INSTRUCTIONS: "
              << (static_cast<double>(values[1]) / duration_in_ns) * 1000 << " per us";
}

static constexpr size_t kQueueSize = 1024;
static constexpr uint64_t kEventServerQueueCreated = 1;
static constexpr uint64_t kEventClientQueueCreated = 2;
static constexpr uint64_t kEventServerReady = 4;
static constexpr uint64_t kEventWakeupServerQueue = 5;

void Server(int infd, int outfd, absl::Duration duration, absl::Duration stat_duration,
            int cpu, size_t sleep_every) {
    faas::stat::StatisticsCollector<int32_t> msg_delay_stat(
        faas::stat::StatisticsCollector<int32_t>::StandardReportCallback("client_msg_delay"));
    faas::stat::Counter msg_counter(
        faas::stat::Counter::StandardReportCallback("client_msg_counter"));
    msg_delay_stat.set_report_interval_in_ms(
        gsl::narrow_cast<uint32_t>(absl::ToInt64Milliseconds(stat_duration)));
    msg_counter.set_report_interval_in_ms(
        gsl::narrow_cast<uint32_t>(absl::ToInt64Milliseconds(stat_duration)));
    if (cpu != -1) {
        BindToCpu(cpu);
    }
    auto perf_event_group = SetupPerfEvents(cpu);

    auto server_queue = faas::ipc::SPSCQueue<int64_t>::Create("server", kQueueSize);
    PCHECK(eventfd_write(outfd, kEventServerQueueCreated) == 0) << "eventfd_write failed";
    uint64_t event_value;
    PCHECK(eventfd_read(infd, &event_value) == 0) << "eventfd_read failed";
    CHECK_EQ(event_value, kEventClientQueueCreated);
    auto client_queue = faas::ipc::SPSCQueue<int64_t>::Open("client");
    PCHECK(eventfd_write(outfd, kEventServerReady) == 0) << "eventfd_write failed";

    perf_event_group->ResetAndEnable();
    int64_t start_timestamp = faas::GetMonotonicNanoTimestamp();
    int64_t stop_timestamp = start_timestamp + absl::ToInt64Nanoseconds(duration);
    size_t recv_message_count = 0;
    bool slept = false;
    while (true) {
        int64_t current_timestamp = faas::GetMonotonicNanoTimestamp();
        int64_t send_value = current_timestamp;
        if (current_timestamp >= stop_timestamp) {
            send_value = -1;
        }
        do {
            if (client_queue->Push(send_value)) {
                break;
            }
            faas::asm_volatile_pause();
        } while (true);
        if (current_timestamp >= stop_timestamp) {
            break;
        }
        if (slept) {
            uint64_t event_value;
            PCHECK(eventfd_read(infd, &event_value) == 0) << "eventfd_read failed";
            CHECK_EQ(event_value, kEventWakeupServerQueue);
            slept = false;
        }
        int64_t recv_value;
        do {
            if (server_queue->Pop(&recv_value)) {
                break;
            }
            faas::asm_volatile_pause();
        } while (true);
        msg_counter.Tick();
        current_timestamp = faas::GetMonotonicNanoTimestamp();
        int64_t send_timestamp = recv_value;
        msg_delay_stat.AddSample(gsl::narrow_cast<int32_t>(current_timestamp - send_timestamp));
        recv_message_count++;
        if (sleep_every > 0 && recv_message_count % sleep_every == 0) {
            server_queue->ConsumerEnterSleep();
            slept = true;
        }
    }
    int64_t elapsed_time = faas::GetMonotonicNanoTimestamp() - start_timestamp;
    perf_event_group->Disable();
    ReadPerfEventValues("Server ", perf_event_group.get(), elapsed_time);
    LOG(INFO) << "Server elapsed nanoseconds: " << elapsed_time;
    VLOG(1) << "Close server socket";
    PCHECK(close(infd) == 0);
    PCHECK(close(outfd) == 0);
}

void Client(int infd, int outfd, absl::Duration stat_duration, int cpu) {
    faas::stat::StatisticsCollector<int32_t> msg_delay_stat(
        faas::stat::StatisticsCollector<int32_t>::StandardReportCallback("server_msg_delay"));
    faas::stat::Counter msg_counter(
        faas::stat::Counter::StandardReportCallback("server_msg_counter"));
    msg_delay_stat.set_report_interval_in_ms(
        gsl::narrow_cast<uint32_t>(absl::ToInt64Milliseconds(stat_duration)));
    msg_counter.set_report_interval_in_ms(
        gsl::narrow_cast<uint32_t>(absl::ToInt64Milliseconds(stat_duration)));
    if (cpu != -1) {
        BindToCpu(cpu);
    }
    auto perf_event_group = SetupPerfEvents(cpu);

    uint64_t event_value;
    PCHECK(eventfd_read(infd, &event_value) == 0) << "eventfd_read failed";
    CHECK_EQ(event_value, kEventServerQueueCreated);
    auto client_queue = faas::ipc::SPSCQueue<int64_t>::Create("client", kQueueSize);
    auto server_queue = faas::ipc::SPSCQueue<int64_t>::Open("server");
    server_queue->SetWakeupConsumerFn([outfd] () {
        PCHECK(eventfd_write(outfd, kEventWakeupServerQueue) == 0) << "eventfd_write failed";
    });
    PCHECK(eventfd_write(outfd, kEventClientQueueCreated) == 0) << "eventfd_write failed";
    PCHECK(eventfd_read(infd, &event_value) == 0) << "eventfd_read failed";
    CHECK_EQ(event_value, kEventServerReady);

    perf_event_group->ResetAndEnable();
    int64_t start_timestamp = faas::GetMonotonicNanoTimestamp();
    while (true) {
        int64_t recv_value;
        do {
            if (client_queue->Pop(&recv_value)) {
                break;
            }
            faas::asm_volatile_pause();
        } while (true);
        if (recv_value == -1) {
            break;
        }
        msg_counter.Tick();
        int64_t current_timestamp = faas::GetMonotonicNanoTimestamp();
        int64_t send_timestamp = static_cast<int64_t>(recv_value);
        msg_delay_stat.AddSample(gsl::narrow_cast<int32_t>(current_timestamp - send_timestamp));
        int64_t send_value = current_timestamp;
        do {
            if (server_queue->Push(send_value)) {
                break;
            }
            faas::asm_volatile_pause();
        } while (true);
    }
    int64_t elapsed_time = faas::GetMonotonicNanoTimestamp() - start_timestamp;
    perf_event_group->Disable();
    ReadPerfEventValues("Client ", perf_event_group.get(), elapsed_time);
    LOG(INFO) << "Client elapsed nanoseconds: " << elapsed_time;
    PCHECK(close(infd) == 0);
    PCHECK(close(outfd) == 0);
}

int main(int argc, char* argv[]) {
    faas::base::InitMain(argc, argv);

    int fd1 = eventfd(0, 0);
    PCHECK(fd1 != -1);
    int fd2 = eventfd(0, 0);
    PCHECK(fd2 != -1);

    pid_t child_pid = fork();
    if (child_pid == 0) {
        Client(fd1, fd2, absl::GetFlag(FLAGS_stat_duration), absl::GetFlag(FLAGS_client_cpu));
        return 0;
    }

    PCHECK(child_pid != -1);
    Server(fd2, fd1, absl::GetFlag(FLAGS_duration),
           absl::GetFlag(FLAGS_stat_duration), absl::GetFlag(FLAGS_server_cpu),
           absl::GetFlag(FLAGS_server_queue_sleep_every));

    int wstatus;
    CHECK(wait(&wstatus) == child_pid);

    return 0;
}
