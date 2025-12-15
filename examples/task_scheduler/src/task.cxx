
#include "../include/task.hxx"
#include <string>
#include <unordered_map>

auto UUID::generate() -> UUID {
    UUID uuid;
    uuid.bytes = generate_uuid_v4();
    return uuid;
}

auto UUID::to_string() const -> std::string { return bytes_to_hex_string(bytes); }

Task::Task(const std::string& func_name,
           double arrival_time,
           double expected_dur,
           double cpu_cores_usage,
           double memory_req,
           double deadline,
           int priority, TaskStatus status)
    : task_name(std::move(func_name))
    , id(UUID::generate())
    , arrival_time(arrival_time)
    , expected_duration(expected_dur)
    , cpu_cores_usage(cpu_cores_usage)
    , memory_req(memory_req)
    , deadline(deadline)
    , priority(priority),status(status) {}

auto Task::wait_time() const -> double {
    double wait_time = get_current_time() - arrival_time;
    return wait_time;
}

auto Task::expected_end() const -> double {
    double end_time = arrival_time + expected_duration;
    return end_time;
}

auto Task::slack_time() const -> double {
    double slack = (arrival_time + expected_duration) - get_current_time();
    return slack;
}

auto Task::missed_deadline() const -> bool {
    bool missed = (arrival_time + expected_duration) > get_current_time();
    return missed;
}

auto Task::get_id() const -> UUID { return this->id; }

auto Task::get_arrival_time() const -> double { return arrival_time; }

auto Task::get_expected_duration() const -> double { return expected_duration; }

auto Task::get_cpu_cores_usage() const -> double { return cpu_cores_usage; }

auto Task::get_memory_req() const -> double { return memory_req; }

auto Task::get_deadline() const -> double { return deadline; }

auto Task::get_priority() const -> int { return priority; }

auto TaskHash::operator()(const Task& task) const -> std::size_t {
    return std::hash<std::string>{}(task.get_id().to_string());
}

auto Task::get_task_name() const ->std::string{
    return task_name;
}

auto Task::get_status() const -> TaskStatus {
    return status;
}

auto Task::mark_assigned() -> void{
    status=ASSIGNED;
}
auto Task::mark_completed() -> void{
    status=COMPLETED;
}