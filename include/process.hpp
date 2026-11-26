#pragma once

#include "shm_channel.hpp"

#include <string>
#include <sys/types.h>

using proc_handle_t = pid_t;

class Process {
public:
    Process(
        const std::string& binary_path,
        const std::string& output_file_path
    );

    ~Process();

    Process(const Process&) = delete;
    Process& operator=(const Process&) = delete;

    void send_line(const std::string& line);
    void close_input();
    void wait();

    unsigned long get_pid() const;

private:
    ShmChannel channel_;
    proc_handle_t pid_;
    bool is_closed_;
};
