#include "process.hpp"

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <iostream>
#include <stdexcept>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {

std::atomic<unsigned long> next_shm_index{1};

} // namespace

Process::Process(
    const std::string& binary_path,
    const std::string& output_file_path
)
    : pid_(-1), is_closed_(false) {
    unsigned long index = next_shm_index.fetch_add(1, std::memory_order_relaxed);
    std::string shm_name = "/os_lab4_" + std::to_string(getpid()) + "_" + std::to_string(index);

    if (!channel_.create(shm_name)) {
        throw std::runtime_error("Failed to create shared memory channel: " + shm_name);
    }

    int file_fd = open(
        output_file_path.c_str(),
        O_WRONLY | O_CREAT | O_TRUNC,
        0644
    );

    if (file_fd == -1) {
        perror("open");
        channel_.close();
        throw std::runtime_error("Failed to open output file: " + output_file_path);
    }

    std::cout.flush();
    std::cerr.flush();

    pid_t fork_rc = fork();

    if (fork_rc == -1) {
        perror("fork");
        if (close(file_fd) == -1) {
            perror("close file_fd");
        }
        channel_.close();
        throw std::runtime_error("Failed to fork");
    } else if (fork_rc == 0) {
        if (file_fd != STDOUT_FILENO) {
            if (dup2(file_fd, STDOUT_FILENO) == -1) {
                perror("dup2 stdout");
                _exit(EXIT_FAILURE);
            }

            if (close(file_fd) == -1) {
                perror("close file_fd");
                _exit(EXIT_FAILURE);
            }
        }

        execl(
            binary_path.c_str(),
            binary_path.c_str(),
            shm_name.c_str(),
            static_cast<char*>(nullptr)
        );

        perror("execl");
        _exit(127);
    } else {
        pid_ = fork_rc;
        if (close(file_fd) == -1) {
            perror("close file_fd");
        }
    }
}

void Process::send_line(const std::string& line) {
    if (is_closed_ || !channel_.is_open()) {
        throw std::runtime_error("Attempted to write to a closed or invalid channel");
    }

    if (!channel_.send_line(line, pid_)) {
        is_closed_ = true;
        throw std::runtime_error("Failed to send line via shared memory");
    }
}

void Process::close_input() {
    if (!is_closed_ && channel_.is_open()) {
        is_closed_ = true;
        if (!channel_.send_eof(pid_)) {
            std::cerr << "Warning: failed to send EOF to child (PID: " << pid_ << ")\n";
        }
    }
}

void Process::wait() {
    if (pid_ > 0) {
        if (!is_closed_) {
            close_input();
        }

        int status = 0;
        pid_t waited_pid = -1;
        while ((waited_pid = waitpid(pid_, &status, 0)) == -1) {
            if (errno == EINTR) {
                continue;
            }
            perror("waitpid");
            break;
        }

        if (waited_pid == pid_) {
            if (WIFEXITED(status) && WEXITSTATUS(status) != 0) {
                std::cerr << "Child process (PID: " << pid_
                          << ") exited with non-zero status: " << WEXITSTATUS(status) << '\n';
            } else if (WIFSIGNALED(status)) {
                std::cerr << "Child process (PID: " << pid_
                          << ") terminated by signal: " << WTERMSIG(status) << '\n';
            }
        }

        pid_ = -1;
    }

    channel_.close();
}

Process::~Process() {
    close_input();
    wait();
}

unsigned long Process::get_pid() const {
    return (pid_ > 0) ? static_cast<unsigned long>(pid_) : 0UL;
}
