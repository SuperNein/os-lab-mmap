#include "shm_channel.hpp"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <iostream>
#include <utility>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {

bool wait_sem(sem_t* sem) {
    while (sem_wait(sem) == -1) {
        if (errno == EINTR) {
            continue;
        }
        perror("sem_wait");
        return false;
    }
    return true;
}

bool try_wait_sem(sem_t* sem) {
    while (sem_trywait(sem) == -1) {
        if (errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

bool wait_ack(sem_t* ack, pid_t peer_pid) {
    if (peer_pid <= 0) {
        return wait_sem(ack);
    }

    while (true) {
        struct timespec ts {};
        if (clock_gettime(CLOCK_REALTIME, &ts) == -1) {
            perror("clock_gettime");
            return false;
        }

        constexpr long TIMEOUT_NS = 200'000'000L;
        ts.tv_nsec += TIMEOUT_NS;
        if (ts.tv_nsec >= 1'000'000'000L) {
            ts.tv_sec += 1;
            ts.tv_nsec -= 1'000'000'000L;
        }

        if (sem_timedwait(ack, &ts) == 0) {
            return true;
        }

        if (errno == EINTR) {
            continue;
        }

        if (errno == ETIMEDOUT) {
            siginfo_t info {};
            int rc = waitid(
                P_PID,
                static_cast<id_t>(peer_pid),
                &info,
                WEXITED | WNOHANG | WNOWAIT
            );
            if (rc == 0) {
                if (info.si_pid == peer_pid) {
                    if (try_wait_sem(ack)) {
                        return true;
                    }
                    std::cerr << "Child process (PID: " << peer_pid
                              << ") exited unexpectedly while waiting for ack\n";
                    return false;
                }
                continue;
            }
            if (errno == EINTR) {
                continue;
            }
            perror("waitid");
            return false;
        }

        perror("sem_timedwait");
        return false;
    }
}

} // namespace

ShmChannel::~ShmChannel() {
    close();
}

ShmChannel::ShmChannel(ShmChannel&& other) noexcept
    : shm_name_(std::move(other.shm_name_)),
      region_(other.region_),
      is_owner_(other.is_owner_),
      sems_initialized_(other.sems_initialized_) {
    other.region_ = nullptr;
    other.is_owner_ = false;
    other.sems_initialized_ = false;
}

ShmChannel& ShmChannel::operator=(ShmChannel&& other) noexcept {
    if (this != &other) {
        close();
        shm_name_ = std::move(other.shm_name_);
        region_ = other.region_;
        is_owner_ = other.is_owner_;
        sems_initialized_ = other.sems_initialized_;

        other.region_ = nullptr;
        other.is_owner_ = false;
        other.sems_initialized_ = false;
    }
    return *this;
}

bool ShmChannel::create(const std::string& name) {
    if (is_open()) {
        close();
    }

    int fd = shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd == -1) {
        perror("shm_open");
        return false;
    }

    if (ftruncate(fd, static_cast<off_t>(sizeof(SharedRegion))) == -1) {
        perror("ftruncate");
        if (::close(fd) == -1) {
            perror("close");
        }
        if (shm_unlink(name.c_str()) == -1) {
            perror("shm_unlink");
        }
        return false;
    }

    void* addr = mmap(
        nullptr,
        sizeof(SharedRegion),
        PROT_READ | PROT_WRITE,
        MAP_SHARED,
        fd,
        0
    );
    if (addr == MAP_FAILED) {
        perror("mmap");
        if (::close(fd) == -1) {
            perror("close");
        }
        if (shm_unlink(name.c_str()) == -1) {
            perror("shm_unlink");
        }
        return false;
    }

    if (::close(fd) == -1) {
        perror("close");
        if (munmap(addr, sizeof(SharedRegion)) == -1) {
            perror("munmap");
        }
        if (shm_unlink(name.c_str()) == -1) {
            perror("shm_unlink");
        }
        return false;
    }

    shm_name_ = name;
    region_ = static_cast<SharedRegion*>(addr);
    is_owner_ = true;
    sems_initialized_ = false;

    region_->length = 0;
    region_->flag = PacketFlag::DATA_CHUNK;
    std::memset(region_->data, 0, sizeof(region_->data));

    if (sem_init(&region_->ready, 1, 0) == -1) {
        perror("sem_init ready");
        close();
        return false;
    }

    if (sem_init(&region_->ack, 1, 0) == -1) {
        perror("sem_init ack");
        if (sem_destroy(&region_->ready) == -1) {
            perror("sem_destroy ready");
        }
        close();
        return false;
    }

    sems_initialized_ = true;
    return true;
}

bool ShmChannel::open(const std::string& name) {
    if (is_open()) {
        close();
    }

    int fd = shm_open(name.c_str(), O_RDWR, 0);
    if (fd == -1) {
        perror("shm_open");
        return false;
    }

    struct stat st {};
    if (fstat(fd, &st) == -1) {
        perror("fstat");
        if (::close(fd) == -1) {
            perror("close");
        }
        return false;
    }

    if (st.st_size < static_cast<off_t>(sizeof(SharedRegion))) {
        std::cerr << "Shared memory object is too small: " << st.st_size
                  << " bytes (expected at least " << sizeof(SharedRegion) << ")\n";
        if (::close(fd) == -1) {
            perror("close");
        }
        return false;
    }

    void* addr = mmap(
        nullptr,
        sizeof(SharedRegion),
        PROT_READ | PROT_WRITE,
        MAP_SHARED,
        fd,
        0
    );
    if (addr == MAP_FAILED) {
        perror("mmap");
        if (::close(fd) == -1) {
            perror("close");
        }
        return false;
    }

    if (::close(fd) == -1) {
        perror("close");
        if (munmap(addr, sizeof(SharedRegion)) == -1) {
            perror("munmap");
        }
        return false;
    }

    shm_name_ = name;
    region_ = static_cast<SharedRegion*>(addr);
    is_owner_ = false;
    sems_initialized_ = false;
    return true;
}

bool ShmChannel::send_line(const std::string& line, pid_t peer_pid) {
    if (!is_open()) {
        return false;
    }

    if (line.empty()) {
        region_->length = 0;
        region_->flag = PacketFlag::LINE_END;

        if (sem_post(&region_->ready) == -1) {
            perror("sem_post ready");
            return false;
        }
        return wait_ack(&region_->ack, peer_pid);
    }

    std::size_t offset = 0;
    while (offset < line.size()) {
        std::size_t remaining = line.size() - offset;
        std::size_t chunk_len = (remaining > CHUNK_SIZE) ? CHUNK_SIZE : remaining;

        std::memcpy(region_->data, line.data() + offset, chunk_len);
        region_->length = chunk_len;
        offset += chunk_len;
        region_->flag = (offset >= line.size()) ? PacketFlag::LINE_END : PacketFlag::DATA_CHUNK;

        if (sem_post(&region_->ready) == -1) {
            perror("sem_post ready");
            return false;
        }
        if (!wait_ack(&region_->ack, peer_pid)) {
            return false;
        }
    }

    return true;
}

bool ShmChannel::send_eof(pid_t peer_pid) {
    if (!is_open()) {
        return false;
    }

    region_->length = 0;
    region_->flag = PacketFlag::END_OF_FILE;

    if (sem_post(&region_->ready) == -1) {
        perror("sem_post ready");
        return false;
    }
    return wait_ack(&region_->ack, peer_pid);
}

bool ShmChannel::receive_line(std::string& out_line, bool& is_eof) {
    out_line.clear();
    is_eof = false;

    if (!is_open()) {
        return false;
    }

    while (true) {
        if (!wait_sem(&region_->ready)) {
            return false;
        }

        std::size_t len = region_->length;
        PacketFlag flag = region_->flag;

        if (len > CHUNK_SIZE) {
            std::cerr << "Invalid chunk length in shared memory: " << len << '\n';
            if (sem_post(&region_->ack) == -1) {
                perror("sem_post ack");
            }
            return false;
        }

        if (flag == PacketFlag::END_OF_FILE) {
            is_eof = true;
            if (sem_post(&region_->ack) == -1) {
                perror("sem_post ack");
            }
            return false;
        }

        if (flag != PacketFlag::DATA_CHUNK && flag != PacketFlag::LINE_END) {
            std::cerr << "Unknown packet flag in shared memory\n";
            if (sem_post(&region_->ack) == -1) {
                perror("sem_post ack");
            }
            return false;
        }

        if (len > 0) {
            out_line.append(region_->data, len);
        }

        if (sem_post(&region_->ack) == -1) {
            perror("sem_post ack");
            return false;
        }

        if (flag == PacketFlag::LINE_END) {
            return true;
        }
    }
}

void ShmChannel::close() {
    if (region_ != nullptr) {
        if (is_owner_ && sems_initialized_) {
            if (sem_destroy(&region_->ready) == -1) {
                perror("sem_destroy ready");
            }
            if (sem_destroy(&region_->ack) == -1) {
                perror("sem_destroy ack");
            }
            sems_initialized_ = false;
        }

        if (munmap(region_, sizeof(SharedRegion)) == -1) {
            perror("munmap");
        }
        region_ = nullptr;
    }

    if (is_owner_ && !shm_name_.empty()) {
        if (shm_unlink(shm_name_.c_str()) == -1) {
            perror("shm_unlink");
        }
    }

    is_owner_ = false;
    shm_name_.clear();
}

bool ShmChannel::is_open() const noexcept {
    return region_ != nullptr;
}

const std::string& ShmChannel::get_name() const noexcept {
    return shm_name_;
}
