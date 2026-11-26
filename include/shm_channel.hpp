#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include <semaphore.h>
#include <sys/types.h>

constexpr std::size_t CHUNK_SIZE = 4096;

enum class PacketFlag : std::uint8_t {
    DATA_CHUNK = 0,
    LINE_END = 1,
    END_OF_FILE = 2
};

struct SharedRegion {
    sem_t ready;
    sem_t ack;
    std::size_t length;
    PacketFlag flag;
    char data[CHUNK_SIZE];
};

class ShmChannel {
public:
    ShmChannel() = default;
    ~ShmChannel();

    ShmChannel(const ShmChannel&) = delete;
    ShmChannel& operator=(const ShmChannel&) = delete;

    ShmChannel(ShmChannel&& other) noexcept;
    ShmChannel& operator=(ShmChannel&& other) noexcept;

    bool create(const std::string& name);
    bool open(const std::string& name);

    bool send_line(const std::string& line, pid_t peer_pid = -1);
    bool send_eof(pid_t peer_pid = -1);
    bool receive_line(std::string& out_line, bool& is_eof);

    void close();

    bool is_open() const noexcept;
    const std::string& get_name() const noexcept;

private:
    std::string shm_name_;
    SharedRegion* region_ = nullptr;
    bool is_owner_ = false;
    bool sems_initialized_ = false;
};
