#include "shm_channel.hpp"
#include "string_utils.hpp"

#include <iostream>
#include <string>
#include <unistd.h>

int child(const std::string& shm_name) {
    std::cerr << "[Child] PID: " << static_cast<unsigned long>(getpid()) << '\n';

    ShmChannel channel;
    if (!channel.open(shm_name)) {
        std::cerr << "[Child] Failed to open shared memory: " << shm_name << '\n';
        return 1;
    }

    std::string line{};
    bool is_eof = false;

    while (channel.receive_line(line, is_eof)) {
        reverse_string(line);
        if (!(std::cout << line << '\n')) {
            std::cerr << "[Child] Failed to write to output file\n";
            channel.close();
            return 1;
        }
    }

    channel.close();

    if (!std::cout.flush()) {
        std::cerr << "[Child] Failed to flush output file\n";
        return 1;
    }

    if (!is_eof) {
        return 1;
    }

    return 0;
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: child <shm_name>\n";
        return 1;
    }

    return child(argv[1]);
}
