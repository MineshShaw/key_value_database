#pragma once

#include <fcntl.h>
#include <unistd.h>
#include <vector>
#include <string>
#include <iostream>
#include <stdexcept>
#include <thread>
#include "../net/spsc_queue.h"
#include "../protocol/wire_format.h"
#include "../net/worker_loop.h"

class WALThread {
public:
    WALThread(const std::string& file_path, 
              SPSCRingBuffer<Command, 4096>& wal_queue,
              std::vector<WorkerLoop*>& workers)
        : wal_queue_(wal_queue), workers_(workers), is_running_(true) {
        
        fd_ = open(file_path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd_ == -1) {
            throw std::runtime_error("Failed to open WAL file");
        }
    }

    ~WALThread() {
        is_running_ = false;
        close(fd_);
    }

    void run() {
        std::vector<Command> batch;
        batch.reserve(1024);
        
        std::vector<uint8_t> io_buffer;
        io_buffer.reserve(1024 * 1024); 

        while (is_running_) {
            Command cmd;
            
            while (wal_queue_.pop(cmd)) {
                batch.push_back(cmd);
                if (batch.size() >= 1024) break;
            }

            if (batch.empty()) {
                std::this_thread::yield();
                continue;
            }

            io_buffer.clear();
            for (const auto& c : batch) {
                const uint8_t* ptr = reinterpret_cast<const uint8_t*>(&c);
                size_t cmd_size = sizeof(MsgHeader) + c.header.key_len + c.header.val_len;
                io_buffer.insert(io_buffer.end(), ptr, ptr + cmd_size);
            }

            ssize_t bytes_written = write(fd_, io_buffer.data(), io_buffer.size());
            if (bytes_written != static_cast<ssize_t>(io_buffer.size())) {
                std::cerr << "[WAL] CRITICAL IO ERROR: Failed to write batch! errno=" << errno << "\n";
                batch.clear();
                continue;
            }

            if (fdatasync(fd_) == -1) {
                std::cerr << "[WAL] CRITICAL IO ERROR: fdatasync failed! errno=" << errno << "\n";
                batch.clear();
                continue;
            }

            for (auto& c : batch) {
                c.status_code = 0; 
                workers_[c.header.client_id]->rx_from_storage_.push(c);
                
                uint64_t w = 1;
                [[maybe_unused]] ssize_t _res = write(workers_[c.header.client_id]->wakeup_fd_, &w, sizeof(w));
            }

            batch.clear();
        }
    }

private:
    int fd_;
    SPSCRingBuffer<Command, 4096>& wal_queue_;
    std::vector<WorkerLoop*>& workers_;
    bool is_running_;
};