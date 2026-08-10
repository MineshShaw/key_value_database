#pragma once

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <fcntl.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>
#include <cstring>
#include <iostream>

#include "../protocol/wire_format.h"
#include "spsc_queue.h" 

// Utility to set non-blocking sockets
inline void set_non_blocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

struct ConnectionState {
    int fd;
    bool backpressure_applied = false;
    std::vector<uint8_t> rx_buffer; // Read accumulation (handles TCP fragmentation)
    std::vector<uint8_t> tx_buffer; // Write accumulation (handles EAGAIN on write)
};

class WorkerLoop {
public:
    WorkerLoop(int id, SPSCRingBuffer<Command, 4096>& tx_queue)
        : id_(id), tx_to_storage_(tx_queue) {
        
        epoll_fd_ = epoll_create1(0);
        
        // Create eventfd for Storage thread to wake us up for ACKs
        wakeup_fd_ = eventfd(0, EFD_NONBLOCK);
        
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET; 
        ev.data.fd = wakeup_fd_;
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wakeup_fd_, &ev);
    }

    void register_connection(int fd) {
        set_non_blocking(fd);
        connections_[fd] = ConnectionState{fd};
        
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = fd;
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev);
    }

    // Queue for the Storage thread to send ACKs back to this specific worker
    SPSCRingBuffer<Command, 4096> rx_from_storage_;
    int wakeup_fd_;

    void run() {
        constexpr int MAX_EVENTS = 64;
        std::array<epoll_event, MAX_EVENTS> events{};

        while (true) {
            int n = epoll_wait(epoll_fd_, events.data(), MAX_EVENTS, -1);

            for (int i = 0; i < n; i++) {
                int fd = events[i].data.fd;

                if (fd == wakeup_fd_) {
                    handle_storage_acks();
                } else if (events[i].events & EPOLLIN) {
                    handle_read(fd);
                } else if (events[i].events & EPOLLOUT) {
                    handle_write(fd);
                }
            }
        }
    }

private:
    int id_;
    int epoll_fd_;
    SPSCRingBuffer<Command, 4096>& tx_to_storage_;
    std::unordered_map<int, ConnectionState> connections_;

    void handle_read(int fd) {
        auto& conn = connections_[fd];
        char buf[4096];
        
        while (true) {
            ssize_t bytes_read = read(fd, buf, sizeof(buf));
            if (bytes_read == -1) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break; // Finished reading
                close(fd); connections_.erase(fd); return;
            }
            if (bytes_read == 0) {
                close(fd); connections_.erase(fd); return; // Client disconnected
            }
            
            conn.rx_buffer.insert(conn.rx_buffer.end(), buf, buf + bytes_read);
            parse_frames(conn);
        }
    }

    void parse_frames(ConnectionState& conn) {
        // TCP fragmentation loop
        while (conn.rx_buffer.size() >= sizeof(MsgHeader)) {
            MsgHeader* hdr = reinterpret_cast<MsgHeader*>(conn.rx_buffer.data());
            size_t total_frame_size = sizeof(MsgHeader) + hdr->key_len + hdr->val_len;

            if (conn.rx_buffer.size() < total_frame_size) {
                break; // Incomplete frame, wait for next epoll trigger
            }

            // Frame is complete. Construct command.
            Command cmd{};
            cmd.header = *hdr;
            cmd.client_fd = conn.fd;
            
            // In a zero-copy design, we'd pass pointers. For Phase 1, we copy to the struct.
            std::memcpy(cmd.key, conn.rx_buffer.data() + sizeof(MsgHeader), hdr->key_len);
            if (hdr->val_len > 0) {
                std::memcpy(cmd.value, conn.rx_buffer.data() + sizeof(MsgHeader) + hdr->key_len, hdr->val_len);
            }

            if (!tx_to_storage_.push(cmd)) {
                // Queue full! Backpressure required.
                apply_backpressure(conn);
                return;
            }

            // Remove parsed frame from buffer
            conn.rx_buffer.erase(conn.rx_buffer.begin(), conn.rx_buffer.begin() + total_frame_size);
        }
    }

    void handle_storage_acks() {
        uint64_t dummy;
        read(wakeup_fd_, &dummy, sizeof(dummy)); // Clear the eventfd

        Command ack;
        while (rx_from_storage_.pop(ack)) {
            auto it = connections_.find(ack.client_fd);
            if (it == connections_.end()) continue;

            // Formulate ACK byte array
            MsgHeader response_hdr = ack.header;
            response_hdr.type = CommandType::ACK;
            response_hdr.val_len = 0; // ACKs might just have status code

            // Append to socket's TX buffer
            uint8_t* ptr = reinterpret_cast<uint8_t*>(&response_hdr);
            it->second.tx_buffer.insert(it->second.tx_buffer.end(), ptr, ptr + sizeof(MsgHeader));

            // Try to write immediately
            handle_write(ack.client_fd);
        }
    }

    void handle_write(int fd) {
        auto& conn = connections_[fd];
        if (conn.tx_buffer.empty()) return;

        ssize_t bytes_written = write(fd, conn.tx_buffer.data(), conn.tx_buffer.size());
        if (bytes_written > 0) {
            conn.tx_buffer.erase(conn.tx_buffer.begin(), conn.tx_buffer.begin() + bytes_written);
        }
        
        // If we couldn't write everything, ensure EPOLLOUT is tracked
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET | (conn.tx_buffer.empty() ? 0 : EPOLLOUT);
        ev.data.fd = fd;
        epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
    }

    void apply_backpressure(ConnectionState& conn) { /* Handled in previous snippet */ }
};