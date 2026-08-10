#pragma once
#include <sys/socket.h>
#include <netinet/in.h>
#include <vector>
#include <memory>
#include "worker_loop.h"

class BossThread {
public:
    BossThread(int port, std::vector<std::shared_ptr<WorkerLoop>>& workers) 
        : workers_(workers), current_worker_(0) {
        
        listen_fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
        
        int opt = 1;
        setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port);

        bind(listen_fd_, (sockaddr*)&addr, sizeof(addr));
        listen(listen_fd_, 4096); 
        
        epoll_fd_ = epoll_create1(0);
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = listen_fd_;
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev);
    }

    void run() {
        std::array<epoll_event, 16> events{};
        while (true) {
            int n = epoll_wait(epoll_fd_, events.data(), 16, -1);
            for (int i = 0; i < n; i++) {
                if (events[i].data.fd == listen_fd_) {
                    accept_connections();
                }
            }
        }
    }

private:
    int listen_fd_;
    int epoll_fd_;
    std::vector<std::shared_ptr<WorkerLoop>>& workers_;
    size_t current_worker_; // For Round-Robin

    void accept_connections() {
        while (true) {
            sockaddr_in client_addr;
            socklen_t client_len = sizeof(client_addr);
            int client_fd = accept(listen_fd_, (sockaddr*)&client_addr, &client_len);
            
            if (client_fd == -1) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                continue;
            }

            // Round-robin assign to a worker
            workers_[current_worker_]->register_connection(client_fd);
            current_worker_ = (current_worker_ + 1) % workers_.size();
        }
    }
};