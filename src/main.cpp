#include <iostream>
#include <vector>
#include <thread>
#include <memory>
#include "net/boss_thread.h"
#include "net/worker_loop.h"

void storage_engine_mock(
    std::vector<SPSCRingBuffer<Command, 4096>*>& worker_tx_queues,
    std::vector<WorkerLoop*>& workers) 
{
    std::cout << "[Storage] Engine started." << std::endl;
    
    while (true) {
        for (size_t i = 0; i < worker_tx_queues.size(); i++) {
            Command cmd;
            while (worker_tx_queues[i]->pop(cmd)) {
                cmd.status_code = 0; 
                workers[i]->rx_from_storage_.push(cmd);
                
                uint64_t w = 1;
                // Suppress unused result warning gracefully
                [[maybe_unused]] ssize_t _res = write(workers[i]->wakeup_fd_, &w, sizeof(w));
            }
        }
        std::this_thread::yield(); 
    }
}

int main() {
    constexpr int NUM_WORKERS = 4;
    constexpr int PORT = 9000;

    std::vector<std::unique_ptr<SPSCRingBuffer<Command, 4096>>> tx_queues;
    std::vector<std::shared_ptr<WorkerLoop>> workers;
    std::vector<SPSCRingBuffer<Command, 4096>*> raw_tx_queues;
    std::vector<WorkerLoop*> raw_workers;

    for (int i = 0; i < NUM_WORKERS; i++) {
        tx_queues.push_back(std::make_unique<SPSCRingBuffer<Command, 4096>>());
        workers.push_back(std::make_shared<WorkerLoop>(i, *tx_queues.back()));
        raw_tx_queues.push_back(tx_queues.back().get());
        raw_workers.push_back(workers.back().get());
    }

    std::vector<std::thread> worker_threads;
    for (auto& worker : workers) {
        worker_threads.emplace_back([w = worker.get()]() { w->run(); });
    }

    std::thread storage_thread(storage_engine_mock, std::ref(raw_tx_queues), std::ref(raw_workers));

    std::cout << "[Boss] Starting Multi-Reactor TCP server on port " << PORT << std::endl;
    BossThread boss(PORT, workers);
    boss.run();

    for (auto& t : worker_threads) t.join();
    storage_thread.join();

    return 0;
}