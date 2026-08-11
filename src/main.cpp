#include <iostream>
#include <vector>
#include <thread>
#include <memory>
#include "net/boss_thread.h"
#include "net/worker_loop.h"
#include "storage/memtable.h"
#include "storage/wal.h"

// Phase 2: The Real Storage Engine
void storage_engine_loop(
    std::vector<SPSCRingBuffer<Command, 4096>*>& worker_tx_queues,
    MemTable& memtable,
    SPSCRingBuffer<Command, 4096>& wal_queue) 
{
    std::cout << "[Storage] Engine started." << std::endl;
    
    while (true) {
        for (size_t worker_id = 0; worker_id < worker_tx_queues.size(); worker_id++) {
            Command cmd;
            while (worker_tx_queues[worker_id]->pop(cmd)) {
                
                // Embed the worker_id so the WAL knows where to send the ACK
                cmd.header.client_id = worker_id;

                if (cmd.header.type == CommandType::PUT) {
                    // 1. Insert into MemTable
                    std::string key(cmd.key, cmd.header.key_len);
                    std::string val(cmd.value, cmd.header.val_len);
                    memtable.put(key, val);
                    
                    // 2. Dispatch to WAL for durability & ACK
                    while (!wal_queue.push(cmd)) {
                        // Backpressure: If WAL is too slow, we spin wait.
                    }
                } else if (cmd.header.type == CommandType::GET) {
                    // Phase 2 GET implementation (Immediate response, no WAL)
                    std::string key(cmd.key, cmd.header.key_len);
                    std::string val;
                    if (memtable.get(key, val)) {
                        cmd.status_code = 0;
                        cmd.header.val_len = val.size();
                        std::memcpy(cmd.value, val.data(), val.size());
                    } else {
                        cmd.status_code = 1; // Not found
                        cmd.header.val_len = 0;
                    }
                    // TODO: Send GET ACKs back to network directly here (skipping WAL).
                    // For brevity in Phase 2, we assume write-heavy workload.
                }
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

    // --- Phase 2: Instantiate Storage Components ---
    MemTable memtable;
    SPSCRingBuffer<Command, 4096> wal_queue;
    WALThread wal("hft_database.wal", wal_queue, raw_workers);

    // Spawn Threads
    std::vector<std::thread> worker_threads;
    for (auto& worker : workers) {
        worker_threads.emplace_back([w = worker.get()]() { w->run(); });
    }

    std::thread wal_sys_thread([&wal]() { wal.run(); });
    std::thread storage_thread(storage_engine_loop, std::ref(raw_tx_queues), std::ref(memtable), std::ref(wal_queue));

    std::cout << "[Boss] Starting Multi-Reactor TCP server on port " << PORT << std::endl;
    BossThread boss(PORT, workers);
    boss.run();

    // Join
    for (auto& t : worker_threads) t.join();
    storage_thread.join();
    wal_sys_thread.join();

    return 0;
}