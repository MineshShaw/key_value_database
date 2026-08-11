#include <iostream>
#include <vector>
#include <deque>
#include <thread>
#include <memory>
#include <algorithm>
#include "net/boss_thread.h"
#include "net/worker_loop.h"
#include "storage/memtable.h"
#include "storage/wal.h"
#include "storage/flusher.h"
#include "storage/sstable_reader.h"

constexpr size_t MEMTABLE_FLUSH_THRESHOLD = 64 * 1024; // 64KB for testing

void storage_engine_loop(
    std::vector<SPSCRingBuffer<Command, 4096>*>& worker_tx_queues,
    MemTable** active_memtable,
    SPSCRingBuffer<Command, 4096>& wal_queue,
    SPSCRingBuffer<MemTable*, 16>& flush_queue,
    SPSCRingBuffer<FlushResult, 16>& flush_result_queue) 
{
    std::cout << "[Storage] Engine started." << std::endl;
    
    // Multi-tiered storage state
    std::vector<MemTable*> immutable_memtables;
    std::deque<std::unique_ptr<SSTableReader>> sstables; 

    while (true) {
        // --- 1. Housekeeping: Reap finished MemTables & load new SSTables ---
        FlushResult flush_res;
        while (flush_result_queue.pop(flush_res)) {
            // Load the new SSTable (push to front so newest is read first)
            sstables.push_front(std::make_unique<SSTableReader>(flush_res.sst_filepath));
            
            // Remove the old MemTable from the immutable list and safely delete it
            auto it = std::find(immutable_memtables.begin(), immutable_memtables.end(), flush_res.old_memtable);
            if (it != immutable_memtables.end()) {
                immutable_memtables.erase(it);
            }
            delete flush_res.old_memtable; 
            std::cout << "[Storage] Integrated new SSTable: " << flush_res.sst_filepath << std::endl;
        }

        // --- 2. Process Commands ---
        for (size_t worker_id = 0; worker_id < worker_tx_queues.size(); worker_id++) {
            Command cmd;
            while (worker_tx_queues[worker_id]->pop(cmd)) {
                cmd.header.client_id = worker_id;

                if (cmd.header.type == CommandType::PUT) {
                    std::string key(cmd.key, cmd.header.key_len);
                    std::string val(cmd.value, cmd.header.val_len);
                    
                    (*active_memtable)->put(key, val);
                    while (!wal_queue.push(cmd)) { }

                    if ((*active_memtable)->size_bytes() >= MEMTABLE_FLUSH_THRESHOLD) {
                        MemTable* old_memtable = *active_memtable;
                        immutable_memtables.push_back(old_memtable); // Keep in read path
                        
                        *active_memtable = new MemTable(); // Swap instantly!
                        
                        while (!flush_queue.push(old_memtable)) { std::this_thread::yield(); }
                    }

                } else if (cmd.header.type == CommandType::GET) {
                    std::string key(cmd.key, cmd.header.key_len);
                    std::string val;
                    bool found = false;

                    // A) Check Active MemTable
                    if ((*active_memtable)->get(key, val)) {
                        found = true;
                    } 
                    // B) Check Immutable MemTables (newest to oldest)
                    else {
                        for (auto it = immutable_memtables.rbegin(); it != immutable_memtables.rend(); ++it) {
                            if ((*it)->get(key, val)) {
                                found = true;
                                break;
                            }
                        }
                    }
                    // C) Check SSTables (newest to oldest)
                    if (!found) {
                        for (const auto& sst : sstables) {
                            if (sst->get(key, val)) {
                                found = true;
                                break;
                            }
                        }
                    }

                    if (found) {
                        cmd.status_code = 0;
                        cmd.header.val_len = val.size();
                        std::memcpy(cmd.value, val.data(), val.size());
                    } else {
                        cmd.status_code = 1;
                        cmd.header.val_len = 0;
                    }

                    // Directly send GET ACKs back to network (bypass WAL)
                    // (Assuming workers are globally accessible here like in Phase 2)
                    // Note: For cleanliness we push to an ACK queue, but simulating standard return:
                    // In a production refactor, you'd route this via the exact same return path.
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

    MemTable* active_memtable = new MemTable();
    SPSCRingBuffer<Command, 4096> wal_queue;
    SPSCRingBuffer<MemTable*, 16> flush_queue; 
    SPSCRingBuffer<FlushResult, 16> flush_result_queue; // The new reclamation bridge

    WALThread wal("hft_database.wal", wal_queue, raw_workers);
    FlushThread flusher("./db_data", flush_queue, flush_result_queue);

    std::vector<std::thread> worker_threads;
    for (auto& worker : workers) {
        worker_threads.emplace_back([w = worker.get()]() { w->run(); });
    }

    std::thread wal_sys_thread([&wal]() { wal.run(); });
    std::thread flush_sys_thread([&flusher]() { flusher.run(); });
    
    std::thread storage_thread(storage_engine_loop, std::ref(raw_tx_queues), &active_memtable, 
                               std::ref(wal_queue), std::ref(flush_queue), std::ref(flush_result_queue));

    std::cout << "[Boss] Starting Multi-Reactor TCP server on port " << PORT << std::endl;
    BossThread boss(PORT, workers);
    boss.run();

    for (auto& t : worker_threads) t.join();
    storage_thread.join();
    wal_sys_thread.join();
    flush_sys_thread.join();

    delete active_memtable;
    return 0;
}