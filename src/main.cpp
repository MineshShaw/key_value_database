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
#include "background/compaction.h" // NEW

constexpr size_t MEMTABLE_FLUSH_THRESHOLD = 64 * 1024; 

void storage_engine_loop(
    std::vector<SPSCRingBuffer<Command, 4096>*>& worker_tx_queues,
    MemTable** active_memtable,
    SPSCRingBuffer<Command, 4096>& wal_queue,
    SPSCRingBuffer<MemTable*, 16>& flush_queue,
    SPSCRingBuffer<FlushResult, 16>& flush_result_queue,
    SPSCRingBuffer<CompactionTask, 4>& compaction_task_queue,
    SPSCRingBuffer<CompactionResult, 4>& compaction_result_queue) 
{
    std::cout << "[Storage] Engine started." << std::endl;
    
    std::vector<MemTable*> immutable_memtables;
    std::deque<std::unique_ptr<SSTableReader>> sstables; 
    bool is_compacting = false;

    while (true) {
        // --- 1. Housekeeping: Reap finished Flushes ---
        FlushResult flush_res;
        while (flush_result_queue.pop(flush_res)) {
            sstables.push_front(std::make_unique<SSTableReader>(flush_res.sst_filepath));
            
            auto it = std::find(immutable_memtables.begin(), immutable_memtables.end(), flush_res.old_memtable);
            if (it != immutable_memtables.end()) immutable_memtables.erase(it);
            
            delete flush_res.old_memtable; 
        }

        // --- 2. Housekeeping: Reap finished Compactions ---
        CompactionResult comp_res;
        while (compaction_result_queue.pop(comp_res)) {
            is_compacting = false;
            if (comp_res.success) {
                // Add the new consolidated SSTable to the BACK (oldest tier)
                sstables.push_back(std::make_unique<SSTableReader>(comp_res.new_sstable));
                
                // Remove old readers and delete underlying files
                for (size_t i = 0; i < comp_res.old_count; ++i) {
                    std::string target = comp_res.old_sstables[i];
                    auto it = std::remove_if(sstables.begin(), sstables.end(), 
                        [&](const std::unique_ptr<SSTableReader>& r) { return r->filepath() == target; });
                    sstables.erase(it, sstables.end());
                    std::filesystem::remove(target);
                }
                std::cout << "[Storage] Integrated compacted SSTable. Cleaned up disk." << std::endl;
            }
        }

        // --- 3. Trigger Background Compaction ---
        // If we have 4 or more SSTables, compact the oldest 4
        if (sstables.size() >= 4 && !is_compacting) {
            CompactionTask task;
            task.count = 4;
            
            size_t start_idx = sstables.size() - 4; // Oldest are at the back of deque
            for (size_t i = 0; i < 4; ++i) {
                std::strncpy(task.sstables[i], sstables[start_idx + i]->filepath().c_str(), 255);
            }
            
            if (compaction_task_queue.push(task)) {
                is_compacting = true;
                std::cout << "[Storage] Triggered compaction for oldest 4 tables." << std::endl;
            }
        }

        // --- 4. Process Network Commands (Same as before) ---
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
                        immutable_memtables.push_back(old_memtable);
                        *active_memtable = new MemTable();
                        while (!flush_queue.push(old_memtable)) { std::this_thread::yield(); }
                    }

                } else if (cmd.header.type == CommandType::GET) {
                    std::string key(cmd.key, cmd.header.key_len);
                    std::string val;
                    bool found = false;

                    if ((*active_memtable)->get(key, val)) found = true;
                    else {
                        for (auto it = immutable_memtables.rbegin(); it != immutable_memtables.rend(); ++it) {
                            if ((*it)->get(key, val)) { found = true; break; }
                        }
                    }
                    if (!found) {
                        for (const auto& sst : sstables) {
                            if (sst->get(key, val)) { found = true; break; }
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
    SPSCRingBuffer<FlushResult, 16> flush_result_queue; 
    
    // NEW Compaction queues
    SPSCRingBuffer<CompactionTask, 4> compaction_task_queue; 
    SPSCRingBuffer<CompactionResult, 4> compaction_result_queue; 

    WALThread wal("hft_database.wal", wal_queue, raw_workers);
    FlushThread flusher("./db_data", flush_queue, flush_result_queue);
    CompactionThread compactor("./db_data", compaction_task_queue, compaction_result_queue);

    std::vector<std::thread> worker_threads;
    for (auto& worker : workers) {
        worker_threads.emplace_back([w = worker.get()]() { w->run(); });
    }

    std::thread wal_sys_thread([&wal]() { wal.run(); });
    std::thread flush_sys_thread([&flusher]() { flusher.run(); });
    std::thread compactor_sys_thread([&compactor]() { compactor.run(); });
    
    std::thread storage_thread(storage_engine_loop, std::ref(raw_tx_queues), &active_memtable, 
                               std::ref(wal_queue), std::ref(flush_queue), std::ref(flush_result_queue),
                               std::ref(compaction_task_queue), std::ref(compaction_result_queue));

    std::cout << "[Boss] Starting Multi-Reactor TCP server on port " << PORT << std::endl;
    BossThread boss(PORT, workers);
    boss.run();

    for (auto& t : worker_threads) t.join();
    storage_thread.join();
    wal_sys_thread.join();
    flush_sys_thread.join();
    compactor_sys_thread.join();

    delete active_memtable;
    return 0;
}