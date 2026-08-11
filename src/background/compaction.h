#pragma once
#include <string>
#include <vector>
#include <queue>
#include <thread>
#include <cstring>
#include <filesystem>
#include <iostream>
#include "../storage/sstable_iterator.h"
#include "../storage/sstable.h"
#include "../net/spsc_queue.h"

// Passed from Storage -> Compactor
struct CompactionTask {
    char sstables[8][256];
    size_t count = 0;
};

// Passed from Compactor -> Storage
struct CompactionResult {
    char old_sstables[8][256];
    size_t old_count = 0;
    char new_sstable[256];
    bool success = false;
};

// Node for the Min-Heap Priority Queue
struct HeapNode {
    std::string key;
    std::string value;
    size_t sstable_idx; // 0 = Newest SSTable, N = Oldest SSTable
    SSTableIterator* iter;

    bool operator<(const HeapNode& other) const {
        if (key == other.key) {
            // If keys match, we want the NEWER one (smaller index) to rise to the top.
            // Priority queues in C++ are Max-Heaps by default, so we invert the logic.
            return sstable_idx > other.sstable_idx; 
        }
        return key > other.key; 
    }
};

class CompactionThread {
public:
    CompactionThread(const std::string& db_dir,
                     SPSCRingBuffer<CompactionTask, 4>& task_queue,
                     SPSCRingBuffer<CompactionResult, 4>& result_queue)
        : db_dir_(db_dir), task_queue_(task_queue), result_queue_(result_queue), 
          is_running_(true), compact_id_counter_(1) {}

    ~CompactionThread() { is_running_ = false; }

    void run() {
        while (is_running_) {
            CompactionTask task;
            if (task_queue_.pop(task)) {
                process_compaction(task);
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(500)); 
            }
        }
    }

private:
    std::string db_dir_;
    SPSCRingBuffer<CompactionTask, 4>& task_queue_;
    SPSCRingBuffer<CompactionResult, 4>& result_queue_;
    bool is_running_;
    uint64_t compact_id_counter_;

    void process_compaction(const CompactionTask& task) {
        std::cout << "[Compactor] Starting K-Way Merge of " << task.count << " SSTables..." << std::endl;
        
        std::vector<std::unique_ptr<SSTableIterator>> iters;
        std::priority_queue<HeapNode> pq;

        for (size_t i = 0; i < task.count; i++) {
            iters.push_back(std::make_unique<SSTableIterator>(task.sstables[i]));
            if (iters.back()->is_valid()) {
                pq.push({iters.back()->key(), iters.back()->value(), i, iters.back().get()});
            }
        }

        std::string out_filepath = db_dir_ + "/table_compacted_" + std::to_string(compact_id_counter_++) + ".sst";
        SSTableBuilder builder(out_filepath, 10000); 

        std::string last_key = "";

        while (!pq.empty()) {
            HeapNode top = pq.top();
            pq.pop();

            // If key is different, it's the absolute newest version of this key.
            if (top.key != last_key) {
                // Garbage Collection: If the value is empty, it's a Tombstone (deletion marker).
                // We drop it completely, recovering disk space!
                if (!top.value.empty()) { 
                    builder.add(top.key, top.value);
                }
                last_key = top.key;
            }

            top.iter->next();
            if (top.iter->is_valid()) {
                pq.push({top.iter->key(), top.iter->value(), top.sstable_idx, top.iter});
            }
        }

        builder.finish();

        CompactionResult result;
        result.old_count = task.count;
        for (size_t i = 0; i < task.count; i++) {
            std::strncpy(result.old_sstables[i], task.sstables[i], 255);
        }
        std::strncpy(result.new_sstable, out_filepath.c_str(), 255);
        result.success = true;

        while (!result_queue_.push(result) && is_running_) {
            std::this_thread::yield();
        }
        std::cout << "[Compactor] Compaction complete: " << out_filepath << std::endl;
    }
};