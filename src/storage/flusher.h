#pragma once

#include <string>
#include <thread>
#include <iostream>
#include <filesystem>
#include <cstring>
#include "memtable.h"
#include "sstable.h"
#include "../net/spsc_queue.h"

// Passed back from Flusher to Storage thread
struct FlushResult {
    MemTable* old_memtable;
    char sst_filepath[256];
};

class FlushThread {
public:
    FlushThread(const std::string& db_dir, 
                SPSCRingBuffer<MemTable*, 16>& flush_queue,
                SPSCRingBuffer<FlushResult, 16>& flush_result_queue)
        : db_dir_(db_dir), flush_queue_(flush_queue), 
          flush_result_queue_(flush_result_queue), 
          is_running_(true), sstable_id_counter_(1) {
        
        std::filesystem::create_directories(db_dir_);
    }

    ~FlushThread() {
        is_running_ = false;
    }

    void run() {
        while (is_running_) {
            MemTable* frozen_memtable = nullptr;
            
            if (flush_queue_.pop(frozen_memtable)) {
                std::string filepath = flush_to_disk(frozen_memtable);
                
                // Handoff to Storage loop for safe memory reclamation
                FlushResult result{};
                result.old_memtable = frozen_memtable;
                std::strncpy(result.sst_filepath, filepath.c_str(), sizeof(result.sst_filepath) - 1);
                
                while (!flush_result_queue_.push(result)) {
                    std::this_thread::yield();
                }
            } else {
                std::this_thread::yield();
            }
        }
    }

private:
    std::string db_dir_;
    SPSCRingBuffer<MemTable*, 16>& flush_queue_;
    SPSCRingBuffer<FlushResult, 16>& flush_result_queue_;
    bool is_running_;
    uint64_t sstable_id_counter_;

    std::string flush_to_disk(MemTable* memtable) {
        std::string sst_filename = db_dir_ + "/table_" + std::to_string(sstable_id_counter_++) + ".sst";
        size_t estimated_keys = (memtable->size_bytes() / 50) + 100;
        
        SSTableBuilder builder(sst_filename, estimated_keys);
        auto it = memtable->begin();
        while (it.is_valid()) {
            builder.add(it.key(), it.value());
            it.next();
        }
        builder.finish();
        
        return sst_filename;
    }
};