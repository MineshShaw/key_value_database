#pragma once
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <memory>
#include <iostream>
#include "sstable.h"
#include "bloom_filter.h"

class SSTableReader {
public:
    SSTableReader(const std::string& filepath) : file_(filepath, std::ios::binary) {
        if (!file_.is_open()) throw std::runtime_error("Cannot open SSTable: " + filepath);
        
        // 1. Read Footer
        file_.seekg(0, std::ios::end);
        size_t file_size = file_.tellg();
        file_.seekg(file_size - sizeof(SSTableFooter), std::ios::beg);
        
        SSTableFooter footer;
        file_.read(reinterpret_cast<char*>(&footer), sizeof(footer));
        if (footer.magic_number != 0xDEADBEEF) throw std::runtime_error("Corrupt SSTable: " + filepath);
        
        // 2. Load Bloom Filter
        file_.seekg(footer.bloom_offset, std::ios::beg);
        std::vector<uint8_t> bloom_data(footer.bloom_size);
        file_.read(reinterpret_cast<char*>(bloom_data.data()), footer.bloom_size);
        bloom_filter_ = std::make_unique<BloomFilter>(std::move(bloom_data), 7);
        
        // 3. Load Sparse Index
        file_.seekg(footer.index_offset, std::ios::beg);
        size_t index_bytes_read = 0;
        while (index_bytes_read < footer.index_size) {
            uint32_t k_len;
            file_.read(reinterpret_cast<char*>(&k_len), sizeof(k_len));
            std::string k(k_len, '\0');
            file_.read(&k[0], k_len);
            
            uint64_t offset;
            file_.read(reinterpret_cast<char*>(&offset), sizeof(offset));
            
            index_.push_back({k, offset});
            index_bytes_read += sizeof(k_len) + k_len + sizeof(offset);
        }
    }

    bool get(const std::string& key, std::string& value) {
        // 1. The Fast Path: Bloom Filter says NO. Avoid hitting disk.
        if (!bloom_filter_->might_contain(key)) return false;

        // 2. Binary search the Sparse Index
        auto it = std::upper_bound(index_.begin(), index_.end(), key, 
            [](const std::string& search_key, const IndexEntry& entry) {
                return search_key < entry.key;
            });

        if (it != index_.begin()) {
            --it; // Move back to the largest key that is <= our search key
        } else if (it == index_.begin() && key < it->key) {
            return false; // Search key is physically before our first indexed block
        }
        
        // 3. The Slow Path: Scan the disk block
        file_.clear(); // Clear EOF flags from previous reads
        file_.seekg(it->offset, std::ios::beg);
        
        while (true) {
            uint32_t k_len;
            if (!file_.read(reinterpret_cast<char*>(&k_len), sizeof(k_len))) break; // EOF
            
            std::string k(k_len, '\0');
            file_.read(&k[0], k_len);
            
            uint32_t v_len;
            file_.read(reinterpret_cast<char*>(&v_len), sizeof(v_len));
            
            if (k == key) {
                value.resize(v_len);
                file_.read(&value[0], v_len);
                return true;
            }
            
            if (k > key) {
                break; // Because keys are sorted, passing it means it's not here
            }
            
            file_.seekg(v_len, std::ios::cur); // Skip the value payload and keep scanning
        }
        return false;
    }

private:
    std::ifstream file_;
    std::unique_ptr<BloomFilter> bloom_filter_;
    std::vector<IndexEntry> index_;
};