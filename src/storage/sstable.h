#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include <fstream>
#include <stdexcept>
#include "bloom_filter.h"

#pragma pack(push, 1)
struct SSTableFooter {
    uint64_t index_offset;
    uint64_t index_size;
    uint64_t bloom_offset;
    uint64_t bloom_size;
    uint64_t magic_number; 
};
#pragma pack(pop)

struct IndexEntry {
    std::string key;
    uint64_t offset;
};

class SSTableBuilder {
public:
    SSTableBuilder(const std::string& filepath, size_t expected_entries) 
        : file_(filepath, std::ios::binary | std::ios::out | std::ios::trunc),
          bloom_filter_(expected_entries),
          current_offset_(0),
          keys_written_(0) {
        if (!file_.is_open()) {
            throw std::runtime_error("Failed to open SSTable for writing");
        }
    }

    void add(const std::string& key, const std::string& value) {
        bloom_filter_.add(key);

        if (keys_written_ % 64 == 0) {
            index_.push_back({key, current_offset_});
        }

        uint32_t k_len = key.size();
        uint32_t v_len = value.size();
        
        file_.write(reinterpret_cast<const char*>(&k_len), sizeof(k_len));
        file_.write(key.data(), k_len);
        file_.write(reinterpret_cast<const char*>(&v_len), sizeof(v_len));
        file_.write(value.data(), v_len);

        current_offset_ += (sizeof(k_len) + k_len + sizeof(v_len) + v_len);
        keys_written_++;
    }

    void finish() {
        uint64_t index_start = current_offset_;
        for (const auto& entry : index_) {
            uint32_t k_len = entry.key.size();
            file_.write(reinterpret_cast<const char*>(&k_len), sizeof(k_len));
            file_.write(entry.key.data(), k_len);
            file_.write(reinterpret_cast<const char*>(&entry.offset), sizeof(entry.offset));
        }
        uint64_t index_size = static_cast<uint64_t>(file_.tellp()) - index_start;

        uint64_t bloom_start = static_cast<uint64_t>(file_.tellp());
        const auto& bloom_data = bloom_filter_.data();
        file_.write(reinterpret_cast<const char*>(bloom_data.data()), bloom_data.size());
        uint64_t bloom_size = bloom_data.size();

        SSTableFooter footer{};
        footer.index_offset = index_start;
        footer.index_size = index_size;
        footer.bloom_offset = bloom_start;
        footer.bloom_size = bloom_size;
        footer.magic_number = 0xDEADBEEF;

        file_.write(reinterpret_cast<const char*>(&footer), sizeof(footer));
        file_.close();
    }

private:
    std::ofstream file_;
    BloomFilter bloom_filter_;
    uint64_t current_offset_;
    size_t keys_written_;
    std::vector<IndexEntry> index_;
};