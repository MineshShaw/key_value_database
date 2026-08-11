#pragma once
#include <vector>
#include <string>
#include <cstdint>
#include "../utils/hash.h"

class BloomFilter {
public:
    BloomFilter(size_t expected_entries, double bits_per_key = 10.0) {
        size_t num_bits = static_cast<size_t>(expected_entries * bits_per_key);
  
        num_bits = (num_bits < 64) ? 64 : num_bits; 
        
        bit_array_.assign((num_bits + 7) / 8, 0);
        num_hashes_ = 7; 
    }

    BloomFilter(std::vector<uint8_t> data, uint8_t num_hashes) 
        : bit_array_(std::move(data)), num_hashes_(num_hashes) {}

    void add(const std::string& key) {
        uint64_t hash = utils::murmur_hash_64(key.data(), key.size());
        uint32_t h1 = static_cast<uint32_t>(hash);
        uint32_t h2 = static_cast<uint32_t>(hash >> 32);
        size_t total_bits = bit_array_.size() * 8;

        for (uint8_t i = 0; i < num_hashes_; i++) {
            // Kirsch-Mitzenmacher optimization
            uint32_t combined_hash = h1 + (i * h2);
            size_t bit_idx = combined_hash % total_bits;
            bit_array_[bit_idx / 8] |= (1 << (bit_idx % 8));
        }
    }

    bool might_contain(const std::string& key) const {
        if (bit_array_.empty()) return false;
        
        uint64_t hash = utils::murmur_hash_64(key.data(), key.size());
        uint32_t h1 = static_cast<uint32_t>(hash);
        uint32_t h2 = static_cast<uint32_t>(hash >> 32);
        size_t total_bits = bit_array_.size() * 8;

        for (uint8_t i = 0; i < num_hashes_; i++) {
            uint32_t combined_hash = h1 + (i * h2);
            size_t bit_idx = combined_hash % total_bits;
            if ((bit_array_[bit_idx / 8] & (1 << (bit_idx % 8))) == 0) {
                return false;
            }
        }
        return true;
    }

    const std::vector<uint8_t>& data() const { return bit_array_; }
    uint8_t num_hashes() const { return num_hashes_; }

private:
    std::vector<uint8_t> bit_array_;
    uint8_t num_hashes_;
};