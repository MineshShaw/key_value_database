#pragma once
#include <string>
#include <fstream>
#include "sstable.h"

class SSTableIterator {
public:
    SSTableIterator(const std::string& filepath) 
        : filepath_(filepath), file_(filepath, std::ios::binary) {
        
        if (!file_.is_open()) return;

        // Jump to footer to find where the data blocks end
        file_.seekg(0, std::ios::end);
        size_t file_size = file_.tellg();
        file_.seekg(file_size - sizeof(SSTableFooter), std::ios::beg);
        
        SSTableFooter footer;
        file_.read(reinterpret_cast<char*>(&footer), sizeof(footer));
        data_end_ = footer.index_offset;
        
        // Reset to beginning and load first KV pair
        file_.seekg(0, std::ios::beg);
        next();
    }

    bool is_valid() const { return valid_; }
    std::string key() const { return current_key_; }
    std::string value() const { return current_val_; }
    std::string filepath() const { return filepath_; }

    void next() {
        if (file_.tellg() >= static_cast<std::streampos>(data_end_) || file_.eof()) {
            valid_ = false;
            return;
        }

        uint32_t k_len;
        if (!file_.read(reinterpret_cast<char*>(&k_len), sizeof(k_len))) {
            valid_ = false; return;
        }

        current_key_.resize(k_len);
        file_.read(&current_key_[0], k_len);

        uint32_t v_len;
        file_.read(reinterpret_cast<char*>(&v_len), sizeof(v_len));

        current_val_.resize(v_len);
        file_.read(&current_val_[0], v_len);

        valid_ = true;
    }

private:
    std::string filepath_;
    std::ifstream file_;
    uint64_t data_end_ = 0;
    bool valid_ = false;
    std::string current_key_;
    std::string current_val_;
};