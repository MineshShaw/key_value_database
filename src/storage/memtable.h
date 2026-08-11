#pragma once

#include <atomic>
#include <vector>
#include <string>
#include <random>
#include <cstring>
#include <iostream>

constexpr int MAX_LEVEL = 16;
constexpr float PROBABILITY = 0.5f;

struct SkipListNode {
    std::string key;
    std::string value;
    std::vector<std::atomic<SkipListNode*>> forward;

    SkipListNode(const std::string& k, const std::string& v, int level)
        : key(k), value(v), forward(level) {
        for (int i = 0; i < level; ++i) {
            forward[i].store(nullptr, std::memory_order_relaxed);
        }
    }
};

class MemTable {
public:
    MemTable() : current_level_(1) {
        head_ = new SkipListNode("", "", MAX_LEVEL);
        size_bytes_.store(0, std::memory_order_relaxed);
    }

    ~MemTable() {
        SkipListNode* current = head_->forward[0].load(std::memory_order_relaxed);
        while (current) {
            SkipListNode* next = current->forward[0].load(std::memory_order_relaxed);
            delete current;
            current = next;
        }
        delete head_;
    }

    void put(const std::string& key, const std::string& value) {
        std::vector<SkipListNode*> update(MAX_LEVEL, nullptr);
        SkipListNode* current = head_;

        for (int i = current_level_ - 1; i >= 0; --i) {
            SkipListNode* next = current->forward[i].load(std::memory_order_acquire);
            while (next && next->key < key) {
                current = next;
                next = current->forward[i].load(std::memory_order_acquire);
            }
            update[i] = current;
        }

        SkipListNode* next_node = current->forward[0].load(std::memory_order_acquire);
        
        if (next_node && next_node->key == key) {
            size_bytes_.fetch_add(value.size() - next_node->value.size(), std::memory_order_relaxed);
            next_node->value = value; 
            return;
        }

        int new_level = random_level();
        if (new_level > current_level_) {
            for (int i = current_level_; i < new_level; ++i) {
                update[i] = head_;
            }
            current_level_ = new_level;
        }

        SkipListNode* new_node = new SkipListNode(key, value, new_level);

        for (int i = 0; i < new_level; ++i) {
            new_node->forward[i].store(update[i]->forward[i].load(std::memory_order_relaxed), std::memory_order_relaxed);
            update[i]->forward[i].store(new_node, std::memory_order_release);
        }
        
        size_bytes_.fetch_add(key.size() + value.size(), std::memory_order_relaxed);
    }

    bool get(const std::string& key, std::string& value) const {
        SkipListNode* current = head_;
        for (int i = current_level_ - 1; i >= 0; --i) {
            SkipListNode* next = current->forward[i].load(std::memory_order_acquire);
            while (next && next->key < key) {
                current = next;
                next = current->forward[i].load(std::memory_order_acquire);
            }
        }
        SkipListNode* next_node = current->forward[0].load(std::memory_order_acquire);
        if (next_node && next_node->key == key) {
            value = next_node->value;
            return true;
        }
        return false;
    }

    size_t size_bytes() const {
        return size_bytes_.load(std::memory_order_relaxed);
    }

    // --- NEW: Lock-Free Iterator for the Flush Thread ---
    class Iterator {
    public:
        Iterator(SkipListNode* start) : current_(start) {}
        bool is_valid() const { return current_ != nullptr; }
        void next() { current_ = current_->forward[0].load(std::memory_order_acquire); }
        std::string key() const { return current_->key; }
        std::string value() const { return current_->value; }
    private:
        SkipListNode* current_;
    };

    Iterator begin() const {
        return Iterator(head_->forward[0].load(std::memory_order_acquire));
    }

private:
    SkipListNode* head_;
    int current_level_;
    std::atomic<size_t> size_bytes_;

    int random_level() {
        static thread_local std::mt19937 generator(std::random_device{}());
        std::uniform_real_distribution<float> distribution(0.0, 1.0);
        int level = 1;
        while (distribution(generator) < PROBABILITY && level < MAX_LEVEL) {
            level++;
        }
        return level;
    }
};