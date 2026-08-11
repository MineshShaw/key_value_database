#pragma once

#include <atomic>
#include <array>
#include <cstddef>
#include <cstdint>

// Standard cache line size for modern x86_64 to prevent false sharing
constexpr size_t CACHE_LINE_SIZE = 64;

template <typename T, size_t Capacity>
class SPSCRingBuffer {
    static_assert((Capacity != 0) && ((Capacity & (Capacity - 1)) == 0), 
                  "Capacity must be a power of 2");

public:
    SPSCRingBuffer() : head_(0), tail_(0) {}

    bool push(const T& item) {
        const size_t current_tail = tail_.load(std::memory_order_relaxed);
        const size_t next_tail = (current_tail + 1) & (Capacity - 1);

        if (next_tail == head_.load(std::memory_order_acquire))
            return false;

        buffer_[current_tail] = item;
        
        tail_.store(next_tail, std::memory_order_release);
        return true;
    }

    bool pop(T& item) {
        const size_t current_head = head_.load(std::memory_order_relaxed);

        if (current_head == tail_.load(std::memory_order_acquire))
            return false;

        item = buffer_[current_head];
        head_.store((current_head + 1) & (Capacity - 1), std::memory_order_release);
        return true;
    }

    size_t size() const {
        size_t h = head_.load(std::memory_order_acquire);
        size_t t = tail_.load(std::memory_order_acquire);
        return (t - h) & (Capacity - 1);
    }

    bool is_approaching_capacity(size_t threshold = Capacity - 128) const {
        return size() >= threshold;
    }

private:
    alignas(CACHE_LINE_SIZE) std::atomic<size_t> head_;
    alignas(CACHE_LINE_SIZE) std::atomic<size_t> tail_;

    alignas(CACHE_LINE_SIZE) std::array<T, Capacity> buffer_;
};