#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>
#include "../src/net/spsc_queue.h"
#include "../src/protocol/wire_format.h"

TEST(SPSCQueueTest, BasicPushPop) {
    SPSCRingBuffer<Command, 4> queue; // Capacity must be power of 2
    Command cmd1{};
    cmd1.header.request_id = 100;
    
    EXPECT_TRUE(queue.push(cmd1));
    EXPECT_EQ(queue.size(), 1);

    Command out{};
    EXPECT_TRUE(queue.pop(out));
    EXPECT_EQ(out.header.request_id, 100);
    EXPECT_EQ(queue.size(), 0);
}

TEST(SPSCQueueTest, BackpressureOnFullQueue) {
    SPSCRingBuffer<Command, 4> queue; 
    Command cmd{};

    // Push 3 items (Max capacity)
    EXPECT_TRUE(queue.push(cmd));
    EXPECT_TRUE(queue.push(cmd));
    EXPECT_TRUE(queue.push(cmd));

    // 5th push should fail instantly, triggering backpressure
    EXPECT_FALSE(queue.push(cmd)); 
    EXPECT_TRUE(queue.is_approaching_capacity(3));
}

TEST(SPSCQueueTest, LockFreeMultiThreadedStress) {
    constexpr size_t NUM_MESSAGES = 1'000'000;
    SPSCRingBuffer<Command, 1024> queue;
    std::atomic<size_t> received_count{0};

    // Consumer Thread (Simulating Storage Engine)
    std::thread consumer([&]() {
        Command out{};
        for (size_t i = 0; i < NUM_MESSAGES; ++i) {
            while (!queue.pop(out)) {
                // Spin-wait until producer writes
            }
            EXPECT_EQ(out.header.request_id, i);
            received_count.fetch_add(1, std::memory_order_relaxed);
        }
    });

    // Producer Thread (Simulating Network Worker)
    std::thread producer([&]() {
        Command cmd{};
        for (size_t i = 0; i < NUM_MESSAGES; ++i) {
            cmd.header.request_id = i;
            while (!queue.push(cmd)) {
                // Spin-wait if queue is full
            }
        }
    });

    producer.join();
    consumer.join();

    EXPECT_EQ(received_count.load(), NUM_MESSAGES);
}