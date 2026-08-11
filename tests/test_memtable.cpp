#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <string>
#include "../src/storage/memtable.h"

TEST(MemTableTest, BasicPutAndGet) {
    MemTable memtable;
    memtable.put("AAPL", "150.00");
    memtable.put("MSFT", "310.50");

    std::string val;
    EXPECT_TRUE(memtable.get("AAPL", val));
    EXPECT_EQ(val, "150.00");

    EXPECT_TRUE(memtable.get("MSFT", val));
    EXPECT_EQ(val, "310.50");

    // Key not found
    EXPECT_FALSE(memtable.get("TSLA", val));
}

TEST(MemTableTest, OverwriteKey) {
    MemTable memtable;
    memtable.put("GOOG", "2800.00");
    
    std::string val;
    EXPECT_TRUE(memtable.get("GOOG", val));
    EXPECT_EQ(val, "2800.00");

    // Overwrite
    memtable.put("GOOG", "2805.50");
    EXPECT_TRUE(memtable.get("GOOG", val));
    EXPECT_EQ(val, "2805.50");
}

TEST(MemTableTest, ConcurrentSingleWriterMultiReader) {
    // Validate our lock-free SWMR logic with std::atomic pointers
    MemTable memtable;
    
    // Writer Thread (Simulates Storage Thread)
    std::thread writer([&memtable]() {
        for (int i = 0; i < 5000; i++) {
            memtable.put("key_" + std::to_string(i), "val_" + std::to_string(i));
        }
    });

    // Reader Thread (Simulates Flush/Compaction Thread)
    std::thread reader([&memtable]() {
        for (int i = 0; i < 5000; i++) {
            std::string val;
            // The value might not be there yet, but reading shouldn't segfault!
            memtable.get("key_" + std::to_string(i), val);
        }
    });

    writer.join();
    reader.join();

    // After join, the 4999th key MUST exist.
    std::string final_val;
    EXPECT_TRUE(memtable.get("key_4999", final_val));
    EXPECT_EQ(final_val, "val_4999");
}