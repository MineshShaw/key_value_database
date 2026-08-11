#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <string>
#include "../src/storage/bloom_filter.h"
#include "../src/storage/sstable.h"

// ============================================================================
// BLOOM FILTER TESTS
// ============================================================================

TEST(BloomFilterTest, TruePositivesAndNegatives) {
    BloomFilter bf(100); // Expected 100 entries
    
    bf.add("AAPL");
    bf.add("MSFT");
    bf.add("GOOG");

    // Must be 100% accurate for keys that exist
    EXPECT_TRUE(bf.might_contain("AAPL"));
    EXPECT_TRUE(bf.might_contain("MSFT"));
    EXPECT_TRUE(bf.might_contain("GOOG"));

    // Should ideally be false for keys that don't exist
    EXPECT_FALSE(bf.might_contain("TSLA"));
    EXPECT_FALSE(bf.might_contain("AMZN"));
}

TEST(BloomFilterTest, FalsePositiveRate) {
    constexpr int NUM_KEYS = 10000;
    BloomFilter bf(NUM_KEYS, 10.0); // 10 bits per key should yield ~1% FP rate

    // Insert 10,000 keys
    for (int i = 0; i < NUM_KEYS; i++) {
        bf.add("key_" + std::to_string(i));
    }

    // Check 10,000 completely different keys
    int false_positives = 0;
    for (int i = NUM_KEYS; i < NUM_KEYS * 2; i++) {
        if (bf.might_contain("key_" + std::to_string(i))) {
            false_positives++;
        }
    }

    // A perfect 10-bit bloom filter has a ~1.2% false positive rate.
    // We assert that ours is well under 2% (200 out of 10,000).
    EXPECT_LT(false_positives, 200) << "False positive rate too high: " << false_positives;
}

// ============================================================================
// SSTABLE BUILDER TESTS
// ============================================================================

TEST(SSTableBuilderTest, BuildAndVerifyFooter) {
    const std::string test_file = "test_table_01.sst";

    // 1. Build the SSTable
    {
        SSTableBuilder builder(test_file, 1000);
        
        // Keys must be added in strictly sorted order
        builder.add("AAPL", "150.00");
        builder.add("AMZN", "3300.00");
        builder.add("GOOG", "2800.50");
        builder.add("MSFT", "310.25");
        builder.add("TSLA", "700.00");
        
        builder.finish();
    }

    // 2. Verify File Layout on Disk
    ASSERT_TRUE(std::filesystem::exists(test_file));
    
    std::ifstream in(test_file, std::ios::binary | std::ios::ate);
    ASSERT_TRUE(in.is_open());
    
    std::streamsize file_size = in.tellg();
    ASSERT_GE(file_size, sizeof(SSTableFooter));

    // 3. Read and verify the Footer
    in.seekg(file_size - sizeof(SSTableFooter), std::ios::beg);
    SSTableFooter footer;
    in.read(reinterpret_cast<char*>(&footer), sizeof(footer));

    EXPECT_EQ(footer.magic_number, 0xDEADBEEF);
    
    // Validate offsets make chronological sense
    EXPECT_GT(footer.index_offset, 0);                 // Index must be after data
    EXPECT_GT(footer.bloom_offset, footer.index_offset); // Bloom must be after index
    EXPECT_GT(footer.bloom_size, 0);

    // Clean up test artifact
    in.close();
    std::filesystem::remove(test_file);
}

TEST(SSTableBuilderTest, SparseIndexGeneration) {
    const std::string test_file = "test_table_02.sst";

    {
        SSTableBuilder builder(test_file, 1000);
        // Add 150 keys (Should generate exactly 3 sparse index entries if interval is 64)
        for (int i = 100; i < 250; i++) {
            builder.add("key_" + std::to_string(i), "value_" + std::to_string(i));
        }
        builder.finish();
    }

    std::ifstream in(test_file, std::ios::binary | std::ios::ate);
    std::streamsize file_size = in.tellg();
    
    in.seekg(file_size - sizeof(SSTableFooter), std::ios::beg);
    SSTableFooter footer;
    in.read(reinterpret_cast<char*>(&footer), sizeof(footer));

    // Calculate sparse index entry counts
    // An entry is: [key_len (4 bytes)] + [key (7 bytes for "key_xxx")] + [offset (8 bytes)] = 19 bytes per entry
    EXPECT_GT(footer.index_size, 0);
    EXPECT_EQ(footer.index_size % 19, 0);
    EXPECT_EQ(footer.index_size / 19, 3); // Keys at index 0, 64, 128

    in.close();
    std::filesystem::remove(test_file);
}