#pragma once
#include <cstdint>
#include <string>
#include <cstring>

namespace utils {

inline uint64_t murmur_hash_64(const void* key, int len, uint64_t seed = 0x9747b28c) {
    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;

    uint64_t h = seed ^ (len * m);
    const uint64_t* data = (const uint64_t*)key;
    const uint64_t* end = data + (len / 8);

    while (data != end) {
        uint64_t k;
        std::memcpy(&k, data, sizeof(uint64_t)); 
        data++;

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
    }

    const uint8_t* data2 = (const uint8_t*)data;
    switch (len & 7) {
        case 7: h ^= uint64_t(data2[6]) << 48; [[fallthrough]];
        case 6: h ^= uint64_t(data2[5]) << 40; [[fallthrough]];
        case 5: h ^= uint64_t(data2[4]) << 32; [[fallthrough]];
        case 4: h ^= uint64_t(data2[3]) << 24; [[fallthrough]];
        case 3: h ^= uint64_t(data2[2]) << 16; [[fallthrough]];
        case 2: h ^= uint64_t(data2[1]) << 8;  [[fallthrough]];
        case 1: h ^= uint64_t(data2[0]);
                h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
}

} 