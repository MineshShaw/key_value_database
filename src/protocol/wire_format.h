#pragma once
#include <cstdint>

enum class CommandType : uint8_t { GET = 1, PUT = 2, DEL = 3, ACK = 4 };

#pragma pack(push, 1)
struct MsgHeader {
    CommandType type;
    uint8_t key_len;        // Max key size 255 bytes
    uint16_t val_len;       // Max value size 65535 bytes
    uint64_t client_id;     // Assigned by Boss thread to route ACKs
    uint64_t request_id;    // Client's internal tracking ID
};
#pragma pack(pop)

struct Command {
    MsgHeader header;
    int client_fd;          // Used by worker to send response
    char key[256];          
    char value[1024];       // Arbitrary limit for Phase 1
    int status_code;        // 0 = Success, 1 = Not Found, etc.
};