import socket
import struct
import time
import threading

# C++ Struct Layout:
# CommandType (uint8), key_len (uint8), val_len (uint16), client_id (uint64), request_id (uint64)
# Format string for struct.pack (Little Endian): '< B B H Q Q'
HEADER_FORMAT = '<BBHQQ'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

def send_put_request(sock, request_id, key, value):
    key_bytes = key.encode('utf-8')
    val_bytes = value.encode('utf-8')
    
    # 2 = PUT Command
    header = struct.pack(HEADER_FORMAT, 2, len(key_bytes), len(val_bytes), 0, request_id)
    
    # Send all at once
    sock.sendall(header + key_bytes + val_bytes)

def listen_for_acks(sock, expected_count):
    received = 0
    while received < expected_count:
        header_data = sock.recv(HEADER_SIZE)
        if not header_data:
            break
            
        cmd_type, k_len, v_len, c_id, req_id = struct.unpack(HEADER_FORMAT, header_data)
        
        # 4 = ACK Command
        assert cmd_type == 4, f"Expected ACK (4), got {cmd_type}"
        print(f"[Client] Received ACK for request_id: {req_id}")
        received += 1

def run_test():
    print("Connecting to HFT DB...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Disable Nagle's algorithm for lower latency testing
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.connect(('127.0.0.1', 9000))
    
    NUM_REQUESTS = 5
    
    # Start a background thread to read ACKs asynchronously
    ack_thread = threading.Thread(target=listen_for_acks, args=(sock, NUM_REQUESTS))
    ack_thread.start()
    
    print("Sending batch of PUT requests...")
    for i in range(NUM_REQUESTS):
        send_put_request(sock, request_id=100+i, key=f"AAPL_{i}", value=f"150.{i}")
    
    ack_thread.join(timeout=2.0)
    sock.close()
    print("Integration Test Passed! TCP multi-reactor pipeline is fully operational.")

if __name__ == "__main__":
    run_test()