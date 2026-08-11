import socket
import struct
import threading

HEADER_FORMAT = '<BBHQQ'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

def send_put_request(sock, request_id, key, value):
    key_bytes = key.encode('utf-8')
    val_bytes = value.encode('utf-8')
    header = struct.pack(HEADER_FORMAT, 2, len(key_bytes), len(val_bytes), 0, request_id)
    sock.sendall(header + key_bytes + val_bytes)

def listen_for_acks(sock, expected_count):
    received = 0
    while received < expected_count:
        header_data = sock.recv(HEADER_SIZE)
        if not header_data:
            break
        cmd_type, k_len, v_len, c_id, req_id = struct.unpack(HEADER_FORMAT, header_data)
        received += 1
        if received % 1000 == 0:
            print(f"[Client] Received {received}/{expected_count} ACKs...")

def run_test():
    print("Connecting to HFT DB...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.connect(('127.0.0.1', 9000))
    
    # 5,000 requests * ~100 bytes each = ~500KB. 
    # This will trigger several 64KB flushes!
    NUM_REQUESTS = 5000 
    
    ack_thread = threading.Thread(target=listen_for_acks, args=(sock, NUM_REQUESTS))
    ack_thread.start()
    
    print(f"Sending {NUM_REQUESTS} PUT requests to trigger MemTable flush...")
    for i in range(NUM_REQUESTS):
        # Making the value artificially large to hit the threshold faster
        send_put_request(sock, request_id=i, key=f"KEY_{i}", value=f"HFT_TICK_DATA_{i}_" * 5)
    
    ack_thread.join(timeout=5.0)
    sock.close()
    print("Stress test complete! Check your terminal for Flusher logs.")

if __name__ == "__main__":
    run_test()