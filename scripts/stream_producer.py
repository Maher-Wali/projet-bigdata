import socket
import time
import csv
import sys

HOST = '0.0.0.0'
PORT = 9999
DELAY = 0.1  # seconds between rows (10 rows/sec)
DATA_FILE = '/home/hadoop/data/fact_trips_final.csv'

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind((HOST, PORT))
server.listen(1)

print(f"Producer listening on port {PORT}, waiting for Spark to connect...")
conn, addr = server.accept()
print(f"Spark connected from {addr}, starting stream...")

try:
    with open(DATA_FILE, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for i, row in enumerate(reader):
            line = ','.join(row) + '\n'
            conn.sendall(line.encode('utf-8'))
            time.sleep(DELAY)
            if (i + 1) % 100 == 0:
                print(f"Sent {i + 1} rows...")
except BrokenPipeError:
    print("Spark disconnected.")
finally:
    conn.close()
    server.close()
