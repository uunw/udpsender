import socket
import sys


from udpsender.client import Client
from udpsender import logging

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python urft_client.py <file_path> <server_ip> <server_port>")
        sys.exit(1)

    try:
        file_path = sys.argv[1]
        server_ip = sys.argv[2]
        server_port = int(sys.argv[3])
    except Exception:
        print("3 arguments required: filename server_ip server_port")
        sys.exit()

    try:
        client = Client(server_ip=server_ip, server_port=server_port)
        client.send_file(file_path)
    except TimeoutError:
        logging.warning("timeout... bailing")
    except ConnectionRefusedError:
        logging.warning("connection closed... bailing")
