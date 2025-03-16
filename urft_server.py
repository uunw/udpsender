import sys

from udpsender import Server
from udpsender import logging

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python urft_server.py <server_ip> <server_port>")
        sys.exit(1)

    server_ip = sys.argv[1]
    server_port = int(sys.argv[2])

    try:
        server = Server(server_ip, server_port)
        server.listen()

    except TimeoutError:
        logging.warning("timout...")

    except ConnectionRefusedError:
        logging.warning("Connection close.")
