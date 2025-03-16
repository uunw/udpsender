from os import path, unlink
import select
import socket
from time import time
from typing import List, OrderedDict, Tuple, Union
from zlib import crc32

from .utils import *


class Server:
    def __init__(self, server_ip: str, server_port: int = 12345):
        self.__server_ip = server_ip
        self.__server_port = server_port

        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self.__socket.bind((self.__server_ip, self.__server_port))
        self.__socket.settimeout(1.5 * LOSS_TIMEOUT)

        self.__segments: OrderedDict[int, bytes] = OrderedDict()
        self.__pending_ack: List[int] = []
        self.__acked: List[int] = []

        self.__send_ack_at: float = -1
        self.__recieving: bool = True

    def listen(self):
        nulls: int = 0
        possible_nulls: List[bytes] = []
        return_address = None
        next_segment: int = 0

        filename: Union[str, None] = None
        filesize: Union[int, None] = None

        logging.info(
            "Server is listening on {}:{}".format(self.__server_ip, self.__server_port)
        )

        while filename is None or filesize is None:
            ready = select.select([self.__socket], [], [], CONSECUTIVE_PACKETS_TIMEOUT)

            if ready[0] and self.__recieving:
                segment, return_address = self.__socket.recvfrom(MAX_SEGMENT_SIZE)
                logging.debug(f"data {segment}")
                logging.debug(f"nulls {nulls}")

                if segment == b"":
                    continue

                logging.debug(f"init segment {segment}")
                result = Utils.decode_init(segment)

                if result is None:
                    continue

                filesize, filename = result

                logging.info(f"filesize {filesize}")
                logging.info(f"filename {filename}")

                self.__pending_ack.append(INIT_SEQUENCE_NUMBER)
                self.__send_acks(return_address)

        if path.exists(filename):
            unlink(filename)

        with open(filename, "ab") as file:
            while not next_segment >= filesize:
                if next_segment >= filesize:
                    if return_address is not None:
                        logging.info("sending ack before exiting")
                        self.__send_acks(return_address)
                    logging.info("recieved all segment, exiting")
                    break

                ready = select.select(
                    [self.__socket], [], [], CONSECUTIVE_PACKETS_TIMEOUT
                )
                if ready[0] and self.__recieving:
                    segment, return_address = self.__socket.recvfrom(MAX_SEGMENT_SIZE)

                    if segment == b"":
                        if nulls <= CONNECTION_END_NULLS_COUNT:
                            nulls += 1
                            possible_nulls.append(segment)
                        else:
                            logging.warning("Closing connection after recieving nulls.")
                            break

                    if next_segment >= filesize:
                        continue

                    self.__send_ack_at = time() + CONSECUTIVE_PACKETS_TIMEOUT

                    if not self.__process_segment(segment):
                        continue

                    if nulls != 0:
                        nulls = 0

                        for segment in possible_nulls:
                            self.__process_segment(segment)

                        possible_nulls = []

                    while next_segment in self.__segments.keys():
                        logging.debug(f"got {next_segment}")

                        segment = self.__segments.pop(next_segment)
                        next_segment += len(segment)

                        logging.debug(f"next should be {next_segment}")
                        file.write(segment)

                if (
                    self.__send_ack_at != -1
                    and time() >= self.__send_ack_at
                    and return_address is not None
                ):
                    logging.info("sending backlogged ack")
                    self.__send_acks(return_address)

    def __send_acks(self, address: Tuple[str, int]):
        logging.info(f"sending acks for {self.__pending_ack}")
        self.__recieving = False
        this_batch = 0
        processed_acks = []

        try:
            for sequence_number in self.__pending_ack:
                segment = Utils.encode_ack(sequence_number=sequence_number)
                logging.debug(f"sending ack for {sequence_number}")

                self.__socket.sendto(segment, address)
                self.__acked.append(sequence_number)
                processed_acks.append(sequence_number)
                this_batch += 1
                if this_batch >= MAX_ACK_PER_BATCH:
                    break

            for ack in processed_acks:
                if ack in self.__pending_ack:
                    self.__pending_ack.remove(ack)
        except Exception as exc:
            logging.error(exc)
        self.__recieving = True

    def __process_segment(self, segment: bytes) -> bool:
        type, sequence_number, claimed_crc32sum = Utils.decode_data_headers(segment)
        payload = segment[HEADER_SIZE::]

        logging.debug("payload {}".format(payload))

        if sequence_number in self.__acked:
            index = self.__acked.index(sequence_number)
            self.__acked.pop(index)
            self.__pending_ack.append(sequence_number)

            return True

        if crc32(payload) != claimed_crc32sum:
            logging.warning(f"crc mismatch {claimed_crc32sum} {crc32(payload)}")

            return False

        logging.debug(f"segment #{sequence_number} {payload}")

        self.__segments[sequence_number] = payload
        self.__pending_ack.append(sequence_number)

        return True
