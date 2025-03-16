from enum import Enum
import logging
from os import path, unlink
import select
import socket
from time import time
from typing import List, OrderedDict, Tuple, Union
from zlib import crc32

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)


class SEGMENT_TYPE(Enum):
    INIT = 1
    DATA = 2
    ACK = 3


HEADER_BYTEORDER = "big"
HEADER_TYPE_LENGTH = 1
HEADER_SEQUENCE_LENGTH = 4
HEADER_CHECKSUM_LENGTH = 4
HEADER_FILESIZE_LENGTH = 4

MAX_SEGMENT_SIZE: int = 1024
HEADER_SIZE: int = HEADER_TYPE_LENGTH + HEADER_SEQUENCE_LENGTH + HEADER_CHECKSUM_LENGTH
MAX_PAYLOAD_SIZE: int = MAX_SEGMENT_SIZE - HEADER_SIZE
ACK_SEGMENT_SIZE = HEADER_TYPE_LENGTH + HEADER_SEQUENCE_LENGTH

LOGGING: bool = True
LOSS_TIMEOUT: float = 3.0
CONSECUTIVE_PACKETS_TIMEOUT: float = 1
EXIT_DELAY: float = LOSS_TIMEOUT + CONSECUTIVE_PACKETS_TIMEOUT
CONNECTION_END_NULLS_COUNT: int = 10
MAX_ACK_PER_BATCH: int = 200

INIT_SEQUENCE_NUMBER = int.from_bytes(b"ffff", HEADER_BYTEORDER)


class Utils:
    @staticmethod
    def encode_init(filesize: int, filename: str) -> bytes:
        segment = bytes()
        segment += SEGMENT_TYPE.INIT.value.to_bytes(
            HEADER_TYPE_LENGTH, HEADER_BYTEORDER
        )

        segment += filesize.to_bytes(HEADER_FILESIZE_LENGTH, HEADER_BYTEORDER)
        segment += filename.encode()

        return segment

    @staticmethod
    def encode_data_headers(sequence_number: int, crc32sum: int) -> bytes:
        headers = bytes()
        headers += SEGMENT_TYPE.DATA.value.to_bytes(
            HEADER_TYPE_LENGTH, HEADER_BYTEORDER
        )

        headers += sequence_number.to_bytes(HEADER_SEQUENCE_LENGTH, HEADER_BYTEORDER)
        headers += crc32sum.to_bytes(HEADER_CHECKSUM_LENGTH, HEADER_BYTEORDER)

        return headers

    @staticmethod
    def encode_ack(sequence_number: int) -> bytes:
        segment = bytes()
        segment += SEGMENT_TYPE.ACK.value.to_bytes(HEADER_TYPE_LENGTH, HEADER_BYTEORDER)
        segment += sequence_number.to_bytes(HEADER_SEQUENCE_LENGTH, HEADER_BYTEORDER)

        return segment

    @staticmethod
    def decode_init(segment: bytes) -> Union[Tuple[int, str], None]:
        # segment_type = SEGMENT_TYPE(
        #     int.from_bytes(segment[0:HEADER_TYPE_LENGTH], HEADER_BYTEORDER)
        # )
        segment_type = SEGMENT_TYPE(
            int.from_bytes(segment[0:HEADER_TYPE_LENGTH], HEADER_BYTEORDER)
        )

        if segment_type != SEGMENT_TYPE.INIT:
            return None

        filesize = int.from_bytes(
            segment[HEADER_TYPE_LENGTH : HEADER_TYPE_LENGTH + HEADER_FILESIZE_LENGTH :],
            HEADER_BYTEORDER,
        )
        filename = path.basename(
            segment[HEADER_TYPE_LENGTH + HEADER_FILESIZE_LENGTH : :].decode()
        )

        return (filesize, filename)

    @staticmethod
    def decode_data_headers(headers: bytes) -> Tuple[SEGMENT_TYPE, int, int]:
        segment_type = SEGMENT_TYPE(
            int.from_bytes(headers[0:HEADER_TYPE_LENGTH], HEADER_BYTEORDER)
        )
        sequence_number = int.from_bytes(
            headers[HEADER_TYPE_LENGTH : HEADER_TYPE_LENGTH + HEADER_SEQUENCE_LENGTH :],
            HEADER_BYTEORDER,
        )
        crc32sum = int.from_bytes(
            headers[
                HEADER_TYPE_LENGTH
                + HEADER_SEQUENCE_LENGTH : HEADER_TYPE_LENGTH
                + HEADER_SEQUENCE_LENGTH
                + HEADER_CHECKSUM_LENGTH :
            ],
            HEADER_BYTEORDER,
        )

        return (segment_type, sequence_number, crc32sum)

    @staticmethod
    def decode_ack(segment: bytes) -> Union[int, None]:
        segment_type = SEGMENT_TYPE(
            int.from_bytes(segment[0:HEADER_TYPE_LENGTH], HEADER_BYTEORDER)
        )
        if segment_type != SEGMENT_TYPE.ACK:
            return None

        sequence_number = int.from_bytes(
            segment[HEADER_TYPE_LENGTH : HEADER_TYPE_LENGTH + HEADER_SEQUENCE_LENGTH :],
            HEADER_BYTEORDER,
        )

        return sequence_number


class InflightSegment:
    def __init__(self, segment_number: int, resend_epoch: float) -> None:
        self.segment_number = segment_number
        self.resend_epoch = resend_epoch


class Client:
    def __init__(self, server_ip: str, server_port: int):
        self.__server_ip = server_ip
        self.__server_port = server_port
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.__socket.connect((self.__server_ip, self.__server_port))
        self.__socket.settimeout(1.5 * LOSS_TIMEOUT)

        self.__segment_inflight: List[InflightSegment] = []

    def __send_segment(self, segment_number: int, payload: bytes):
        checksum = crc32(payload)
        headers = Utils.encode_data_headers(segment_number, checksum)
        segment = headers + payload

        resend_epoch = time() + LOSS_TIMEOUT
        self.__segment_inflight.append(InflightSegment(segment_number, resend_epoch))

        logging.debug(f"sending #{segment_number}({checksum})")
        logging.debug(f"sending #{segment_number}({checksum}) {payload}")

        self.__socket.send(segment)

    def send_file(self, filepath: str):
        segment_number = 0
        filesize = path.getsize(filepath)
        filename = path.basename(filepath)

        logging.info("Filename {}".format(filename))
        logging.debug("Size {}".format(filesize))

        with open(filepath, "rb") as file:
            # init
            init_acked = False
            resend_epoch = 0
            while init_acked is False:
                if time() > resend_epoch:
                    payload = Utils.encode_init(filesize, filename)
                    logging.debug(f"sending init")
                    # logging.debug(f'init payload {payload}')
                    self.__socket.send(payload)
                    resend_epoch = time() + LOSS_TIMEOUT
                    # self.__segment_inflight.append((INIT_SEQUENCE_NUMBER))

                # wait until init is acked
                ready = select.select(
                    [self.__socket], [], [], CONSECUTIVE_PACKETS_TIMEOUT
                )
                if ready[0]:
                    segment, _ = self.__socket.recvfrom(MAX_SEGMENT_SIZE)
                    if segment == b"":
                        continue
                    sequence_number = Utils.decode_ack(segment)
                    if sequence_number is None:
                        continue
                    if sequence_number == INIT_SEQUENCE_NUMBER:
                        logging.debug(f"got acked for init ({sequence_number})")
                        break

            # first pass of data
            while payload := file.read(MAX_PAYLOAD_SIZE):
                payload_length = len(payload)
                self.__send_segment(segment_number=segment_number, payload=payload)
                segment_number += payload_length

                self.__segment_inflight = sorted(
                    self.__segment_inflight, key=lambda x: x.resend_epoch
                )

            # recieve ack and resend if necessary
            while self.__segment_inflight:
                resend_segment = self.__segment_inflight[0]
                logging.debug(f"now {time()}")
                logging.debug(f"resend queue at {resend_segment.resend_epoch}")

                if time() > resend_segment.resend_epoch:
                    segment = self.__segment_inflight.pop(0)
                    logging.info(f"resending {segment.segment_number}")

                    # Calculate the correct file position for this sequence number
                    # You need to track original file positions or calculate them
                    file_position = (
                        segment.segment_number
                    )  # This needs to be the correct file position!
                    file.seek(file_position)

                    payload = file.read(MAX_PAYLOAD_SIZE)
                    self.__send_segment(
                        segment_number=segment.segment_number, payload=payload
                    )

                    # Don't discard resent segments - add them back to the inflight queue with a new timeout
                    # self.__segment_inflight.append(InflightSegment(segment.segment_number, time() + LOSS_TIMEOUT))

                ready = select.select(
                    [self.__socket], [], [], CONSECUTIVE_PACKETS_TIMEOUT
                )
                if ready[0]:
                    segment, _ = self.__socket.recvfrom(MAX_SEGMENT_SIZE)
                    if segment == b"":
                        continue
                    sequence_number = Utils.decode_ack(segment)
                    if sequence_number is None:
                        continue

                    try:
                        logging.info(f"recieved ack for {sequence_number}")
                        # logging.info(f'inflights {list(map(lambda x: x.segment_number, self.__segment_inflight))}')
                        self.__segment_inflight = [
                            seg
                            for seg in self.__segment_inflight
                            if seg.segment_number != sequence_number
                        ]
                    except:
                        logging.exception(
                            f"cant pop {sequence_number} from inflight segment"
                        )

            logging.info("Send file {} success.".format(filename))

            self.__socket.close()


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
