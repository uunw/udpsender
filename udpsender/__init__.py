from enum import Enum
import logging
from os import path
from typing import Tuple, Union

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
