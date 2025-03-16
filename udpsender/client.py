import select
import socket
from time import time
from typing import List
from zlib import crc32
from udpsender import *


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
