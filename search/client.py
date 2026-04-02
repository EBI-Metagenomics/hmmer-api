import socket
import logging
import os
import time
from typing import Optional

from result.models import HmmdSearchStatus, HmmpgmdStatus

logger = logging.getLogger(__name__)


class HmmpgmdError(Exception):
    """Base exception for all hmmpgmd errors."""

    def __init__(self, status: HmmpgmdStatus, message: str):
        super().__init__(f"Hmmpgmd error: {message} ({status})")


class HmmpgmdValueError(HmmpgmdError):
    """Invalid argument/parameter or value out of range."""

    pass


class HmmpgmdServerError(HmmpgmdError):
    """Server-related errors."""

    pass


class Client:
    def __init__(self, address="127.0.0.1", port=51371):
        self.address = address
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def __enter__(self):
        self.connect()

        return self

    def __exit__(self, exc_value, exc_type, traceback):
        self.close()

    def connect(self):
        self.socket.settimeout(10)
        self.socket.connect((self.address, self.port))

    def close(self):
        self.socket.close()

    def search(
        self,
        db_cmd: str,
        parameters: str,
        query: str,
        path: Optional[os.PathLike] = None,
        ranges: Optional[str] = None,
    ) -> Optional[bytes]:

        if ranges:
            command = f"@{db_cmd} {ranges} {parameters}\n{query}\n//"
        else:
            command = f"@{db_cmd} {parameters}\n{query}\n//"

        logger.debug(f"Sending command: {command}")
        self.socket.settimeout(60)
        self.socket.sendall(command.encode("ascii"))

        self.socket.settimeout(30 * 60)
        t0 = time.monotonic()
        status_raw = self.socket.recv(HmmdSearchStatus.size())
        status = HmmdSearchStatus.from_bytes(status_raw)
        logger.info(f"Status received in {time.monotonic() - t0:.1f}s, message_size={status.message_size}")

        if status.status != HmmpgmdStatus.OK:
            message = self.socket.recv(status.message_size)
            decoded_message = message.decode().replace("\x00", "")

            if status.status in (
                HmmpgmdStatus.EINVAL,
                HmmpgmdStatus.ERANGE,
                HmmpgmdStatus.ETYPE,
                HmmpgmdStatus.EFORMAT,
            ):
                raise HmmpgmdValueError(status.status, decoded_message)
            else:
                raise HmmpgmdServerError(status.status, decoded_message)

        chunk_size = 65536

        if path is not None:
            with open(path, mode="wb") as fh:
                bytes_read = 0
                t0 = time.monotonic()

                while bytes_read < status.message_size:
                    chunk = self.socket.recv(min(status.message_size - bytes_read, chunk_size))
                    fh.write(chunk)
                    bytes_read += len(chunk)

                elapsed = time.monotonic() - t0
                logger.info(
                    f"Data transfer complete: {bytes_read} bytes in {elapsed:.1f}s "
                    f"({bytes_read / elapsed / 1e6:.1f} MB/s)"
                )
        else:
            bytes_read = 0
            data = b""
            t0 = time.monotonic()

            while bytes_read < status.message_size:
                chunk = self.socket.recv(min(status.message_size - bytes_read, chunk_size))
                bytes_read += len(chunk)
                data += chunk

            elapsed = time.monotonic() - t0
            logger.info(
                f"Data transfer complete: {bytes_read} bytes in {elapsed:.1f}s "
                f"({bytes_read / elapsed / 1e6:.1f} MB/s)"
            )

            return data
