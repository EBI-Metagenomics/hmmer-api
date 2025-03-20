import socket
import logging
import os
from typing import Optional

from result.models import HmmdSearchStatus, HmmpgmdStatus

logger = logging.getLogger(__name__)


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
        self.socket.connect((self.address, self.port))

    def close(self):
        self.socket.close()

    def search(self, db_cmd: str, parameters: str, query: str, path: Optional[os.PathLike] = None) -> Optional[bytes]:

        # command = f"@{db_cmd} {parameters}\n{query}\n//"
        command = f"@{db_cmd}\n{query}\n//"
        logger.debug(f"Sending command: {command}")

        self.socket.sendall(command.encode("ascii"))

        status_raw = self.socket.recv(HmmdSearchStatus.size())
        status = HmmdSearchStatus.from_bytes(status_raw)

        if status.status != HmmpgmdStatus.OK:
            message = self.socket.recv(status.message_size)
            raise Exception(message.decode())

        if path is not None:
            with open(path, mode="wb") as fh:
                bytes_read = 0

                while bytes_read < status.message_size:
                    chunk = self.socket.recv(min(status.message_size - bytes_read, 2048))
                    fh.write(chunk)
                    bytes_read += len(chunk)
        else:
            bytes_read = 0
            data = b""

            while bytes_read < status.message_size:
                chunk = self.socket.recv(min(status.message_size - bytes_read, 2048))
                bytes_read += len(chunk)
                data += chunk

            return data
