import json
from pathlib import Path
from time import sleep
from typing import Protocol

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exchange_type import ExchangeType
from config import config


class Publisher(Protocol):
    def publish(self, queue: str, message: bytes) -> None:
        ...


class BlockingPublisher:

    def __init__(self, connection_url: str) -> None:
        self.parameters = pika.URLParameters(connection_url)
        self._connection: pika.BlockingConnection | None = None
        self._channel: BlockingChannel | None = None

    @property
    def connection(self) -> pika.BlockingConnection:
        if not self._connection or not self._connection.is_open:
            self._connection = pika.BlockingConnection(self.parameters)
        return self._connection

    @property
    def channel(self) -> BlockingChannel:
        if not self._channel or not self._channel.is_open:
            self._channel = self.connection.channel()
        return self._channel

    def declare_queue(self, queue: str):
        return self.channel.queue_declare(queue=queue)

    def publish(self, queue: str, message: bytes) -> None:
        self._channel.basic_publish(routing_key=queue, body=message, exchange='')

    def close(self) -> None:
        if self._channel and self._channel.is_open:
            self._channel.close()
        if self._connection and self._connection.is_open:
            self._connection.close()


def get_files(folder: Path) -> list[str]:
    return [str(file) for file in folder.iterdir()]


def file_sender(publisher: Publisher, folder: Path, queue: str, delay: int) -> None:
    while True:
        files = get_files(folder)
        if files:
            publisher.publish(queue, json.dumps(files).encode('utf-8'))
            sleep(delay)


if __name__ == '__main__':
    publisher = BlockingPublisher(config.rabbit_connection)
    if publisher.declare_queue(config.queue):
        try:
            file_sender(publisher, config.wk_dir, config.queue, config.publisher_delay)
        except Exception as e:
            pass
    publisher.close()
