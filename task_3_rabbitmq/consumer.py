"""
consumer code mostly from pika documentation example
"""
import asyncio
import functools
import time
from random import randint
import json
import pika
from pika.adapters.asyncio_connection import AsyncioConnection
from pathlib import Path
from config import config


class ExampleConsumer(object):
    QUEUE = config.queue

    def __init__(self, amqp_url, loop):
        self.should_reconnect = False
        self.was_consuming = False

        self._loop = loop
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = amqp_url
        self._consuming = False
        self._prefetch_count = 1

    def connect(self):
        return AsyncioConnection(
            custom_ioloop=self._loop,
            parameters=pika.URLParameters(self._url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            pass
        else:
            self._connection.close()

    def on_connection_open(self, _unused_connection):
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self.reconnect()

    def reconnect(self):
        self.should_reconnect = True
        self.stop()

    def open_channel(self):
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_queue(self.QUEUE)

    def add_on_channel_close_callback(self):
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        self.close_connection()

    def setup_queue(self, queue_name):
        cb = functools.partial(self.on_queue_declareok, userdata=queue_name)
        self._channel.queue_declare(queue=queue_name, callback=cb)

    def on_queue_declareok(self, _unused_frame, userdata):
        self.set_qos()

    def set_qos(self):
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _unused_frame):
        self.start_consuming()

    def start_consuming(self):
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            self.QUEUE, self.on_message)
        self.was_consuming = True
        self._consuming = True

    def add_on_cancel_callback(self):
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        if self._channel:
            self._channel.close()

    # -------------------------------------- main task is here ------------------------------
    def get_data(self, files: list[Path]) -> list[str]:
        data = []
        for file in files:
            if file.exists():
                with file.open('r', encoding='utf-8') as f:
                    data.append(f.read())
        return data

    def write_data(self, data: list[str], file_name: str) -> None:
        with (config.result_dir / Path(str(file_name))).open('w') as f:
            f.write('\n'.join(data))

    def delete_files(self, files: list[Path]) -> None:
        for file in files:
            if file.exists():
                file.unlink()

    async def _handle_message(self, basic_deliver, properties, body):
        n = randint(1, 10000000)
        print(n, body)
        files = [Path(file) for file in json.loads(body) if Path(file).exists()]
        data = self.get_data(files)
        await asyncio.sleep(35)
        self.write_data(data, basic_deliver.delivery_tag)
        self.delete_files(files)
        print(n, files)

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        self._loop.create_task(self._handle_message(basic_deliver, properties, body))

        # this is a source of bugs
        # we acknowledge message before we know processing result
        # to avoid it we need to track all tasks in separate non-blocking task
        self.acknowledge_message(basic_deliver.delivery_tag)

    # ---------------------------------------------------------------------------------------

    def acknowledge_message(self, delivery_tag):
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        if self._channel:
            cb = functools.partial(
                self.on_cancelok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, cb)

    def on_cancelok(self, _unused_frame, userdata):
        self._consuming = False
        self.close_channel()

    def close_channel(self):
        self._channel.close()

    def run(self):
        self._connection = self.connect()
        self._connection.ioloop.run_forever()

    def stop(self):
        if not self._closing:
            self._closing = True
            if self._consuming:
                self.stop_consuming()
                self._connection.ioloop.run_forever()
            else:
                self._connection.ioloop.stop()


class ReconnectingExampleConsumer(object):

    def __init__(self, amqp_url, loop):
        self._reconnect_delay = 0
        self._amqp_url = amqp_url
        self._consumer = ExampleConsumer(self._amqp_url, loop)

    def run(self):
        while True:
            try:
                self._consumer.run()
            except KeyboardInterrupt:
                self._consumer.stop()
                break
            self._maybe_reconnect()

    def _maybe_reconnect(self):
        if self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            time.sleep(reconnect_delay)
            self._consumer = ExampleConsumer(self._amqp_url)

    def _get_reconnect_delay(self):
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay


def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    consumer = ReconnectingExampleConsumer(config.rabbit_connection, loop)
    consumer.run()


if __name__ == '__main__':
    main()
