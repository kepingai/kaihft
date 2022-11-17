import aio_pika
from aio_pika import ExchangeType, Message, DeliveryMode
import logging, json
from typing import Optional
from datetime import datetime, timezone
from .sub_clients import KaiSubscriber


class KaiRabbitSubscriberClient(KaiSubscriber):
    """ Rabbit MQ asynchronous producer/consumer client using aio-pika."""
    def __init__(
            self,
            broker_url: Optional[str] = None,
            username: str = 'service-account-kbg'
    ):
        """
            Parameters
            ----------
            broker_url: `str`
                rabbitmq broker url
                format:
                    scheme://username:password@host:port
                    example: amqp://user:kepingai@rabbit-broker:5762
            username: `str`

            Attributes
            ----------
            connection: `aio_pika.robust_connection.AbstractRobustConnection`
                the RabbitMQ broker connection object
        """
        super(KaiRabbitSubscriberClient, self).__init__(subscriber_type="rabbit_mq")
        self.broker_url = broker_url
        self.username = username

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.broker_url)
        logging.info(f"[init] [connection] [subscriber] initialized connection "
                     f"to RabbitMQ broker: '{self.broker_url}' ...")

    async def subscribe(
            self,
            callback: callable,
            exchange_name: str,
            queue_name: Optional[str] = None,
            binding_key: Optional[str] = None,
            auto_ack: bool = False
    ):
        """ Subscribe messages asynchronously.

            Parameters
            ----------
            callback: `callable`
                The callback function.
            exchange_name: `str`
                The exchange name to bind the queue with.
            queue_name: `Optional[str]`
                The queue name. The default value is None, hence auto-generated.
            binding_key: `Optional[str]`
                The queue's binding key.
            auto_ack: `bool`
                Automatically ack the pulled message.
        """
        logging.info(f"[subscription] consuming for messages on queue: "
                     f"{queue_name} and exchange: {exchange_name}.")
        channel = await self.connection.channel()
        await channel.set_qos(prefetch_count=10)  # set max 10 messages

        # declare queue and exchange
        exchange = await channel.get_exchange(
            name=exchange_name, ensure=False)
        queue = await channel.declare_queue(
            name=queue_name, auto_delete=True, exclusive=True, durable=True)

        # queue bind and consume
        logging.info(f"[subscription] start consuming queue with routing key: "
                     f"{binding_key} and auto ack: {auto_ack}.")
        await queue.bind(exchange=exchange, routing_key=binding_key)
        await queue.consume(callback=callback, no_ack=auto_ack)

    async def close(self):
        """ Close the RabbitMQ client connection. """
        await self.connection.close()
