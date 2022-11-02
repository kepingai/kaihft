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
            auto_ack: bool = True
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

    async def publish(
            self,
            origin: str,
            exchange_name: str,
            exchange_type: ExchangeType,
            data: dict,
            delivery_mode: DeliveryMode = DeliveryMode.NOT_PERSISTENT,
            routing_key: str = "",
            attributes: dict = {}
    ):
        """ Publish using rabbitmq asynchronously.

            Parameters
            ----------
            origin: `str`
                class name
            exchange_name: `str`
                topic name
            exchange_type: `ExchangeType`
                aio-pika exchange type, such as FANOUT or DIRECT
            delivery_mode: `DeliveryMode`
                aio-pika delivery mode object. PERSISTENT or NOT_PERSISTENT
            data: `dict`
                the data to publish
            routing_key: `str`
                routing key. Automatically use empty string if exchange_type is
                ExchangeType.FANOUT
            attributes: `dict`
                the message headers
        """
        if exchange_type == ExchangeType.FANOUT:
            routing_key = ""
        logging.debug(f"[publishing] {str(exchange_type)} message on exchange: "
                      f"{exchange_name} with routing key: '{routing_key}'.")
        attributes.update({'origin': origin, 'username': self.username})
        body = json.dumps({"data": data}).encode('utf-8')

        async with self.connection.channel() as channel:
            exchange = await channel.declare_exchange(
                name=exchange_name, type=exchange_type, durable=True)
            message = Message(
                body=body, headers=attributes, delivery_mode=delivery_mode,
                timestamp=datetime.now(tz=timezone.utc).timestamp()
            )
            await exchange.publish(message, routing_key=routing_key)
            logging.debug(f"[publishing] finished publishing data: {body}'")

    async def close(self):
        """ Close the RabbitMQ client connection. """
        await self.connection.close()
