import pika
from pika import DeliveryMode
from pika.exchange_type import ExchangeType
import logging, json
from typing import Optional, Union
from datetime import datetime, timezone
from .pub_clients import KaiPublisher


class KaiRabbitPublisherClient(KaiPublisher):
    """ Rabbit MQ synchronous producer client using pika."""
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
                the KepingAI username

            Attributes
            ----------
            connection: `aio_pika.robust_connection.AbstractRobustConnection`
                the RabbitMQ broker connection object
        """
        super(KaiRabbitPublisherClient, self).__init__(
            publisher_type="rabbit_mq", username=username
        )
        self.broker_url = "" if broker_url is None else broker_url
        # set pika logging level to WARNING
        logging.getLogger(pika.__name__).setLevel(logging.WARNING)
        # create connection parameters
        self.conn_params = pika.URLParameters(self.broker_url)
        logging.info(f"[init] [connection] [publisher] initialized connection "
                     f"to RabbitMQ broker: '{self.broker_url}' ...")

    def publish(
            self,
            origin: str,
            exchange_name: str,
            exchange_type: Union[ExchangeType, str],
            data: dict,
            delivery_mode: DeliveryMode = DeliveryMode.Transient,
            routing_key: str = "",
            attributes: dict = {}
    ):
        """ Publish using rabbitmq synchronously.

            Parameters
            ----------
            origin: `str`
                class name
            exchange_name: `str`
                topic name
            exchange_type: `ExchangeType`
                pika exchange type, such as FANOUT or DIRECT
            delivery_mode: `DeliveryMode`
                pika delivery mode object. PERSISTENT or NOT_PERSISTENT
            data: `dict`
                the data to publish
            routing_key: `str`
                routing key. Automatically use empty string if exchange_type is
                ExchangeType.FANOUT
            attributes: `dict`
                the message headers
        """
        if exchange_type in [ExchangeType.fanout, "fanout"]:
            routing_key = ""
        logging.debug(f"[publishing] {str(exchange_type).upper()} message on exchange: "
                      f"{exchange_name} with routing key: '{routing_key}'.")
        # assign timestamp to header, because pika does NOT support timestamp
        # in the raw number form (int or float)
        attributes.update({
            'origin': origin, 'username': self.username,
            'timestamp': str(datetime.now(tz=timezone.utc).timestamp())
        })
        body = json.dumps({"data": data}).encode('utf-8')

        with pika.BlockingConnection(parameters=self.conn_params) as connection:
            channel = connection.channel()
            channel.exchange_declare(
                exchange=exchange_name, exchange_type=exchange_type, durable=True)
            properties = pika.BasicProperties(
                headers=attributes, delivery_mode=delivery_mode)
            channel.basic_publish(
                exchange=exchange_name, routing_key=routing_key, body=body,
                properties=properties
            )
            logging.debug(f"[publishing] finished publishing data: {body}'")
