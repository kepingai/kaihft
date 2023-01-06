import logging
from typing import Union
from kaihft.publishers.exchanges import BinanceTickerPublisher
from kaihft.publishers.rabbitmq_client import KaiRabbitPublisherClient
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager


def main(markets: dict,
         production: bool,
         exp0a: bool,
         exp1a: bool,
         topic_path: str = 'layer1-ticker-binance-v0',
         restart_every: Union[int, float] = 60):
    """ Retrieve real-time binance data via websocket &
        then publish binance tickers to Cloud Pub/Sub. 

        Parameters
        ----------
        markets: `dict`
            A dictionary containing the symbols to 
            retrieve data from websocket.
        production: `bool`
            if `True` publisher will publish to production topic.
        exp0a: `bool`
            if `True` publisher will publish to exp0a topic.
        exp1a: `bool`
            if `True` publisher will publish to exp1a topic.
        topic_path: `str`
            The topic path to publish ticker.
        restart_every: `Union[int, float]`
            Restart the ticker pod every X minute(s), default is 60 minutes
    """
    rabbit_broker_url = "amqp://Jr7k2xVus1o1ilzTOg:ZQUi5x0NdFQ5ZcoFvA@35.184.74.57:5672"
    if production: mode = "production"
    elif exp0a: mode = "experiment-0a"
    elif exp1a: mode = "experiment-1a"
    else: mode = "development"
    logging.info(f"[{mode}-mode] tickers-BINANCE-FUTURES, topic: {topic_path}, "
                 f"markets: {markets}.")
    # binance only allows 1024 subscriptions in one stream
    # channels and markets and initiate multiplex stream
    # channels x markets = (total subscription)
    channels = {'kline_1m'}
    # connect to binance.com and create the stream
    # the stream id is returned after calling `create_stream()`
    binance_websocket_api_manager = BinanceWebSocketApiManager(
        exchange="binance.com-futures",
        throw_exception_if_unrepairable=True)
    stream_id = binance_websocket_api_manager.create_stream(
        channels=channels, 
        markets=markets)
    # initialize publisher
    publisher = KaiRabbitPublisherClient(broker_url=rabbit_broker_url)
    # initialize binance ticker publisher
    # and run the publisher.
    ticker_publisher = BinanceTickerPublisher(
        websocket=binance_websocket_api_manager,
        stream_id=stream_id,
        publisher=publisher,
        topic_path=topic_path,
        restart_every=restart_every
    )
    ticker_publisher.run()
