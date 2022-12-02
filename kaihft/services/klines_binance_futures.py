import logging
from kaihft.publishers.exchanges import BinanceKlinesPublisher
from kaihft.publishers.rabbitmq_client import KaiRabbitPublisherClient
from binance.client import Client
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager


def main(
    n_klines: int,
    interval: int,
    markets: dict, 
    production: bool,
    exp0a: bool,
    exp1a: bool,
    topic_path: str = 'layer1-klines-binance-{timeframe}-v0'):
    """ Retrieve real-time binance data via websocket &
        then publish binance klines to Cloud Pub/Sub. 
        
        Parameters
        ----------
        n_klines: `int`
            The number of klines to publish.
        interval: `int`
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
            The topic path to publish klines.
    """
    rabbit_broker_url = "amqp://Jr7k2xVus1o1ilzTOg:ZQUi5x0NdFQ5ZcoFvA@35.184.74.57:5672"
    if production: mode="prediction"
    elif exp0a: mode="experiment-0a"
    elif exp1a: mode="experiment-1a"
    else: mode="development"
    logging.warn(f"[{mode}-mode] tickers-BINANCE-FUTURES, markets: {markets}, "
        f"topic: prod-{topic_path}")
    # binance only allows 1024 subscriptions in one stream
    # channels and markets and initiate multiplex stream
    # channels x markets = (total subscription)
    channels = {'kline_15m'}
    # connect to binance.com and create the stream
    # the stream id is returned after calling `create_stream()`
    binance_websocket_api_manager = BinanceWebSocketApiManager(
        exchange="binance.com-futures",
        throw_exception_if_unrepairable=True)
    stream_id = binance_websocket_api_manager.create_stream(
        channels=channels, 
        markets=markets)
    # initialize publisher
    # initialize binance klines publisher
    # and run the publisher.
    publisher = KaiRabbitPublisherClient(broker_url=rabbit_broker_url)
    klines_publisher = BinanceKlinesPublisher(
        client=Client("", ""),
        websocket=binance_websocket_api_manager,
        stream_id=stream_id,
        publisher=publisher,
        topic_path=topic_path,
        n_klines=n_klines,
        markets=list(markets),
        interval=interval)
    klines_publisher.run()