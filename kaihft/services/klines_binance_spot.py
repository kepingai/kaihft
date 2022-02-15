import logging
from kaihft.publishers.exchanges import BinanceKlinesPublisher
from kaihft.publishers.client import KaiPublisherClient
from binance.client import Client
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager

def main(
    n_klines: int, 
    markets: dict, 
    production: bool,
    exp0a: bool,
    exp1a: bool,
    topic_path: str = 'klines-binance-v0'):
    """ Retrieve real-time binance data via websocket &
        then publish binance klines to Cloud Pub/Sub. 
        
        Parameters
        ----------
        n_klines: `int`
            The number of klines to publish.
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
    if production: topic_path = f'prod-{topic_path}'; mode="prediction"
    elif exp0a: topic_path = f'exp0a-{topic_path}'; mode="experiment-0a"
    elif exp1a: topic_path = f'exp1a-{topic_path}'; mode="experiment-1a"
    else: topic_path = f'dev-{topic_path}'; mode="development"
    logging.warn(f"[{mode}-mode] tickers-BINANCE-SPOT, markets: {markets}, "
        f"topic: prod-{topic_path}")
    # binance only allows 1024 subscriptions in one stream
    # channels and markets and initiate multiplex stream
    # channels x markets = (total subscription)
    channels = {'kline_15m'}
    # connect to binance.com and create the stream
    # the stream id is returned after calling `create_stream()`
    binance_websocket_api_manager = BinanceWebSocketApiManager(
        exchange="binance.com",
        throw_exception_if_unrepairable=True)
    stream_id = binance_websocket_api_manager.create_stream(
        channels=channels, 
        markets=markets)
    # initialize publisher
    publisher = KaiPublisherClient()
    # initialize binance klines publisher
    # and run the publisher.
    klines_publisher = BinanceKlinesPublisher(
        client=Client("",""),
        websocket=binance_websocket_api_manager,
        stream_id=stream_id,
        publisher=publisher,
        topic_path=topic_path,
        n_klines=n_klines,
        markets=list(markets))
    klines_publisher.run()