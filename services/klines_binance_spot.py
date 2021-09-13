import logging
from publishers.exchanges import BinanceKlinesPublisher
from publishers.client import KaiPublisherClient
from binance.client import Client
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager

def main(
    publisher: KaiPublisherClient, 
    n_klines: int, 
    markets: dict, 
    production: bool,
    topic_path: str = 'klines-binance-v0'):
    """ Retrieve real-time binance data via websocket &
        then publish binance klines to Cloud Pub/Sub. 
        
        Parameters
        ----------
        publisher: `KaiPublisherClient`
            Is a Cloud Pub/Sub Client.
        n_klines: `int`
            The number of klines to publish.
        markets: `dict`
            A dictionary containing the symbols to 
            retrieve data from websocket.
        production: `bool`
            if `True` publisher will publish to production topic.
        topic_path: `str`
            The topic path to publish klines.
    """
    if production:
        topic_path = f"prod-{topic_path}"
        logging.warn(f"[production-mode] klines: {n_klines}-BINANCE-SPOT, markets: {markets}, topic: prod-{topic_path}")
    else: topic_path = f'dev-{topic_path}'
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