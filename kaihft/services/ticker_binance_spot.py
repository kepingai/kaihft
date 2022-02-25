import logging
from kaihft.publishers.exchanges import BinanceSpotTickerPublisher
from kaihft.publishers.client import KaiPublisherClient
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager


def main(production: bool,
         topic_path: str = 'ticker-binance-v0'):
    """ Retrieve real-time binance data via websocket &
        then publish binance klines to Cloud Pub/Sub.

        Parameters
        ----------
        production: `bool`
            if `True` publisher will publish to production topic.
        topic_path: `str`
            The topic path to publish klines.
    """
    if production:
        topic_path = f"prod-{topic_path}"
        logging.warn(f"[production-mode] klines: markPrice-BINANCE-SPOT, "
                     f"markets: All binance spot pairs, topic: prod-{topic_path}")
    else:
        topic_path = f'dev-{topic_path}'

    # connect to binance.com and create the stream.
    # the stream id is returned after calling `create_stream()`
    binance_websocket_api_manager = BinanceWebSocketApiManager(
        exchange="binance.com",
        throw_exception_if_unrepairable=True)
    # use this value of channels and markets to get the current mark price of all tickers
    channels = ["!miniTicker"]
    markets = ["arr"]
    stream_id = binance_websocket_api_manager.create_stream(
        channels=channels,
        markets=markets)
    # initialize publisher
    publisher = KaiPublisherClient()
    # initialize binance usdm ticker publisher
    # and run the publisher.
    klines_publisher = BinanceSpotTickerPublisher(
        websocket=binance_websocket_api_manager,
        stream_id=stream_id,
        publisher=publisher,
        topic_path=topic_path,)
    klines_publisher.run()
