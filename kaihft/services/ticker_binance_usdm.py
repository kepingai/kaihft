import logging
from kaihft.publishers.exchanges import BinanceUSDMTickerPublisher
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
        logging.warn(f"[production-mode] klines: markPrice-BINANCE-USDM, "
                     f"markets: All binance usdm pairs, topic: prod-{topic_path}")
    else:
        topic_path = f'dev-{topic_path}'

    # connect to binance.com and create the stream.
    # the stream id is returned after calling `create_stream()`
    binance_websocket_api_manager = BinanceWebSocketApiManager(
        exchange="binance.com-futures",
        throw_exception_if_unrepairable=True)
    # use this value of channels and markets to get the current mark price of all tickers
    channels = ["!markPrice"]
    markets = "arr@1s"
    stream_id = binance_websocket_api_manager.create_stream(
        channels=channels,
        markets=markets)
    # initialize publisher
    publisher = KaiPublisherClient()
    # initialize binance usdm ticker publisher
    # and run the publisher.
    ticker_publisher = BinanceUSDMTickerPublisher(
        websocket=binance_websocket_api_manager,
        stream_id=stream_id,
        publisher=publisher,
        topic_path=topic_path,)
    ticker_publisher.run()
