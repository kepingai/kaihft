import logging
from kaihft.publishers.exchanges import BinanceUSDMKlinesPublisher
from kaihft.publishers.client import KaiPublisherClient
from binance.client import Client
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager


def main(
        n_klines: int,
        production: bool,
        timeframe: int,
        topic_path: str = 'klines-binance-v0'):
    """ Retrieve real-time binance data via websocket &
        then publish binance klines to Cloud Pub/Sub.

        Parameters
        ----------
        n_klines: `int`
            The number of klines to publish.
        timeframe: `int`
            Market timeframe in minutes. For 1h timeframe, use 60, etc
        production: `bool`
            if `True` publisher will publish to production topic.
        topic_path: `str`
            The topic path to publish klines.
    """
    # we stream mark price and all market pairs
    if timeframe < 60:
        channels = [f"kline_{timeframe}m"]
    else:
        n_hour = timeframe / 60
        channels = [f"kline_{n_hour}h"]

    # initialize python binance client
    client = Client("", "")
    markets = [ticker['symbol'] for ticker in client.get_all_tickers()]

    if production:
        topic_path = f"prod-{topic_path}"
        logging.warn(f"[production-mode] klines: {n_klines}-BINANCE-SPOT, markets: {markets}, topic: prod-{topic_path}")
    else:
        topic_path = f'dev-{topic_path}'
    # binance only allows 1024 subscriptions in one stream
    # channels and markets and initiate multiplex stream
    # channels x markets = (total subscription)

    # connect to binance.com and create the stream
    # the stream id is returned after calling `create_stream()`
    binance_websocket_api_manager = BinanceWebSocketApiManager(
        exchange="binance.com-futures",
        throw_exception_if_unrepairable=True)
    stream_id = binance_websocket_api_manager.create_stream(
        channels=channels,
        markets=markets)
    # initialize publisher
    publisher = KaiPublisherClient()
    # initialize binance klines publisher
    # and run the publisher.
    klines_publisher = BinanceUSDMKlinesPublisher(
        client=client,
        websocket=binance_websocket_api_manager,
        stream_id=stream_id,
        publisher=publisher,
        topic_path=topic_path,
        n_klines=n_klines,
        timeframe=timeframe)
    klines_publisher.run()
