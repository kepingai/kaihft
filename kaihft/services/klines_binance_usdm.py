import logging
from kaihft.publishers.exchanges import BinanceUSDMKlinesPublisher
from kaihft.publishers.client import KaiPublisherClient
from binance.client import Client
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager
from kaihft.alerts import RestartPodException
from kaihft.databases import KaiRealtimeDatabase


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
        topic_path = f"{topic_path}-{timeframe}m"
    else:
        n_hour = int(timeframe / 60)
        channels = [f"kline_{n_hour}h"]
        topic_path = f"{topic_path}-{n_hour}h"

    # get the list of tickers for inference
    database = KaiRealtimeDatabase()
    mode = "prod" if production else "dev"
    pairs_ref = f"{mode}/pairs"
    logging.info(f"[{mode}-mode] [{channels[0]}] pairs db reference: {pairs_ref}")

    markets_long_short = database.get(pairs_ref)
    markets = list(set().union(markets_long_short['long'], markets_long_short['short']))
    topic_path = f"{mode}-{topic_path}"
    logging.info(f"[{mode}-mode] {channels[0]}: {n_klines}-BINANCE-USDM, "
                 f"topic: {topic_path}, markets: {markets}")
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
        client=Client("", ""),
        websocket=binance_websocket_api_manager,
        markets=markets,
        stream_id=stream_id,
        publisher=publisher,
        topic_path=topic_path,
        n_klines=n_klines,
        timeframe=timeframe,
        database=database,
        pairs_ref=pairs_ref)
    klines_publisher.run()
