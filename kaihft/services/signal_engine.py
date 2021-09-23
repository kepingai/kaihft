import logging
from kaihft.databases import KaiRealtimeDatabase
from kaihft.publishers.client import KaiPublisherClient
from kaihft.subscribers.client import KaiSubscriberClient
from kaihft.engines import SignalEngine

def main(exchange: str,
         strategy: str,
         production: bool,
         log_every: int,
         log_metrics_every: int,
         timeout: int = None,
         version: str = 'v0'):
    """ Running specified strategy to specified exchange 
        and publish signal strategy to Cloud Pub/Sub.

        Parameters
        ----------
        strategy: `str`
            A limited strategy choices to run.
        production: `bool`
            if `True` publisher will publish to production topic.
        exchange: `str`
            the exchange name, default to binance.
        timeout: `int`
            The ticker & klines subscription timeout, default to None.
        version: `str`
            The topic versions.
        log_every: `int`
                Log ticker and klines messages every.
        log_metrics_every: `int`
            Log layer 2 inference metrics every.
    """
    if production:
        ticker_topic_path = f'prod-ticker-{exchange}-{version}-sub'
        klines_topic_path = f'prod-klines-{exchange}-{version}-sub'
        dist_topic_path = f'prod-distribute-signal-{exchange}-{version}'
        archive_topic_path = f'prod-signal-{exchange}-{version}'
        database_ref = f"prod/signals"
        logging.warn(f"[production-mode] strategy: BINANCE-SPOT-{strategy}"
            f"paths=ticker: {ticker_topic_path}, klines: {klines_topic_path},"
            f"distribute: {dist_topic_path}, archive: {archive_topic_path}")
    else: 
        ticker_topic_path = f'dev-ticker-{exchange}-{version}-sub'
        klines_topic_path = f'dev-klines-{exchange}-{version}-sub'
        dist_topic_path = f'dev-distribute-signal-{exchange}-{version}'
        archive_topic_path = f'dev-signal-{exchange}-{version}'
        database_ref = f"dev/signals"
    # initialize signal engine class and run it
    params = {
        "ticker": dict(id=ticker_topic_path, timeout=timeout),
        "klines": dict(id=klines_topic_path, timeout=timeout)
    }
    # initialize publisher
    publisher = KaiPublisherClient()
    ticker_subscriber = KaiSubscriberClient()
    klines_subscriber = KaiSubscriberClient()
    # initiate access to database
    database = KaiRealtimeDatabase()
    # initialize engine and start running!
    signal_engine = SignalEngine(
        database=database,
        database_ref=database_ref,
        archive_topic_path=archive_topic_path,
        dist_topic_path=dist_topic_path,
        publisher=publisher,
        ticker_subscriber=ticker_subscriber,
        klines_subscriber=klines_subscriber,
        subscriptions_params=params,
        log_every=log_every,
        log_metrics_every=log_metrics_every,
        strategy=strategy)
    # run the engine!
    signal_engine.run()