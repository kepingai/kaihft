import logging
from databases import KaiRealtimeDatabase
from publishers.client import KaiPublisherClient
from subscribers.client import KaiSubscriberClient
from engines import SignalEngine

def main(exchange: str,
         strategy: str,
         production: bool,
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
    """
    if production:
        ticker_topic_path = f'prod-ticker-{exchange}-{version}-sub'
        klines_topic_path = f'prod-klines-{exchange}-{version}-sub'
        dist_topic_path = f'prod-distribution-signal-{exchange}-{version}-sub'
        logging.warn(f"[production-mode] strategy: BINANCE-SPOT-{strategy}"
            f"paths=ticker: {ticker_topic_path}, klines: {klines_topic_path},"
            f"distribution: {dist_topic_path}")
    else: 
        ticker_topic_path = f'dev-ticker-{exchange}-{version}-sub'
        klines_topic_path = f'dev-klines-{exchange}-{version}-sub'
        dist_topic_path = f'dev-distribution-signal-{exchange}-{version}-sub'
    # initialize signal engine class and run it
    params = {
        "ticker": dict(id=ticker_topic_path, timeout=timeout),
        "klines": dict(id=klines_topic_path, timeout=timeout)
    }
    signal_engine = SignalEngine(
        database=KaiRealtimeDatabase(),
        database_ref='signals',
        topic_path=dist_topic_path,
        publisher=KaiPublisherClient(),
        subscriber=KaiSubscriberClient(),
        subscriptions_params=params,
        strategy=strategy)
    signal_engine.run()