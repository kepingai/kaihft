import logging
from kaihft.databases import KaiRealtimeDatabase
from kaihft.engines.strategy import StrategyType
from kaihft.publishers.client import KaiPublisherClient
from kaihft.subscribers.client import KaiSubscriberClient
from kaihft.engines import SignalEngine


def main(exchange: str,
         strategy: str,
         production: bool,
         exp0a: bool,
         exp1a: bool,
         log_every: int,
         log_metrics_every: int,
         timeout: int = None,
         version: str = 'v0',
         strategy_params: dict = {}):
    """ Running specified strategy to specified exchange 
        and publish signal strategy to Cloud Pub/Sub.

        Parameters
        ----------
        strategy: `str`
            A limited strategy choices to run.
        production: `bool`
            if `True` publisher will publish to production topic.
        exp0a: `bool`
            if `True` publisher will publish to exp0a topic.
        exp1a: `bool`
            if `True` publisher will publish to exp1a topic.
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
        strategy_params: `dict`
            contains the params to run the signal engine
    """
    # ensure that strategy is valid before starting the signal engine
    try: strategy = StrategyType(strategy)
    except Exception as e:
        logging.error(f"[strategy] strategy: {strategy} is not valid!")
        raise e
    # retrieve the appropriate paths for topics and database references
    if production: path='prod'; endpoint=f"predict_{strategy_params['timeframe']}"; mode="production"
    elif exp0a: path='exp0a'; endpoint=f"EXP0A_predict_{strategy_params['timeframe']}"; mode="experiment-0a"
    elif exp1a: path='exp1a'; endpoint=f"EXP1A_predict_{strategy_params['timeframe']}"; mode="experiment-1a"
    else: path='dev'; endpoint=f"dev_predict_{strategy_params['timeframe']}"; mode="development"
    ticker_topic_path = f'{path}-ticker-{exchange}-{version}-sub'
    klines_topic_path = f'{path}-klines-{exchange}-{version}-{strategy_params["timeframe"]}-sub'
    dist_topic_path = f'{path}-distribute-signal-{exchange}-{version}'
    archive_topic_path = f'{path}-signal-{exchange}-{version}'
    database_ref = f"{path}/signals"
    thresholds_ref = f"{path}/thresholds"
    max_drawdowns_ref = f"{path}/max_drawdowns"
    buffers_ref = f"{path}/buffers"
    pairs_ref = f"{path}/pairs"
    logging.warn(f"[{mode}-mode] strategy: BINANCE-FUTURES-{strategy}"
        f"paths: ticker: {ticker_topic_path}, klines: {klines_topic_path},"
        f"distribute: {dist_topic_path}, archive: {archive_topic_path}, "
        f"layer 2 endpoint: {endpoint}")
    # initialize signal engine class and run it
    params = {
        "ticker": dict(id=ticker_topic_path, timeout=timeout),
        "klines": dict(id=klines_topic_path, timeout=timeout)}

    if strategy == 'HEIKIN_ASHI_COINSSPOR':
        heikin_ashi_topic_path = f'{path}-klines-{exchange}-{version}-{strategy_params["ha_timeframe"]}-sub'
        heikin_ashi_subscriber = KaiSubscriberClient()
        params.update({"ha_klines": dict(id=heikin_ashi_topic_path, timeout=timeout)})
        strategy_params.update({"heikin_ashi_subscriber": heikin_ashi_subscriber,
                                "mode": path})

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
        thresholds_ref=thresholds_ref,
        max_drawdowns_ref=max_drawdowns_ref,
        buffers_ref=buffers_ref,
        pairs_ref=pairs_ref,
        archive_topic_path=archive_topic_path,
        dist_topic_path=dist_topic_path,
        publisher=publisher,
        ticker_subscriber=ticker_subscriber,
        klines_subscriber=klines_subscriber,
        subscriptions_params=params,
        log_every=log_every,
        log_metrics_every=log_metrics_every,
        strategy=strategy,
        endpoint=endpoint,
        strategy_params=strategy_params)
    # run the engine!
    signal_engine.run()
