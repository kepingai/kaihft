import os, asyncio, json
from datetime import datetime
import firebase_admin
from kaihft.engines import SignalEngine
from kaihft.databases import KaiRealtimeDatabase
from kaihft.engines.strategy import StrategyType
from kaihft.publishers.client import KaiPublisherClient
from kaihft.subscribers.client import KaiSubscriberClient

# ensure that you have the right credentials for this
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials.json'

# initialize publisher
publisher = KaiPublisherClient()
ticker_subscriber = KaiSubscriberClient()
klines_subscriber = KaiSubscriberClient()
# initiate access to database
database = KaiRealtimeDatabase()
# initialize engine on development topics
# this signal engine will be running STS strategy.
signal_engine = SignalEngine(
    database=database,
    database_ref='dev/signals',
    thresholds_ref='dev/thresholds',
    max_drawdowns_ref='dev/max_drawdowns',
    buffers_ref='dev/buffers',
    pairs_ref="dev/pairs",
    archive_topic_path='dev-signal-binance-v0',
    dist_topic_path='dev-distribute-signal-binance-v0',
    publisher=publisher,
    ticker_subscriber=ticker_subscriber,
    klines_subscriber=klines_subscriber,
    subscriptions_params=dict(
        ticker=dict(id='dev-ticker-binance-v0-sub', timeout=None),
        klines=dict(id='dev-klines-binance-v0-sub', timeout=None)),
    log_every=1000,
    log_metrics_every=1000,
    strategy=StrategyType.SUPER_TREND_SQUEEZE)

def test_listen_thresholds():
    def callback(event):
        thresholds = event.data
        assert 'long' in thresholds and 'short' in thresholds
        assert 'bet_threshold' in thresholds['long'].keys()
        assert 'ttp_threshold' in thresholds['long'].keys()
        assert 'bet_threshold' in thresholds['short'].keys()
        assert 'ttp_threshold' in thresholds['short'].keys()
    try:
        loop = asyncio.get_event_loop() 
        loop.run_until_complete(signal_engine.listen_thresholds(callback))
    finally: 
        if signal_engine.listener_thresholds: 
            signal_engine.listener_thresholds.close()

def test_update_thresholds():
    long_spread, long_ttp = 2, 0.5
    short_spread, short_ttp = 3, 0.75
    class Event():
        def __init__(self):
            self.event_type = 'put'
            self.path = '/'
            self.data = dict(
                long=dict(bet_threshold=long_spread, ttp_threshold=long_ttp),
                short=dict(bet_threshold=short_spread, ttp_threshold=short_ttp))
    signal_engine._update_thresholds(Event())
    assert signal_engine.strategy.long_spread == long_spread
    assert signal_engine.strategy.long_ttp == long_ttp
    assert signal_engine.strategy.short_spread == short_spread
    assert signal_engine.strategy.short_ttp == short_ttp

def test_listen_pairs():
    def callback(event):
        pairs = event.data
        assert 'long' in pairs and 'short' in pairs
    try:
        loop = asyncio.get_event_loop() 
        loop.run_until_complete(signal_engine.listen_pairs(callback))
    finally: 
        if signal_engine.listener_pairs: 
            signal_engine.listener_pairs.close()

def test_update_pairs():
    long = ["MKRUSDT", "SOLUSDT", "MANAUSDT"]
    short = ["BTCUSDT", "ETHUSDT", "LTCUSDT"]
    class Event():
        def __init__(self):
            self.event_type = 'put'
            self.path = '/'
            self.data = dict(long=long, short=short)
    signal_engine._update_pairs(Event())
    assert signal_engine.pairs["long"] == long
    assert signal_engine.pairs["short"] == short

def test_update_signals():
    now = datetime.utcnow().timestamp()
    class Message():
        def __init__(self):
            self.attributes = dict(
                timestamp=now,
                symbol='test')
            self.data = json.dumps(dict(data=dict(last_price=1))).encode('utf-8')
        def ack(self): pass
    signal_engine.update_signals(Message())
    assert signal_engine.ticker_counts > 1

def test_scout_signals():
    now = datetime.utcnow().timestamp()
    class Message():
        def __init__(self):
            self.attributes = dict(
                timestamp=now,
                symbol='test')
            self.data = json.dumps(dict(data=dict(last_price=1))).encode('utf-8')
        def ack(self): pass
    signal_engine.scout_signals(Message())
    assert signal_engine.klines_counts > 1
    
    # TODO
    # ensure that you delete the app at the end of the last of test function
    firebase_admin.delete_app(database.application)