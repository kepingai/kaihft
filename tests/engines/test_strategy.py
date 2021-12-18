import pandas as pd
from datetime import datetime, timedelta
from binance.client import Client
from pytz import timezone
from kaihft.engines.strategy import MaxDrawdownSpread, SuperTrendSqueeze, MaxDrawdownSqueeze

def to_dataframe(base: str, klines: list, quote: str, interval: str) -> pd.DataFrame:
    """ Will convert klines from binance to dataframe.

        Parameters
        ----------
        base: `str`
            The base symbol of instrument.
        klines: `list`
            A list of list containing binance formatted klines.
        quote: `str`
            The quote of the instrument.
        interval: `str`
            The interval.
        
        Returns
        -------
        `pd.DataFrame`
            The dataframe formatted klines.
    """
    floatings = [
        "open", "high", "close", "low", 
        "volume", "quote_asset_volume",
        "taker_buy_asset_vol", "taker_buy_quote_vol"
    ]
    columns = [
        "timestamp", "open", "high", "low", 
        "close", "volume", "close_time",
        "quote_asset_volume", "number_of_trades", 
        "taker_buy_asset_vol", "taker_buy_quote_vol", 
        "ignore"
    ]
    dataframe = pd.DataFrame(klines, columns=columns)
    dataframe['datetime'] = dataframe.close_time.apply(
        lambda x: str(pd.to_datetime(datetime.fromtimestamp(
            (x / 1000), tz=timezone("UTC")).strftime('%c'))))
    dataframe['ticker'] = f"{base}{quote}".upper()
    dataframe['interval'] = interval
    dataframe[floatings] = dataframe[floatings].astype('float32')
    return dataframe

zec_long_timestamp = 1635058800
eth_short_timestamp = 1635102000

def test_strategy():
    # initialize strategy with very low spread
    # long and short to ensure that signal creation.
    super_trend_squeeze = SuperTrendSqueeze(
        endpoint='predict_15m',
        long_spread=0.15, 
        long_ttp=0.15,
        short_spread=0.15,
        short_ttp=0.15,
        pairs=dict(long=['ZECUSDT', 'ETHUSDT'], short=['ZECUSDT', 'ETHUSDT']),
        log_every=100)
    
    # initialize strategy with very low spread
    # long and short to ensure that signal creation.
    max_drawdown_squeeze = MaxDrawdownSqueeze(
        endpoint='predict_15m',
        long_spread=0.15, 
        long_ttp=0.15,
        long_max_drawdown=0.6,
        short_spread=0.15,
        short_ttp=0.15,
        short_max_drawdown=0.6,
        pairs=dict(long=['ZECUSDT', 'ETHUSDT'], short=['ZECUSDT', 'ETHUSDT']),
        log_every=100)
    
    # initialize strategy with very low spread
    # long and short to ensure that signal creation.
    max_drawdown_spread = MaxDrawdownSpread(
        endpoint='predict_15m',
        long_spread=0.15, 
        long_ttp=0.15,
        long_max_drawdown=0.6,
        short_spread=0.15,
        short_ttp=0.15,
        short_max_drawdown=0.6,
        pairs=dict(long=['ZECUSDT', 'ETHUSDT'], short=['ZECUSDT', 'ETHUSDT']),
        log_every=100,
        buffer=5)
    
    # test both strategy
    for strategy in [super_trend_squeeze, max_drawdown_squeeze, max_drawdown_spread]:
        # get ZEC short signal
        client = Client("","")
        base = 'ZEC'
        quote = 'USDT'
        end = datetime.fromtimestamp(zec_long_timestamp)
        start = (end - timedelta(minutes=(15 * 250))).timestamp()
        klines = client.get_historical_klines(
            f"{base}{quote}".upper(),
            client.KLINE_INTERVAL_15MINUTE,
            start_str=str(start),
            end_str=str(datetime.fromtimestamp(zec_long_timestamp)))
        df = to_dataframe(base, klines, quote, '15m')
        # ensure that short signal is created
        signal = strategy.scout(
            base=base, 
            quote=quote, 
            dataframe=df, 
            callback=lambda x:x)
        # at this point we can't guarantee that model will
        # predict the direction will be long but if so
        if signal: assert signal.direction == 1

        
        # get ETH short signal
        client = Client("","")
        base = 'ETH'
        quote = 'USDT'
        end = datetime.fromtimestamp(eth_short_timestamp)
        start = (end - timedelta(minutes=(15 * 250))).timestamp()
        klines = client.get_historical_klines(
            f"{base}{quote}".upper(),
            client.KLINE_INTERVAL_15MINUTE,
            start_str=str(start),
            end_str=str(datetime.fromtimestamp(eth_short_timestamp)))
        df = to_dataframe(base, klines, quote, '15m')
        # ensure that short signal is created
        signal = strategy.scout(
            base=base, 
            quote=quote, 
            dataframe=df, 
            callback=lambda x:x)
        # at this point we can't guarantee that model will
        # predict the direction will be shorting but if so 
        if signal: assert signal.direction == 0

    # test select direction
    shorting = [-0.6200340390205383,
        -0.7400986552238464,
        -0.7209125757217407,
        -0.6443493366241455]
    spread, dir = max_drawdown_squeeze.select_direction(shorting)
    assert dir == 0
    assert spread == 0.7400986552238464