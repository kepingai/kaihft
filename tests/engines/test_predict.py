import pandas as pd
from datetime import datetime, timedelta
from kaihft.engines.predict import predict
from binance.client import Client
from pytz import timezone

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
    drop = ["timestamp", "ignore", "close_time"]
    dataframe = pd.DataFrame(klines, columns=columns)
    dataframe['datetime'] = dataframe.close_time.apply(
        lambda x: str(pd.to_datetime(datetime.fromtimestamp(
            (x / 1000), tz=timezone("UTC")).strftime('%c'))))
    dataframe['ticker'] = f"{base}{quote}".upper()
    dataframe['interval'] = interval
    dataframe[floatings] = dataframe[floatings].astype('float32')
    dataframe.drop(columns=drop, inplace=True)
    return dataframe

def test_predict():
    client = Client("","")
    base = 'BTC'
    quote = 'USDT'
    start = (datetime.utcnow() - timedelta(minutes=15*250)).timestamp()
    klines = client.get_historical_klines(
        f"{base}{quote}".upper(),
        client.KLINE_INTERVAL_15MINUTE,
        start_str=str(start))
    df = to_dataframe(base, klines, quote, '15m')
    # begin testing predict here
    result = predict(base=base, quote=quote, data=df.to_dict('list'))
    assert result is not None
    pred = result['predictions']
    assert float(pred['percentage_spread']) > 0
    assert int(pred['direction']) < 2 and int(pred['direction']) >= 0
    assert result['base'] == base
    assert result['quote'] == quote