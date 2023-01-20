from binance import Client
from datetime import datetime
import pandas as pd
import pandas_ta as ta

client = Client("", "")
symbol = "XRPUSDT"
interval = "15m"
klines = client.get_historical_klines(symbol, Client.KLINE_INTERVAL_15MINUTE, "1 day ago UTC")
__FLOATINGS = [
        "open", "high", "close", "low",
        "volume", "quote_asset_volume",
        "taker_buy_base_vol", "taker_buy_quote_vol"]
__COLUMNS = [
        "timestamp", "open", "high", "low", "close",
        "volume", "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_vol", "taker_buy_quote_vol", "ignore"]
__DROP = ["timestamp", "ignore"]
dataframe = pd.DataFrame(klines, columns=__COLUMNS)
dataframe['symbol'] = symbol.upper()
dataframe['timeframe'] = interval
dataframe[__FLOATINGS] = dataframe[__FLOATINGS].astype('float32')
dataframe.drop(columns=__DROP, inplace=True)
ohlcv = ['open', 'high', 'close', 'low', 'volume']
# convert the close time to utc string datetime
dataframe['datetime'] = dataframe.close_time.apply(
    lambda x: str(pd.to_datetime(datetime.fromtimestamp(
        (x / 1000)).strftime('%c'))))
dataframe[ohlcv] = dataframe[ohlcv].astype('float32')
dataframe = dataframe.ta.ha()
print(dataframe.iloc[-1])
ha_klines = dataframe.rename(
                    columns={'HA_open': 'open', 'HA_high': 'high',
                             'HA_low': 'low', 'HA_close': 'close'})
ha_klines.loc[ha_klines["close"] >= ha_klines["open"], "color"] = "green"
print(ha_klines)
