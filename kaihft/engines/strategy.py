import time
import logging
import pandas as pd
import numpy as np
import pandas_ta as ta
from enum import Enum
from typing import Union, Tuple, Optional
from .signal import Signal
from .predict import predict, predict_cloud_run, predict_cloud_run_regression
from datetime import datetime, timedelta, timezone
from abc import abstractmethod
from google.cloud import pubsub_v1
import json
import os
from kaihft.features import (EMASlope, EMARelationship,
                             RVOL, VWAP, CCI,
                             BollingerMidBand,
                             AverageDirectionalIndex,
                             DirectionalMovement,
                             OnBalanceVolume,
                             UlcerIndex,
                             MACD,
                             RSI, ATR)
from statsmodels.tsa.stattools import adfuller
from numpy_fracdiff import fracdiff
from scipy.optimize import brentq
import pickle

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

pd.options.mode.chained_assignment = None


class StrategyType(Enum):
    SUPER_TREND_SQUEEZE = "SUPER_TREND_SQUEEZE"
    MAX_DRAWDOWN_SQUEEZE = "MAX_DRAWDOWN_SQUEEZE"
    MAX_DRAWDOWN_SPREAD = "MAX_DRAWDOWN_SPREAD"
    MAX_DRAWDOWN_SUPER_TREND_SPREAD = "MAX_DRAWDOWN_SUPER_TREND_SPREAD"
    HEIKIN_ASHI_HYBRID = "HEIKIN_ASHI_HYBRID"
    HEIKIN_ASHI_REGRESSION = "HEIKIN_ASHI_REGRESSION"
    HEIKIN_ASHI_FRAC_DIFF = "HEIKIN_ASHI_FRAC_DIFF"

    def __str__(self):
        """ Convert the enum object to string. 

            Returns
            -------
            `str`
                The exchange enum object as string.
        """
        return str(self.value)

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, str):
            return str(self.value).upper() == __o.upper()
        return super().__eq__(__o)


class Strategy():
    """ Abstract strategy class. """

    def __init__(self,
                 name: str,
                 strategy: StrategyType,
                 description: str,
                 endpoint: str,
                 long_spread: float,
                 long_ttp: float,
                 short_spread: float,
                 short_ttp: float,
                 pairs: dict,
                 log_every: int,
                 expiration_minutes: Optional[Union[int, float]] = None):
        self.strategy = strategy
        self.name = name
        self.description = description
        self.endpoint = endpoint
        self.long_spread = long_spread
        self.long_ttp = long_ttp
        self.short_spread = short_spread
        self.short_ttp = short_ttp
        self.pairs = pairs
        self.log_every = log_every
        self.expiration_minutes = expiration_minutes
        # initialize multi-core threads
#         ne.set_vml_num_threads(8)
        # initialize running metrics
        self.metrics = {}
        self.buffer = 5  # default buffer
        self.buffers = {}
        logging.info(
            f"[strategy] Strategy initialized {self.name}  ({self.strategy}), "
            f"long_spread: {self.long_spread}, long_ttp: {self.long_ttp}, "
            f"short_spread: {self.short_spread}, short_ttp: {self.short_ttp}")
        if self.expiration_minutes is not None:
            logging.info(f"[strategy] The signal is set to be expired in "
                         f"{self.expiration_minutes} minute(s).")

    @abstractmethod
    def scout(self, base: str, quote: str, dataframe: pd.DataFrame,
              callback: callable) -> Union[Signal, None]:
        raise NotImplementedError()

    def layer2(self, base: str, quote: str, data: dict):
        """ Connect to layer 2 and inference to specific model. 

            Parameters
            ----------
            base: `str`
                The base symbol of the ticker.
            quote: `str`
                The quote symbol of the ticker.
            data: `dict`
                Dictionary containing list of klines.
            
            Returns
            -------
            `(float, int, int, str, str)`
                If prediction is successful it will return the
                percentage spread, direction, the number of n-tick
                predicted forward, the base pair and the quote pair.
        """
        result = predict(endpoint=self.endpoint, base=base, quote=quote,
                         data=data)
        # if prediction fails return nones
        if not result: return None, None, None, base, quote
        # retrieve the predictions dictionary only
        pred = result['predictions']
        # conduct strategy based upon strategy type
        if 'MAX_DRAWDOWN' in str(self.strategy).upper():
            # ensure that there's percentage array in the prediction
            # note that only some of the models has this integrated
            if 'percentage_arr' not in pred: return None, None, None, base, quote
            percentage_spread, direction = self.select_direction(
                pred['percentage_arr'])
            return (
                percentage_spread,
                direction,
                int(pred['n_tick_forward']),
                str(result['base']),
                str(result['quote'])
            )
        elif self.strategy == StrategyType.SUPER_TREND_SQUEEZE:
            return (float(pred['percentage_spread']),
                    int(pred['direction']),
                    int(pred['n_tick_forward']),
                    str(result['base']),
                    str(result['quote']))
        else:
            logging.error(
                f"[layer2] strategy: {self.strategy} is not implemented yet!")
        return None, None, None, base, quote

    def select_direction(self, percentage_arr: list) -> Tuple[float, float]:
        """ Function to select the trade direction based on the
            percentage spread array, bet threshold and safety deviation.
            
            Parameters
            -----------
            percentage_arr: `list`
                list of all of the percentage spread

            Returns
            --------
            `(float, float)`
                The percentage spread predicted by the model and the 
                direction predicted by the model in integer format
                0 being short and 1 being long. Will also return `None, None`
                if no prediction spread and direction is expected.
        """
        predicted_spread = np.array(percentage_arr)
        max_pred = np.max(percentage_arr)
        min_pred = np.min(percentage_arr)
        # long-tendency if max_pred > bet_threshold
        margin_long = max_pred - self.long_spread
        # short-tendency if min_pred < -bet_threshold
        margin_short = min_pred + self.short_spread
        # Option: LONG only
        if (margin_long > 0) and (margin_short > 0):
            direction = 'long'
        # Option: SHORT only
        elif (margin_long < 0) and (margin_short < 0):
            direction = 'short'
        # Option: Available for both LONG and SHORT
        elif margin_long > 0 and margin_short < 0:
            direction = 'short' if np.abs(margin_short) > np.abs(
                margin_long) else 'long'
        else:
            direction = None
        # ensure that within predicted spreads it will not move
        # above the allowed maximum drawdowns from both directions
        if direction == 'long' and all(
                predicted_spread > (-1 * self.long_max_drawdown)):
            return float(np.abs(max_pred)), 1
        elif direction == 'short' and all(
                predicted_spread < self.short_max_drawdown):
            return float(np.abs(min_pred)), 0
        return None, None

    def save_metrics(self, start: float, symbol: str):
        """ Will save metrics of running specific symbol.

            Parameters
            ----------
            start: `str`
                The starting time of analysis in floating seconds.
            symbol: `str`
                The ticker symbol.
        """
        end = time.time()
        execution_time = round(end - start, 2)
        utctime = str(datetime.utcnow())
        if symbol not in self.metrics:
            self.metrics[symbol] = dict(
                last_execution_time=execution_time,
                last_utc_timestamp=utctime,
                count=1,
                average_execution_time=execution_time,
                total_execution_time=execution_time)
        else:
            count = self.metrics[symbol]['count'] + 1
            total_execution_time = self.metrics[symbol][
                                       'total_execution_time'] + execution_time
            self.metrics[symbol].update(
                dict(
                    last_execution_time=execution_time,
                    last_utc_timestamp=utctime,
                    count=count,
                    average_execution_time=round(total_execution_time / count,
                                                 2),
                    total_execution_time=round(total_execution_time, 2)
                )
            )
        if self.metrics[symbol]['count'] % self.log_every == 0:
            metrics = ", ".join([f"{key} : {value}"
                                 for key, value in
                                 self.metrics[symbol].items()])
            logging.info(f"[metrics] strategy-{self.name}, symbol: {symbol}, "
                         f"metrics: {metrics}, metrics-reset-every {self.log_every} times.")
            # reset the count to 1 to avoid memory leak
            self.metrics[symbol] = dict(
                last_execution_time=execution_time,
                last_utc_timestamp=str(datetime.utcnow()),
                count=1,
                average_execution_time=execution_time,
                total_execution_time=execution_time)

    def is_valid_buffer(self, symbol: str) -> bool:
        """ Will check if the current time is above the buffer seconds
            if so add timedelta to it and return that its valid. If not,
            create a new buffer time.

            Parameters
            ----------
            symbol: `str`
                The pair symbol of the model.
            
            Returns
            -------
            `bool`
                Will return `True` if symbol
                above the buffer seconds from prev
                inference time.
        """
        if symbol in self.buffers:
            # check if the time surpasses the buffer time
            if datetime.utcnow() >= self.buffers[symbol]:
                self.buffers[symbol] += timedelta(seconds=self.buffer)
                return True
        else:
            # initialize the time buffer 
            self.buffers[symbol] = datetime.utcnow() + timedelta(
                seconds=self.buffer)
            return True
        return False


class HeikinAshiBase(Strategy):
    """ Heikin Ashi Coinsspor strategy implementation, will scout for
        potential actionable intelligence based on a specific market behavior.

        Notes
        -----
        Will act as the base class for all Heikin-Ashi strategy.
    """

    def __init__(self,
                 name: str,
                 strategy: StrategyType,
                 mode: str,
                 kaiforecast_version: Optional[str],
                 long_spread: float,
                 short_spread: float,
                 long_ttp: float,
                 short_ttp: float,
                 pairs: dict,
                 ha_timeframe: str,
                 model_timeframe: str,
                 ha_ema_len: int,
                 log_every: int,
                 description: str = "Heikin-Ashi Buy and Sell Strategy by Coinsspor",
                 endpoint: str = "",
                 expiration_minutes: Optional[Union[int, float]] = None):

        super(HeikinAshiBase, self).__init__(
            name=name,
            strategy=strategy,
            description=description,
            endpoint=endpoint,
            long_spread=long_spread,
            long_ttp=long_ttp,
            short_spread=short_spread,
            short_ttp=short_ttp,
            pairs=pairs,
            log_every=log_every,
            expiration_minutes=expiration_minutes
        )

        self.ha_timeframe = ha_timeframe
        self.model_timeframe = model_timeframe
        self.ha_ema_len = ha_ema_len
        self.mode = mode
        self.kaiforecast_version = kaiforecast_version
        logging.info(f"[strategy] [{str(strategy)}] run the {model_timeframe} "
                     f"strategy with kaiforecast {str(kaiforecast_version)}. "
                     f"[heikin-ashi] config --- timeframe: {ha_timeframe}, "
                     f"EMA length: {ha_ema_len}.")
        # initialize the heikin-ashi trend dict
        self.ha_klines_counts = 1
        self.higher_ha_klines_counts = 1
        self.ha_trend, self.prev_ha_trend, self.higher_ha_trend, self.ha_cooldowns = {}, {}, {}, {}
        self.ha_candle, self.higher_ha_candle = {}, {}
        self.ha_type, self.prev_ha_type = {}, {}
        self.ha_colors = {}
        self.higher_ha_type, self.prev_higher_ha_type = {}, {}
        self.ha_cooldown = 100  # second(s)
        for _, v in pairs.items():
            for p in v:
                if p not in self.ha_trend: self.ha_trend.update({p: 0})
                if p not in self.ha_candle: self.ha_candle.update({p: 0})
                if p not in self.higher_ha_trend: self.higher_ha_trend.update(
                    {p: 0})
                if p not in self.prev_ha_trend: self.prev_ha_trend.update(
                    {p: 0})
                if p not in self.ha_colors: self.ha_colors.update(
                    {p: [0, 0, 0]})
        self.natr = 0
        logging.info(f"[strategy] [{str(strategy)}] initialized the heikin-"
                     f"ashi trend and candle, ha_trend: {self.ha_trend}, "
                     f"ha_candle: {self.ha_candle}")

    def scout(self,
              base: str,
              quote: str,
              dataframe: pd.DataFrame,
              callback: callable) -> Union[Signal, None]:
        """ Scouts potential signals based on Heikin Ashi candles and TFT 1m predictions

            Parameters
            ----------
            base: `str`
                base to predict
            quote: `str`
                the pair's quote
            dataframe: `pd.DataFrame`
                the dataframe of the timeseries data to predict
            callback: `callable`
                a function which is called if the signal is closed

            Returns
            -------
            `Union[Signal, None]`
                a signal object, is returned if the conditions to create a
                signal are met. Otherwie returns None

        """
        start = time.time()
        signal = False
        clean_df = dataframe.copy()
        # format the clean df before inference
        clean_df.rename(columns=dict(timeframe="interval",
                                     symbol="ticker",
                                     taker_buy_base_vol="taker_buy_asset_vol"),
                        inplace=True)
        clean_df = clean_df[
            ['open', 'high', 'low', 'close', 'volume', 'close_time',
             'quote_asset_volume', 'number_of_trades', 'taker_buy_asset_vol',
             'taker_buy_quote_vol', 'datetime', 'ticker', 'interval']]

        last_price = clean_df.iloc[-1].close
        open_price = clean_df.iloc[-1].open
        pair = f"{base}{quote}".upper()

        if self.ha_trend[pair] == 1 and self.ha_candle[pair] == 1 \
                and pair in self.pairs['long'] and last_price > open_price:
            # inference to layer 2
            _spread, _direction, _n_tick, base, quote = self.layer2(
                base=base,
                quote=quote,
                data=clean_df.to_dict('list'),
                mode=self.mode,
                ha_trend=self.ha_trend[pair]
            )
            if _spread is None or _direction is None: return None
            # ensure that spread is above threshold and direction matches.
            if _spread >= self.long_spread and _direction == 1:
                ttp = self.long_ttp
                signal = True
            # record the ending time of analysis
            self.save_metrics(start, f"{base}{quote}")

        # else if direction is short and squeeze is off and red candle
        elif self.ha_trend[pair] == -1 and self.ha_candle[pair] == -1 \
                and pair in self.pairs['short'] and last_price < open_price:
            # inference to layer 2
            _spread, _direction, _n_tick, base, quote = self.layer2(
                base=base,
                quote=quote,
                data=clean_df.to_dict('list'),
                mode=self.mode,
                ha_trend=self.ha_trend[pair]
            )
            if _spread is None or _direction is None: return None
            # ensure that spread is above threshold and direction matches.
            if _spread >= self.short_spread and _direction == 0:
                ttp = self.short_ttp
                signal = True
            # record the ending time of analysis
            self.save_metrics(start, f"{base}{quote}")

        return Signal(
            base=base,
            quote=quote,
            take_profit=ttp,
            spread=_spread,
            buffer=_n_tick,
            purchase_price=float(last_price),
            last_price=float(last_price),
            direction=_direction,
            callback=callback,
            n_tick_forward=_n_tick,
            expiration_minutes=self.expiration_minutes
        ) if signal else None

    def layer2(self,
               base: str,
               quote: str,
               data: dict,
               mode: str,
               ha_trend: int):
        """ Connect to layer 2 and inference to specific model. """
        raise NotImplementedError()

    def calculate_heikin_ashi_trend(self,
                                    message: pubsub_v1.subscriber.message.Message):
        """ This function calculates the buy and sell signals based on Coinsspor Heikin Ashi Tradingview signals.

            Parameters
            ----------
            message: `pubsub_v1.subscriber.message.Message`
                message containing the klines which will be analysed using Heikin-Ashi

        """
        if message.headers and 'timestamp' in message.headers:
            # get the attributes of the message
            symbol = message.headers.get("symbol")
            timestamp = int(float(message.headers.get("timestamp")))
            klines_time = datetime.utcfromtimestamp(timestamp)
            seconds_passed = (datetime.utcnow() - klines_time).total_seconds()
            # only accept messages within 150 seconds latency
            if 10 >= seconds_passed >= 0 and self.is_valid_cooldown(symbol):
                # only run strategy if symbol is currently
                # not an ongoing signal and also not currently
                # awaiting for a result from running strategy

                # run a separate thread to run startegy
                klines = pd.DataFrame(
                    json.loads(message.body.decode('utf-8'))['data'])
                ha_dataframe = self.format_dataframe(klines)

                # ohlc4 is a tradingview variable. It is the average of the OHLC value
                ohlc4 = (ha_dataframe['open'] + ha_dataframe['high'] +
                         ha_dataframe['low'] + ha_dataframe[
                             'close']) / 4

                # calculate the heiken ashi candles and rename the columns so that we can calculate the EMA
                if symbol == "BTCUSDT":
                    natr = ha_dataframe.ta.natr(
                        length=14, scalar=100, drift=2, offset=1).iloc[-1]
                    self.natr = natr
                ha_klines = ha_dataframe.ta.ha()
                ha_klines = ha_klines.rename(
                    columns={'HA_open': 'open', 'HA_high': 'high',
                             'HA_low': 'low', 'HA_close': 'close'})

                # get the latest heikin-ashi candle of the specific symbol
                ha_candles = ha_klines["close"] - ha_klines[
                    "open"]
                self.ha_candle[symbol] = 1 if ha_candles.iloc[-1] > 0 else -1

                # start calculation. See tradingview for reference
                hac = pd.DataFrame((ohlc4 + ha_klines['open'].fillna(0)
                                    + pd.concat([ha_dataframe['high'],
                                                 ha_klines[
                                                     'open'].fillna(0)]).max(
                            level=0)
                                    + pd.concat([ha_dataframe['low'],
                                                 ha_klines[
                                                     'open'].fillna(0)]).min(
                            level=0)) / 4,
                                   columns=['close'])
                ema1 = pd.DataFrame(hac.ta.ema(length=self.ha_ema_len)).rename(
                    columns={f'EMA_{self.ha_ema_len}': 'close'})
                ema2 = pd.DataFrame(ema1.ta.ema(length=self.ha_ema_len)).rename(
                    columns={f'EMA_{self.ha_ema_len}': 'close'})
                ema3 = pd.DataFrame(ema2.ta.ema(length=self.ha_ema_len)).rename(
                    columns={f'EMA_{self.ha_ema_len}': 'close'})
                tma1 = 3 * ema1 - 3 * ema2 + ema3
                ema4 = pd.DataFrame(tma1.ta.ema(length=self.ha_ema_len)).rename(
                    columns={f'EMA_{self.ha_ema_len}': 'close'})
                ema5 = pd.DataFrame(ema4.ta.ema(length=self.ha_ema_len)).rename(
                    columns={f'EMA_{self.ha_ema_len}': 'close'})
                ema6 = pd.DataFrame(ema5.ta.ema(length=self.ha_ema_len)).rename(
                    columns={f'EMA_{self.ha_ema_len}': 'close'})
                tma2 = 3 * ema4 - 3 * ema5 + ema6
                ipek = tma1 - tma2
                yasin = tma1 + ipek
                hlc3 = pd.DataFrame((ha_dataframe['high'] + ha_dataframe[
                    'low'] + ha_dataframe['close']) / 3,
                                    columns=['close'])
                ema7 = pd.DataFrame(hlc3.ta.ema(length=self.ha_ema_len)).rename(
                    columns={f'EMA_{self.ha_ema_len}': 'close'})
                ema8 = pd.DataFrame(ema7.ta.ema(length=self.ha_ema_len)).rename(
                    columns={f'EMA_{self.ha_ema_len}': 'close'})
                ema9 = pd.DataFrame(ema8.ta.ema(length=self.ha_ema_len)).rename(
                    columns={f'EMA_{self.ha_ema_len}': 'close'})
                tma3 = 3 * ema7 - 3 * ema8 + ema9
                ema10 = pd.DataFrame(
                    tma3.ta.ema(length=self.ha_ema_len)).rename(
                    columns={f'EMA_{self.ha_ema_len}': 'close'})
                ema11 = pd.DataFrame(
                    ema10.ta.ema(length=self.ha_ema_len)).rename(
                    columns={f'EMA_{self.ha_ema_len}': 'close'})
                ema12 = pd.DataFrame(
                    ema11.ta.ema(length=self.ha_ema_len)).rename(
                    columns={f'EMA_{self.ha_ema_len}': 'close'})
                tma4 = 3 * ema10 - 3 * ema11 + ema12

                # These are some turkish words. mavi means blue and kirmizi means red.
                # if mavi > kirmizi, we are in an uptrend and vice versa.
                # No idea what ipek and yasin mean. I just followed the vocab he used in tradingview.
                ipek1 = tma3 - tma4
                yasin1 = tma3 + ipek1
                mavi = yasin1.fillna(0)
                kirmizi = yasin.fillna(0)

                # define trend for long and short positions
                # if True, it means the model can go long. If False, the model can go short
                uptrend = np.array(np.array(mavi) > np.array(kirmizi))
                downtrend = np.array(np.array(kirmizi) > np.array(mavi))

                if np.all(uptrend[-2:-1]):
                    self.ha_trend.update({symbol: 1})
                elif np.all(downtrend[-2:-1]):
                    self.ha_trend.update({symbol: -1})
                else:
                    self.ha_trend.update({symbol: 0})

                if uptrend[-3]:
                    self.prev_ha_trend.update({symbol: 1})
                elif downtrend[-3]:
                    self.prev_ha_trend.update({symbol: -1})
                else:
                    self.prev_ha_trend.update({symbol: 0})

                # classify heikin ashi candle type
                ha_klines.loc[
                    ha_klines["close"] >= ha_klines["open"], "color"] = "green"
                ha_klines.loc[
                    ha_klines["close"] < ha_klines["open"], "color"] = "red"
                ha_klines.loc[(ha_klines["color"] == "green")
                              & (ha_klines["high"] - ha_klines["close"]
                                 > 1.5 * (ha_klines["open"] - ha_klines[
                            "low"])),
                              "candle_type"] = "bullish"
                ha_klines.loc[(ha_klines["color"] == "green")
                              & (ha_klines["high"] - ha_klines["close"]
                                 < 1.5 * (ha_klines["open"] - ha_klines[
                            "low"])),
                              "candle_type"] = "undecided"
                ha_klines.loc[(ha_klines["color"] == "red")
                              & (ha_klines["close"] - ha_klines["low"]
                                 > 1.5 * (ha_klines["high"] - ha_klines[
                            "open"])),
                              "candle_type"] = "bearish"
                ha_klines.loc[(ha_klines["color"] == "red")
                              & (ha_klines["close"] - ha_klines["low"]
                                 < 1.5 * (ha_klines["high"] - ha_klines[
                            "open"])),
                              "candle_type"] = "undecided"

                self.ha_colors.update(
                    {symbol: [ha_klines["color"].iloc[-3],
                              ha_klines["color"].iloc[-2],
                              ha_klines["color"].iloc[-1]]})
                self.ha_type.update({symbol: ha_klines["candle_type"].iloc[-1]})
                self.prev_ha_type.update(
                    {symbol: ha_klines["candle_type"].iloc[-2]})

            # restarting the pod if latency above 10 minute(s),
            # as the safety net to handle message flooding.
            if seconds_passed > 600:
                logging.critical(f"[restart] restarting the signal engine due "
                                 f"to excessive heikin-ashi klines message "
                                 f"latency: {seconds_passed} seconds.")
                if os.path.exists('tmp/healthy'): os.remove('tmp/healthy')
            # logging the ha klines counter
            if self.ha_klines_counts % (self.log_every * 10) == 0:
                logging.info(f"[ha_klines] cloud pub/sub messages running, "
                             f"latency: {seconds_passed} sec, last-symbol: "
                             f"{symbol}, "
                             f"ha_trend: {self.ha_trend}")
                # reset the signal counts to 1
                self.ha_klines_counts = 1
            # add the counter for each message received
            self.ha_klines_counts += 1

    def format_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """ Will format the dataframe to ensure appropriate format for
            inferencing to layer 2 without errors.

            Parameters
            ----------
            dataframe: `pd.DataFrame`
                The dataframe to format.

            Returns
            -------
            `pd.DataFrame`
                Newly formatted dataframe.
        """
        ohlcv = ['open', 'high', 'close', 'low', 'volume']
        # convert the close time to utc string datetime
        dataframe['datetime'] = dataframe.close_time.apply(
            lambda x: str(pd.to_datetime(datetime.fromtimestamp(
                (x / 1000)).strftime('%c'))))
        dataframe[ohlcv] = dataframe[ohlcv].astype('float32')
        return dataframe

    def is_valid_cooldown(self, symbol: str) -> bool:
        """ Will check if cooldown time have passed if a heikin-ashi trend
            have been calculated for that pair.

            Parameters
            ----------
            symbol: `str`
                The symbol of coin.

            Returns
            -------
            `bool`
                Return `False` if counter have not surpass the cooldown time.
        """
        if symbol in self.ha_cooldowns:
            # check if the time surpasses the cooldown time
            if datetime.utcnow() >= self.ha_cooldowns[symbol]:
                self.ha_cooldowns[symbol] += timedelta(seconds=self.ha_cooldown)
                return True
        else:
            # initialize the time buffer
            self.ha_cooldowns[symbol] = datetime.utcnow() + timedelta(
                seconds=self.ha_cooldown)
            return True
        return False


class HeikinAshiFractionalDifference(HeikinAshiBase):
    """ A strategy which uses fractional differencing for feature engineering
    """

    def __init__(self,
                 long_ttp: "float or list",
                 short_ttp: "float or list",
                 long_spread: float,
                 short_spread: float,
                 pairs: dict,
                 log_every: int,
                 ha_timeframe: str,
                 model_timeframe: str,
                 ha_ema_len: int,
                 features: list,
                 mode: str = "exp0a",
                 kaiforecast_version: str = "",
                 description: str = "Heikin-Ashi Buy and Sell Strategy by Coinsspor",
                 endpoint: str = "",
                 expiration_minutes: Optional[Union[int, float]] = 240):
        super(HeikinAshiFractionalDifference, self).__init__(
            name=str(StrategyType.HEIKIN_ASHI_FRAC_DIFF),
            strategy=StrategyType.HEIKIN_ASHI_FRAC_DIFF,
            description="Heikin Ashi Random Forest Long vs. Short strategy.",
            endpoint="",
            long_spread=long_spread,
            long_ttp=long_ttp,
            short_spread=short_spread,
            short_ttp=short_ttp,
            pairs=pairs,
            log_every=log_every,
            expiration_minutes=expiration_minutes,
            mode=mode,
            kaiforecast_version=kaiforecast_version,
            ha_timeframe=ha_timeframe,
            model_timeframe=model_timeframe,
            ha_ema_len=ha_ema_len
        )

        self.interval_s = {"15m": 900,
                           "1h": 3600}
        self.last_signal = {}
        self.models = self.load_models()
        logging.info(f"List of models {self.models.keys()}")
        self.thresholds = self.load_thresholds()
        self.features = features

    def load_thresholds(self) -> dict:
        """ Load the thresholds for this strategy

            Returns
            -------
            `dict`
                {"long": {"BTC": btc_threshold, "ETH": eth_threshold},
                 "short": {"BTC": btc_threshold, "ETH": eth_threshold}}

        """
        thresholds = {"long": {}, "short": {}}

        try:
            with open("models/prediction_thresholds.json", 'r') as fp:
                threshold_data = json.load(fp)
                for direction in ["long", "short"]:
                    for pair in self.pairs[direction]:
                        base = pair.replace("USDT", "")
                        base_threshold = threshold_data.get(base, {}) \
                            .get(direction.upper(), 0.5)
                        thresholds[direction].update({pair: base_threshold})

        except Exception:
            logging.info("Threshold file not found. Proceeds with 0.5 as "
                         "the prediction prbability threshold")
            for direction in ["long", "short"]:
                for pair in self.pairs[direction]:
                    thresholds[direction].update({pair: 0.5})

        return thresholds

    def load_models(self) -> dict:
        """ Load the models for this strategy

            Returns
            -------
            `dict`
                {"long": {"BTC": btc_model, "ETH": eth_model},
                 "short": {"BTC": btc_model, "ETH": eth_model}}

        """
        models = {"long": {}, "short": {}}
        for direction in ["long", "short"]:
            for pair in self.pairs[direction]:
                self.last_signal.update({pair: 0})
                filename = f"models/RFClassifier_{pair.replace('USDT', '')}_{direction.upper()}.sav"
                loaded_model = pickle.load(open(filename, 'rb'))
                models[direction].update({pair: loaded_model})
        return models

    def get_features(self, feature_list: list):
        """ Get the kaiforecast feature objects based on given features 
            in environment variable file. 
            
            Parameters
            ----------
            feature_list: `list`
                list of features
        """
        feature_templates = {"ema_slope": EMASlope(),
                             "ema_relationship": EMARelationship(),
                             "rvol": RVOL(),
                             "vwap": VWAP(),
                             "cci": CCI(),
                             "adx": AverageDirectionalIndex(),
                             "dm": DirectionalMovement(),
                             "bollingermidband": BollingerMidBand(),
                             "ui": UlcerIndex(),
                             "obv": OnBalanceVolume(),
                             "rsi": RSI(),
                             "macd": MACD(),
                             "atr": ATR()}

        return [feature_templates[feature] for feature in feature_list
                if feature in feature_templates.keys()]

    def scout(self,
              base: str,
              quote: str,
              dataframe: pd.DataFrame,
              callback: callable):
        """ Will scout for potential market trigger from heikin ashi fractional
            difference. Each time the heikin-ashi coinsspor parameter changes its
            direction, the function calls the Random Forest model to determine
            if the signal should be taken or not.

            Parameters
            ----------
            base: `str`
                The base pair.
            quote: `str`
                The quote pair.
            dataframe: `pd.DataFrame`
                The klines to run technical analysis on.
            callback: `callable`
                The closing signal callback.

            Returns
            -------
            `Union[Signal, None]`
                Will return a Signal object or None.
        """
        start = time.time()
        signal = False
        _spread = 2
        _n_tick = 8
        clean_df = dataframe.copy()
        # format the clean df before inference
        clean_df.rename(columns=dict(timeframe="interval",
                                     symbol="ticker",
                                     taker_buy_base_vol="taker_buy_asset_vol"),
                        inplace=True)
        clean_df = clean_df[
            ['open', 'high', 'low', 'close', 'volume', 'close_time',
             'quote_asset_volume', 'number_of_trades', 'taker_buy_asset_vol',
             'taker_buy_quote_vol', 'datetime', 'ticker', 'interval']]
        pair = f"{base}{quote}".upper()
        last_price = clean_df.iloc[-1].close
        direction = "long" if self.ha_trend.get(pair, 0) == 1 else "short"

        if pair not in self.models[direction]:
            return
        else:
            interval = clean_df.iloc[-1]["interval"]
            candle_age = (self.interval_s[interval]
                          - ((dataframe.iloc[-1]["close_time"]/1e3)
                             - datetime.now(tz=timezone.utc).timestamp()))
            if (self.ha_colors[pair][1] == "green"
                    and self.ha_colors[pair][0] == "red"
                    and self.ha_colors[pair][2] == "green"
                    # self.ha_trend[pair] == 1
                    # and (self.prev_ha_trend[pair] == -1)
                    and candle_age < self.interval_s[interval] / 10
                    and datetime.now(tz=timezone.utc).timestamp()-self.last_signal[pair] > 1800):
                prediction = self.layer2(
                    base=base, quote=quote,
                    data=clean_df[:-1],
                    mode=self.mode,
                    # ha_trend=self.ha_trend[pair],
                    ha_trend=1
                )

                self.save_metrics(start, f"{base}{quote}")
                signal = True if prediction is True else False

            elif (self.ha_colors[pair][1] == "red"
                  and self.ha_colors[pair][0] == "green"
                  and self.ha_colors[pair][2] == "red"
                  #   self.ha_trend[pair] == -1
                  # and (self.prev_ha_trend[pair] == 1)
                  and candle_age < self.interval_s[interval] / 10
                  and datetime.now(tz=timezone.utc).timestamp()-self.last_signal[pair] > 1800):
                prediction = self.layer2(
                    base=base, quote=quote,
                    data=clean_df[:-1],
                    mode=self.mode,
                    # ha_trend=self.ha_trend[pair]
                    ha_trend=-1
                )

                self.save_metrics(start, f"{base}{quote}")
                signal = True if prediction is True else False

        # if self.natr < 0.25:
        #     direction = 0 if self.ha_trend[pair] == 1 else 1
        # else:
        #     direction = 1 if self.ha_trend[pair] == 1 else 0
        return Signal(
            base=base,
            quote=quote,
            # take_profit=(self.long_ttp
            #              if self.ha_trend[pair] == 1
            #              else self.short_ttp),
            take_profit=(self.long_ttp
                         if self.ha_colors[pair][2] == "green"
                         else self.short_ttp),
            spread=_spread,
            buffer=_n_tick,
            purchase_price=float(last_price),
            last_price=float(last_price),
            # direction=direction,
            direction=1 if self.ha_colors[pair][2] == "green" else 0,
            callback=callback,
            n_tick_forward=_n_tick,
            expiration_minutes=self.expiration_minutes,
            ha_reverse=False,
            # stop_loss=1.0
        ) if signal else None

    def layer2(self,
               base: str,
               quote: str,
               data: pd.DataFrame,
               mode: str,
               ha_trend: int) -> bool:
        """ This function takes the kline input and predict if a should be
            should opened

            Parameters
            ----------
            base: `str`
            quote: `str`
            data: `list`
                klines data
            mode: `str`
                dev or prod
            ha_trend: `int`
                current 15m heikin-ashi direction

            Returns
            -------
            `Tuple`
                the prediction result. True if a position should be opened

        """
        # combine the features
        features = self.get_features(self.features)
        features = [
            pd.DataFrame({feature.feature_name: feature.preprocess(data)})
            for feature in features]
        aggregated_features = pd.concat(features, axis=1)
        aggregated_features.index = data.index
        feature_cols = list(aggregated_features.columns)
        aggregated_data = pd.concat([data, aggregated_features], axis=1)
        aggregated_data.dropna(axis=0, inplace=True)
        volume_col = ["volume"]
        aggregated_data = aggregated_data[volume_col + feature_cols]

        for column in list(aggregated_data.columns):
            series = aggregated_data[column]
            if not self.is_stationary(series):
                d = brentq(f=self.find_d_rolling, a=0, b=1, args=(series,))
                frac_series = fracdiff(series.values, order=d, tau=1e-2)
                aggregated_data[column] = frac_series
        model_input = aggregated_data[-225:].values.reshape(1, -1)
        direction = "long" if ha_trend == 1 else "short"
        y_prob = self.models[direction][f"{base}{quote}"]\
            .predict_proba(model_input)[0][1]
        logging.info(
            f"Predicting {direction} position {base}. Result: {y_prob}")
        return True if y_prob >= self.thresholds[direction][f"{base}{quote}"] else False

    def find_d_rolling(self, d, series: pd.Series) -> float:
        """ Parameters
            ----------
            d: `float`
                fractional difference order
            series: `pd.Series`
                series to fractionally differentiate

            Returns
            -------
            `float`
                the value to reduce to zero, which is the difference between
                the p value and significance

        """
        significance = 5e-2
        frac_series = fracdiff(series.values, order=d, tau=1e-2)
        skip = len(frac_series[np.isnan(frac_series)])
        adf = adfuller(frac_series[skip:], maxlag=1, autolag=None)
        p_val = adf[1]

        return p_val - significance

    def is_stationary(self, series: pd.Series) -> bool:
        """ Check if a pandas series is stationary

        Parameters
        ----------
        series: `pd.Series`
            the series to check

        Returns
        -------
        `bool`
            True if the series is stationary
        """
        significance = 5e-2
        adf = adfuller(series, maxlag=1, autolag=None)
        p_val = adf[1]
        return p_val <= significance


class HeikinAshiRegression(HeikinAshiBase):
    """ Heikin Ashi Regression strategy implementation
        will scout for potential actionable
        intelligence based on a specific market behavior.
    """

    def __init__(self,
                 mode: str,
                 kaiforecast_version: Optional[str],
                 long_spread: float,
                 short_spread: float,
                 long_ttp: float,
                 short_ttp: float,
                 pairs: dict,
                 ha_timeframe: str,
                 model_timeframe: str,
                 ha_ema_len: int,
                 log_every: int,
                 endpoint: str = "",
                 expiration_minutes: Optional[Union[int, float]] = None):
        description = "Heikin-Ashi Buy and Sell Strategy by Coinsspor for " \
                      "Regression Model"
        super(HeikinAshiRegression, self).__init__(
            name=str(StrategyType.HEIKIN_ASHI_REGRESSION),
            strategy=StrategyType.HEIKIN_ASHI_REGRESSION,
            mode=mode,
            kaiforecast_version=kaiforecast_version,
            long_spread=long_spread,
            short_spread=short_spread,
            long_ttp=long_ttp,
            short_ttp=short_ttp,
            pairs=pairs,
            ha_timeframe=ha_timeframe,
            model_timeframe=model_timeframe,
            ha_ema_len=ha_ema_len,
            log_every=log_every,
            description=description,
            endpoint=endpoint,
            expiration_minutes=expiration_minutes
        )
        self.interval_s = {"15m": 900,
                           "1h": 3600}

    def layer2(self,
               base: str,
               quote: str,
               data: dict,
               mode: str,
               ha_trend: int
               ):
        """ Connect to layer 2 and inference to specific model.

            Parameters
            ----------
            base: `str`
                The base symbol of the ticker.
            quote: `str`
                The quote symbol of the ticker.
            data: `dict`
                Dictionary containing list of klines.
            mode: `str`
            ha_trend: `int`

            Returns
            -------
            `(float, int, int, str, str)`
                If prediction is successful it will return the
                percentage spread, direction, the number of n-tick
                predicted forward, the base pair and the quote pair.
        """
        # use the cloud function predictor (old/prod model)
        if self.kaiforecast_version is None:
            reg_result = predict(
                endpoint=self.endpoint, base=base, quote=quote, data=data)
            if not reg_result: return None, None, None, base, quote
            pred = reg_result['predictions']

            # use the SUPER_TREND_SQUEEZE method
            # if 'percentage_arr' not in pred: return None, None, None, base, quote
            # _spread, _direction = self.select_direction(pred['percentage_arr'])
            _spread = float(pred['percentage_spread'])
            _direction = int(pred['direction'])
            _n_ticks = int(pred['n_tick_forward'])
        # use the cloud run predictor (latest/dev model)
        else:
            reg_result = predict_cloud_run_regression(
                mode=mode,
                kaiforecast_version=self.kaiforecast_version,
                base=base,
                quote=quote,
                data=data,
                timeframe=self.model_timeframe,
                ha_trend=ha_trend
            )
            # if prediction fails return nones
            if not reg_result: return None, None, None, base, quote
            _spread = reg_result['predictions']['percentage_spread']
            _direction = reg_result['predictions']['direction']
            _n_ticks = reg_result['predictions']['n_tick_forward']
        return _spread, _direction, _n_ticks, base, quote

    def scout(self,
              base: str,
              quote: str,
              dataframe: pd.DataFrame,
              callback: callable) -> Union[Signal, None]:
        """ Scouts potential signals based on Heikin Ashi candles and TFT 1m predictions

            Parameters
            ----------
            base: `str`
                base to predict
            quote: `str`
                the pair's quote
            dataframe: `pd.DataFrame`
                the dataframe of the timeseries data to predict
            callback: `callable`
                a function which is called if the signal is closed

            Returns
            -------
            `Union[Signal, None]`
                a signal object, is returned if the conditions to create a
                signal are met. Otherwie returns None

        """
        start = time.time()
        signal = False
        clean_df = dataframe.copy()
        # format the clean df before inference
        clean_df.rename(columns=dict(timeframe="interval",
                                     symbol="ticker",
                                     taker_buy_base_vol="taker_buy_asset_vol"),
                        inplace=True)
        clean_df = clean_df[
            ['open', 'high', 'low', 'close', 'volume', 'close_time',
             'quote_asset_volume', 'number_of_trades', 'taker_buy_asset_vol',
             'taker_buy_quote_vol', 'datetime', 'ticker', 'interval']]

        last_price = clean_df.iloc[-1].close
        pair = f"{base}{quote}".upper()

        interval = clean_df.iloc[-1]["interval"]
        candle_age = (self.interval_s[interval]
                      - ((dataframe.iloc[-1]["close_time"] / 1e3)
                         - datetime.now(tz=timezone.utc).timestamp()))

        if (self.ha_trend[pair] == 1
                and pair in self.pairs['long']
                and self.prev_ha_trend[pair] == -1
                and self.higher_ha_trend[pair] == 1
                and candle_age < self.interval_s[interval] / 15):
            # inference to layer 2
            _spread, _direction, _n_tick, base, quote = self.layer2(
                base=base,
                quote=quote,
                data=clean_df.iloc[:-1].to_dict('list'),
                mode=self.mode,
                ha_trend=self.ha_trend[pair]
            )
            if _spread is None or _direction is None: return None
            # ensure that spread is above threshold and direction matches.
            if _spread >= self.long_spread and _direction == 1:
                ttp = self.long_ttp
                signal = True
            # record the ending time of analysis
            self.save_metrics(start, f"{base}{quote}")

        # else if direction is short and squeeze is off and red candle
        elif (self.ha_trend[pair] == -1
              and pair in self.pairs['short']
              and self.prev_ha_trend[pair] == 1
              and self.higher_ha_trend[pair] == -1
              and candle_age < self.interval_s[interval] / 15):
            # inference to layer 2
            _spread, _direction, _n_tick, base, quote = self.layer2(
                base=base,
                quote=quote,
                data=clean_df.iloc[:-1].to_dict('list'),
                mode=self.mode,
                ha_trend=self.ha_trend[pair]
            )
            if _spread is None or _direction is None: return None
            # ensure that spread is above threshold and direction matches.
            if _spread >= self.short_spread and _direction == 0:
                ttp = self.short_ttp
                signal = True
            # record the ending time of analysis
            self.save_metrics(start, f"{base}{quote}")

        return Signal(
            base=base,
            quote=quote,
            take_profit=ttp,
            spread=_spread,
            buffer=_n_tick,
            purchase_price=float(last_price),
            last_price=float(last_price),
            direction=_direction,
            callback=callback,
            n_tick_forward=_n_tick,
            expiration_minutes=self.expiration_minutes
        ) if signal else None


class HeikinAshiHybrid(HeikinAshiBase):
    """ Heikin Ashi Hybrid strategy implementation
        will scout for potential actionable
        intelligence based on a specific market behavior.
    """

    def __init__(self,
                 mode: str,
                 kaiforecast_version: Optional[str],
                 classification_threshold: float,
                 long_spread: float,
                 short_spread: float,
                 long_ttp: float,
                 short_ttp: float,
                 pairs: dict,
                 ha_timeframe: str,
                 model_timeframe: str,
                 ha_ema_len: int,
                 log_every: int,
                 expiration_minutes: Optional[Union[int, float]] = None):
        description = "Heikin-Ashi Buy and Sell Strategy by Coinsspor for " \
                      "Hybrid Model (Regression and Classification)"
        super(HeikinAshiHybrid, self).__init__(
            name=str(StrategyType.HEIKIN_ASHI_HYBRID),
            strategy=StrategyType.HEIKIN_ASHI_HYBRID,
            mode=mode,
            kaiforecast_version=kaiforecast_version,
            long_spread=long_spread,
            long_ttp=long_ttp,
            short_spread=short_spread,
            short_ttp=short_ttp,
            pairs=pairs,
            ha_timeframe=ha_timeframe,
            model_timeframe=model_timeframe,
            ha_ema_len=ha_ema_len,
            log_every=log_every,
            description=description,
            expiration_minutes=expiration_minutes
        )
        self.classification_threshold = classification_threshold
        if self.kaiforecast_version is None:
            raise ValueError(f"Please input the kaiforecast version!")

    def layer2(self,
               base: str,
               quote: str,
               data: dict,
               mode: str,
               ha_trend: int
               ):
        """ Connect to layer 2 and inference to specific model.

            Parameters
            ----------
            base: `str`
                The base symbol of the ticker.
            quote: `str`
                The quote symbol of the ticker.
            data: `dict`
                Dictionary containing list of klines.
            mode: `str`
            ha_trend: `int`

            Returns
            -------
            `(float, int, int, str, str)`
                If prediction is successful it will return the
                percentage spread, direction, the number of n-tick
                predicted forward, the base pair and the quote pair.
        """
        reg_result, cls_result = predict_cloud_run(
            mode=mode,
            kaiforecast_version=self.kaiforecast_version,
            base=base,
            quote=quote,
            data=data,
            timeframe=self.model_timeframe,
            ha_trend=ha_trend
        )
        # if prediction fails return nones
        if not reg_result and not cls_result:
            return None, None, None, base, quote
        else:
            cls_prob = cls_result['predictions']
            if (
                    # cls_prob[0] > cls_prob[1] and
                    cls_prob[0] > self.classification_threshold):
                _spread = reg_result['predictions']['percentage_spread']
                _direction = reg_result['predictions']['direction']
                _n_ticks = reg_result['predictions']['n_tick_forward']
                return _spread, _direction, _n_ticks, base, quote
            else:
                return None, None, None, base, quote


class SuperTrendSqueeze(Strategy):
    """ SuperTrend Squeeze strategy implementation
        will scout for potential actionable
        intelligence based on a specific market behavior. 
    """

    def __init__(self,
                 endpoint: str,
                 long_spread: float,
                 long_ttp: float,
                 short_spread: float,
                 short_ttp: float,
                 pairs: dict,
                 log_every: int,
                 expiration_minutes: Optional[Union[int, float]] = None):
        """ Initialize SuperTrendSqueeze class with specified spread & take profit
            percentage thresholds.

            Parameters
            ----------
            endpoint: `str`
                The endpoint to request to layer 2.
            long_spread: `float`
                The longing spread required from layer 2 prediction.
            long_ttp: `float`
                The long signal take profit percentage to take from the signal.
            short_spread: `float`
                The shorting spread required from layer 2 prediction.
            short_ttp: `float`
                The short signal take profit percentage to take from the signal.
            pairs: `dict`
                A dictionary of `long` and `short` pairs allowed to scout.
            log_every: `int`
                Log the metrics from layer 2 every n-iteration.
        """
        super(SuperTrendSqueeze, self).__init__(
            name=str(StrategyType.SUPER_TREND_SQUEEZE),
            strategy=StrategyType.SUPER_TREND_SQUEEZE,
            description="SuperTrend x Squeeze Long vs. Short strategy.",
            endpoint=endpoint,
            long_spread=long_spread,
            long_ttp=long_ttp,
            short_spread=short_spread,
            short_ttp=short_ttp,
            pairs=pairs,
            log_every=log_every,
            expiration_minutes=expiration_minutes
        )
        # in this class we will be using
        # lazybear's momentum squeeze, ema 99
        # supertrend and sma for technical analysis
        self.ema, self.supertrend_len = 99, 24
        self.supertrend_mul, self.sma = 0.6, 20
        self._technical_analysis = [
            {
                "kind": "squeeze",
                "lazybear": True
            },
            {
                "kind": "ema",
                "length": self.ema
            },
            {
                "kind": "supertrend",
                "length": self.supertrend_len,
                "multiplier": self.supertrend_mul
            },
            {
                "kind": "sma",
                "close": "volume",
                "length": self.sma,
                "prefix": "VOLUME"
            }]
        self.technical_analysis = ta.Strategy(name=self.name,
                                              description=self.description,
                                              ta=self._technical_analysis)

    def scout(self,
              base: str,
              quote: str,
              dataframe: pd.DataFrame,
              callback: callable) -> Union[Signal, None]:
        """ Will scout for potential market trigger from  SuperTrend and 
            Momentum Squeeze, if triggered run spread and direction forecast 
            from Layer 2.

            Note
            ----
            *Signal will be created if technical analysis 
            is triggered and a "green light" given from Layer 2.
            In this specific strategy, Layer 2's forecasted 
            potential spread in the next n-tick interval and 
            direction would be the final decision mechanism.*

            Parameters
            ----------
            symbol: `str`
                The ticker symbol.
            base: `str`
                The base pair.
            quote: `str`
                The quote pair.
            dataframe: `pd.DataFrame`
                The klines to run technical analysis on.
            callback: `callable`
                The closing signal callback.
            
            Returns
            -------
            `Union[Signal, None]`
                Will return a Signal object or None.
        """
        start = time.time()
        signal = False
        clean_df = dataframe.copy()
        # format the clean df before inference
        clean_df.rename(columns=dict(
            timeframe="interval", symbol="ticker",
            taker_buy_base_vol="taker_buy_asset_vol"), inplace=True)
        clean_df = clean_df[
            ['open', 'high', 'low', 'close', 'volume', 'close_time',
             'quote_asset_volume', 'number_of_trades', 'taker_buy_asset_vol',
             'taker_buy_quote_vol', 'datetime', 'ticker', 'interval']]
        ta_dataframe = dataframe.copy()
        # retrieve the technical indicators
        ta_dataframe.ta.strategy(self.technical_analysis)
        # run technical analysis for long and short strategy
        # retrieve the necessary indicators
        supertrend = f"SUPERTd_{self.supertrend_len}_{self.supertrend_mul}"
        direction = ta_dataframe.iloc[-1][supertrend]
        squeeze = ta_dataframe.iloc[-1].SQZ_OFF
        last_price = ta_dataframe.iloc[-1].close
        open_price = ta_dataframe.iloc[-1].open
        pair = f"{base}{quote}".upper()
        ttp = 0
        # if direction is long and squeeze is off and green candle
        if (direction == 1 and squeeze == 1 and
                pair in self.pairs['long'] and last_price > open_price):
            # inference to layer 2
            _spread, _direction, _n_tick, base, quote = self.layer2(
                base=base, quote=quote, data=clean_df.to_dict('list'))
            if _spread is None or _direction is None: return None
            # ensure that spread is above threshold and direction matches.
            if _spread >= self.long_spread and _direction == 1:
                ttp = self.long_ttp
                signal = True
            # record the ending time of analysis
            self.save_metrics(start, f"{base}{quote}")
        # else if direction is short and squeeze is off and red candle
        elif (direction == -1 and squeeze == 1 and
              pair in self.pairs['short'] and last_price < open_price):
            # inference to layer 2
            _spread, _direction, _n_tick, base, quote = self.layer2(
                base=base, quote=quote, data=clean_df.to_dict('list'))
            if _spread is None or _direction is None: return None
            # ensure that spread is above threshold and direction matches.
            if _spread >= self.short_spread and _direction == 0:
                ttp = self.short_ttp
                signal = True
            # record the ending time of analysis
            self.save_metrics(start, f"{base}{quote}")
        # return the appropriate result
        return Signal(
            base=base,
            quote=quote,
            take_profit=ttp,
            spread=_spread,
            buffer=_n_tick,
            purchase_price=float(last_price),
            last_price=float(last_price),
            direction=_direction,
            callback=callback,
            n_tick_forward=_n_tick,
            expiration_minutes=self.expiration_minutes
        ) if signal else None


class MaxDrawdownSpread(Strategy):
    """ Maxdrawdown Spread strategy implementation
        will scout for potential actionable
        intelligence based on specific market behavior.
    """

    def __init__(self,
                 endpoint: str,
                 long_spread: float,
                 long_ttp: float,
                 long_max_drawdown: float,
                 short_spread: float,
                 short_ttp: float,
                 short_max_drawdown: float,
                 pairs: dict,
                 log_every: int,
                 buffer: int,
                 expiration_minutes: Optional[Union[int, float]] = None):
        """ Initialize MaxDrawdownSpread class with specified minimum spread, 
            take profit percentage thresholds and max drawdowns.

            Parameters
            ----------
            endpoint: `str`
                The endpoint to request to layer 2.
            long_spread: `float`
                The longing spread required from layer 2 prediction.
            long_ttp: `float`
                The long signal take profit percentage to take from the signal.
            long_max_drawdown: `float`
                The maximum allowable drawdown on a given long signal.
            short_spread: `float`
                The shorting spread required from layer 2 prediction.
            short_ttp: `float`
                The short signal take profit percentage to take from the signal.
            short_max_drawdown: `float`
                The maximum allowable drawdown on a given short signal.
            pairs: `dict`
                A dictionary of `long` and `short` pairs allowed to scout.
            log_every: `int`
                Log the metrics from layer 2 every n-iteration.
            buffer: `int`
                The buffer time before next inference of a symbol.
        """
        super(MaxDrawdownSpread, self).__init__(
            name=str(StrategyType.MAX_DRAWDOWN_SPREAD),
            strategy=StrategyType.MAX_DRAWDOWN_SPREAD,
            description="Maximum Drawdown x Spread Long vs. Short strategy.",
            endpoint=endpoint,
            long_spread=long_spread,
            long_ttp=long_ttp,
            short_spread=short_spread,
            short_ttp=short_ttp,
            pairs=pairs,
            log_every=log_every,
            expiration_minutes=expiration_minutes
        )
        # in this class we will be using maximum drawdowns
        # as the main algorithmic approach from layer 2 predictions
        # also using lazy bear momentum squeeze as trigger.
        self.long_max_drawdown = long_max_drawdown
        self.short_max_drawdown = short_max_drawdown
        # the buffer second before next inference
        self.buffer = buffer
        self.buffers = {}

    def scout(self,
              base: str,
              quote: str,
              dataframe: pd.DataFrame,
              callback: callable) -> Union[Signal, None]:
        """ Will scout for potential market trigger from Momentum Squeeze, 
            if triggered run spread and direction forecast from Layer 2. 
            Once prediction series generated from layer 2 conduct max
            drawdown algorithm to determine the signal's max percentage spread
            and direction.

            Note
            ----
            *Signal will be created if squeeze is off
             and prediction from layer 2 matches the
             max drawdown algorithm.*

            Parameters
            ----------
            symbol: `str`
                The ticker symbol.
            base: `str`
                The base pair.
            quote: `str`
                The quote pair.
            dataframe: `pd.DataFrame`
                The klines to run technical analysis on.
            callback: `callable`
                The closing signal callback.
            
            Returns
            -------
            `Union[Signal, None]`
                Will return a Signal object or None.
        """
        start = time.time()
        signal = False
        clean_df = dataframe.copy()
        # format the clean df before inference
        clean_df.rename(columns=dict(
            timeframe="interval", symbol="ticker",
            taker_buy_base_vol="taker_buy_asset_vol"), inplace=True)
        clean_df = clean_df[
            ['open', 'high', 'low', 'close', 'volume', 'close_time',
             'quote_asset_volume', 'number_of_trades', 'taker_buy_asset_vol',
             'taker_buy_quote_vol', 'datetime', 'ticker', 'interval']]
        high_price = clean_df.iloc[-1].high
        low_price = clean_df.iloc[-1].low
        open_price = clean_df.iloc[-1].open
        last_price = clean_df.iloc[-1].close
        # get the current tick spread
        tick_spread = abs((high_price - low_price) / low_price) * 100
        minimum_spread = min(self.long_ttp, self.short_ttp)
        pair = f"{base}{quote}".upper()
        ttp = 0
        # ensure that the current tick spread is above the minimum
        # spread of either long or short strategy and ensure
        # that the symbol has a valid buffer time before the next inference time.
        if tick_spread >= minimum_spread and self.is_valid_buffer(pair):
            # inference to layer 2
            _spread, _direction, _n_tick, base, quote = self.layer2(
                base=base, quote=quote, data=clean_df.to_dict('list'))
            # if max drawdown algorithm did not pass
            if _spread is None or _direction is None: return None
            # ensure that direction and spread prediction
            # is above specified spread for layer 1
            if (_direction == 1 and pair in self.pairs['long']
                    and last_price > open_price):
                ttp = self.long_ttp
            elif (_direction == 0 and pair in self.pairs['short']
                  and last_price < open_price):
                ttp = self.short_ttp
            else:
                return None
            signal = True
            # record the ending time of analysis
            self.save_metrics(start, f"{base}/{quote}")
            # return the appropriate result
        return Signal(
            base=base,
            quote=quote,
            take_profit=ttp,
            spread=_spread,
            purchase_price=float(last_price),
            last_price=float(last_price),
            direction=_direction,
            callback=callback,
            n_tick_forward=_n_tick,
            expiration_minutes=self.expiration_minutes
        ) if signal else None


class MaxDrawdownSuperTrendSpread(Strategy):
    """ Maxdrawdown SuperTrend Spread strategy implementation
        will scout for potential actionable
        intelligence based on specific market behavior.
    """

    def __init__(self,
                 endpoint: str,
                 long_spread: float,
                 long_ttp: float,
                 long_max_drawdown: float,
                 short_spread: float,
                 short_ttp: float,
                 short_max_drawdown: float,
                 pairs: dict,
                 log_every: int,
                 buffer: int,
                 expiration_minutes: Optional[Union[int, float]] = None):
        """ Initialize MaxDrawdownSuperTrendSpread class with specified 
            minimum spread, take profit percentage thresholds and max 
            drawdowns.

            Parameters
            ----------
            endpoint: `str`
                The endpoint to request to layer 2.
            long_spread: `float`
                The longing spread required from layer 2 prediction.
            long_ttp: `float`
                The long signal take profit percentage to take from the signal.
            long_max_drawdown: `float`
                The maximum allowable drawdown on a given long signal.
            short_spread: `float`
                The shorting spread required from layer 2 prediction.
            short_ttp: `float`
                The short signal take profit percentage to take from the signal.
            short_max_drawdown: `float`
                The maximum allowable drawdown on a given short signal.
            pairs: `dict`
                A dictionary of `long` and `short` pairs allowed to scout.
            log_every: `int`
                Log the metrics from layer 2 every n-iteration.
            buffer: `int`
                The buffer time before next inference of a symbol.
        """
        super(MaxDrawdownSuperTrendSpread, self).__init__(
            name=str(StrategyType.MAX_DRAWDOWN_SUPER_TREND_SPREAD),
            strategy=StrategyType.MAX_DRAWDOWN_SUPER_TREND_SPREAD,
            description="Maximum Drawdown x SuperTrend x Spread Long vs. Short strategy.",
            endpoint=endpoint,
            long_spread=long_spread,
            long_ttp=long_ttp,
            short_spread=short_spread,
            short_ttp=short_ttp,
            pairs=pairs,
            log_every=log_every,
            expiration_minutes=expiration_minutes
        )
        # in this class we will be using maximum drawdowns
        # as the main algorithmic approach from layer 2 predictions
        # also using lazy bear momentum squeeze as trigger.
        self.long_max_drawdown = long_max_drawdown
        self.short_max_drawdown = short_max_drawdown
        # initialize technical analysis for supertrend
        self.supertrend_mul, self.supertrend_len = 0.6, 24
        self._technical_analysis = [
            dict(kind="supertrend",
                 length=self.supertrend_len,
                 multiplier=self.supertrend_mul)
        ]
        self.technical_analysis = ta.Strategy(name=self.name,
                                              description=self.description,
                                              ta=self._technical_analysis)
        # the buffer second before next inference
        self.buffer = buffer
        self.buffers = {}

    def scout(self,
              base: str,
              quote: str,
              dataframe: pd.DataFrame,
              callback: callable) -> Union[Signal, None]:
        """ Will scout for potential market trigger from ticker spread, 
            if triggered run spread and direction forecast from Layer 2. 
            Once prediction series generated from layer 2 conduct max
            drawdown algorithm to determine the signal's max percentage spread
            and direction and match with supertrend.

            Note
            ----
            *Signal will be created if spread is above
             ttp threshold and prediction from layer 2 
             matches the max drawdown algorithm and supertrend.*

            Parameters
            ----------
            symbol: `str`
                The ticker symbol.
            base: `str`
                The base pair.
            quote: `str`
                The quote pair.
            dataframe: `pd.DataFrame`
                The klines to run technical analysis on.
            callback: `callable`
                The closing signal callback.
            
            Returns
            -------
            `Union[Signal, None]`
                Will return a Signal object or None.
        """
        start = time.time()
        signal = False
        clean_df = dataframe.copy()
        # format the clean df before inference
        clean_df.rename(columns=dict(
            timeframe="interval", symbol="ticker",
            taker_buy_base_vol="taker_buy_asset_vol"), inplace=True)
        clean_df = clean_df[
            ['open', 'high', 'low', 'close', 'volume', 'close_time',
             'quote_asset_volume', 'number_of_trades', 'taker_buy_asset_vol',
             'taker_buy_quote_vol', 'datetime', 'ticker', 'interval']]
        ta_dataframe = dataframe.copy()
        # retrieve the technical indicators
        ta_dataframe.ta.strategy(self.technical_analysis)
        # run technical analysis for long and short strategy
        # retrieve the necessary indicators
        supertrend = f"SUPERTd_{self.supertrend_len}_{self.supertrend_mul}"
        prev_direction = ta_dataframe.iloc[-2][supertrend]
        direction = ta_dataframe.iloc[-1][supertrend]
        high_price = clean_df.iloc[-1].high
        low_price = clean_df.iloc[-1].low
        open_price = clean_df.iloc[-1].open
        last_price = clean_df.iloc[-1].close
        # get the current tick spread
        tick_spread = abs((high_price - low_price) / low_price) * 100
        minimum_spread = min(self.long_ttp, self.short_ttp)
        pair = f"{base}{quote}".upper()
        ttp = 0
        # ensure that the current tick spread is above the minimum
        # spread of either long or short strategy and ensure
        # that the symbol has a valid buffer time before the next inference time.
        if tick_spread >= minimum_spread and self.is_valid_buffer(pair):
            # inference to layer 2
            _spread, _direction, _n_tick, base, quote = self.layer2(
                base=base, quote=quote, data=clean_df.to_dict('list'))
            # if max drawdown algorithm did not pass
            if _spread is None or _direction is None: return None
            # ensure that direction and spread prediction
            # is above specified spread for layer 1
            # and pred direction matches supertrend direction
            if (_direction == 1 and pair in self.pairs['long']
                    and direction == 1 and prev_direction == 1):
                ttp = self.long_ttp
            elif (_direction == 0 and pair in self.pairs['short']
                  and direction == -1 and prev_direction == -1):
                ttp = self.short_ttp
            else:
                return None
            signal = True
            # record the ending time of analysis
            self.save_metrics(start, f"{base}/{quote}")
            # return the appropriate result
        return Signal(
            base=base,
            quote=quote,
            take_profit=ttp,
            spread=_spread,
            buffer=_n_tick,
            purchase_price=float(last_price),
            last_price=float(last_price),
            direction=_direction,
            callback=callback,
            n_tick_forward=_n_tick,
            expiration_minutes=self.expiration_minutes
        ) if signal else None


class MaxDrawdownSqueeze(Strategy):
    """ Maxdrawdown Squeeze strategy implementation
        will scout for potential actionable
        intelligence based on specific market behavior.
    """

    def __init__(self,
                 endpoint: str,
                 long_spread: float,
                 long_ttp: float,
                 long_max_drawdown: float,
                 short_spread: float,
                 short_ttp: float,
                 short_max_drawdown: float,
                 pairs: dict,
                 log_every: int,
                 expiration_minutes: Optional[Union[int, float]] = None):
        """ Initialize MaxDrawdownSqueeze class with specified spread, take profit
            percentage thresholds and max drawdowns.

            Parameters
            ----------
            endpoint: `str`
                The endpoint to request to layer 2.
            long_spread: `float`
                The longing spread required from layer 2 prediction.
            long_ttp: `float`
                The long signal take profit percentage to take from the signal.
            long_max_drawdown: `float`
                The maximum allowable drawdown on a given long signal.
            short_spread: `float`
                The shorting spread required from layer 2 prediction.
            short_ttp: `float`
                The short signal take profit percentage to take from the signal.
            short_max_drawdown: `float`
                The maximum allowable drawdown on a given short signal.
            pairs: `dict`
                A dictionary of `long` and `short` pairs allowed to scout.
            log_every: `int`
                Log the metrics from layer 2 every n-iteration.
        """
        super(MaxDrawdownSqueeze, self).__init__(
            name=str(StrategyType.MAX_DRAWDOWN_SQUEEZE),
            strategy=StrategyType.MAX_DRAWDOWN_SQUEEZE,
            description="Maximum Drawdown x Squeeze Long vs. Short strategy.",
            endpoint=endpoint,
            long_spread=long_spread,
            long_ttp=long_ttp,
            short_spread=short_spread,
            short_ttp=short_ttp,
            pairs=pairs,
            log_every=log_every,
            expiration_minutes=expiration_minutes
        )
        # in this class we will be using maximum drawdowns
        # as the main algorithmic approach from layer 2 predictions
        # also using lazy bear momentum squeeze as trigger.
        self.long_max_drawdown = long_max_drawdown
        self.short_max_drawdown = short_max_drawdown
        self._technical_analysis = [dict(kind="squeeze", lazybear=True)]
        self.technical_analysis = ta.Strategy(name=self.name,
                                              description=self.description,
                                              ta=self._technical_analysis)

    def scout(self,
              base: str,
              quote: str,
              dataframe: pd.DataFrame,
              callback: callable) -> Union[Signal, None]:
        """ Will scout for potential market trigger from Momentum Squeeze, 
            if triggered run spread and direction forecast from Layer 2. 
            Once prediction series generated from layer 2 conduct max
            drawdown algorithm to determine the signal's max percentage spread
            and direction.

            Note
            ----
            *Signal will be created if squeeze is off
             and prediction from layer 2 matches the
             max drawdown algorithm.*

            Parameters
            ----------
            symbol: `str`
                The ticker symbol.
            base: `str`
                The base pair.
            quote: `str`
                The quote pair.
            dataframe: `pd.DataFrame`
                The klines to run technical analysis on.
            callback: `callable`
                The closing signal callback.
            
            Returns
            -------
            `Union[Signal, None]`
                Will return a Signal object or None.
        """
        start = time.time()
        signal = False
        clean_df = dataframe.copy()
        # format the clean df before inference
        clean_df.rename(columns=dict(
            timeframe="interval", symbol="ticker",
            taker_buy_base_vol="taker_buy_asset_vol"), inplace=True)
        clean_df = clean_df[
            ['open', 'high', 'low', 'close', 'volume', 'close_time',
             'quote_asset_volume', 'number_of_trades', 'taker_buy_asset_vol',
             'taker_buy_quote_vol', 'datetime', 'ticker', 'interval']]
        ta_dataframe = dataframe.copy()
        # retrieve the technical indicators
        ta_dataframe.ta.strategy(self.technical_analysis)
        # run technical analysis for long and short strategy
        # retrieve the necessary indicators
        squeeze = ta_dataframe.iloc[-1].SQZ_OFF
        last_price = ta_dataframe.iloc[-1].close
        pair = f"{base}{quote}".upper()
        ttp = 0
        # check if squeeze is off
        if squeeze == 1:
            # inference to layer 2
            _spread, _direction, _n_tick, base, quote = self.layer2(
                base=base, quote=quote, data=clean_df.to_dict('list'))
            # if max drawdown algorithm did not pass
            if _spread is None or _direction is None: return None
            # ensure that direction and spread prediction
            # is above specified spread for layer 1
            if _direction == 1 and pair in self.pairs['long']:
                ttp = self.long_ttp
            elif _direction == 0 and pair in self.pairs['short']:
                ttp = self.short_ttp
            else:
                return None
            signal = True
            # record the ending time of analysis
            self.save_metrics(start, f"{base}/{quote}")
        # return the appropriate result
        return Signal(
            base=base,
            quote=quote,
            take_profit=ttp,
            spread=_spread,
            purchase_price=float(last_price),
            last_price=float(last_price),
            direction=_direction,
            callback=callback,
            n_tick_forward=_n_tick,
            expiration_minutes=self.expiration_minutes
        ) if signal else None


__REGISTRY = {
    str(StrategyType.SUPER_TREND_SQUEEZE): SuperTrendSqueeze,
    str(StrategyType.MAX_DRAWDOWN_SQUEEZE): MaxDrawdownSqueeze,
    str(StrategyType.MAX_DRAWDOWN_SPREAD): MaxDrawdownSpread,
    str(StrategyType.MAX_DRAWDOWN_SUPER_TREND_SPREAD): MaxDrawdownSuperTrendSpread,
    str(StrategyType.HEIKIN_ASHI_HYBRID): HeikinAshiHybrid,
    str(StrategyType.HEIKIN_ASHI_REGRESSION): HeikinAshiRegression,
    str(StrategyType.HEIKIN_ASHI_FRAC_DIFF): HeikinAshiFractionalDifference
}


def get_strategy(strategy: StrategyType) -> Union[Strategy, None]:
    """ A helper function that will retrieve the strategy class from `string_id` 
        
        Parameters
        ----------
        strategy: `StrategyType`
            The string type
        
        Returns
        -------
        `Union[Strategy, None]`
            A strategy that inherits `Strategy` class.
    """
    strategy = str(strategy)
    if strategy in __REGISTRY:
        return __REGISTRY[strategy]
    else:
        raise KeyError(f"Strategy {strategy} not found, only: "
                       f"{__REGISTRY.keys()} available!")
