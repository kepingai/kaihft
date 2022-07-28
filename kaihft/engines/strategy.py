import time
import logging
import pandas as pd
import numpy as np
import numexpr as ne
import pandas_ta as ta
from enum import Enum
from typing import Union, Tuple, Optional
from .signal import Signal
from .predict import predict, predict_cloud_run, predict_cloud_run_regression
from datetime import datetime, timedelta
from abc import abstractmethod
from google.cloud import pubsub_v1
import json
import ccxt
import os
import requests


class StrategyType(Enum):
    SUPER_TREND_SQUEEZE = "SUPER_TREND_SQUEEZE"
    MAX_DRAWDOWN_SQUEEZE = "MAX_DRAWDOWN_SQUEEZE"
    MAX_DRAWDOWN_SPREAD = "MAX_DRAWDOWN_SPREAD"
    MAX_DRAWDOWN_SUPER_TREND_SPREAD = "MAX_DRAWDOWN_SUPER_TREND_SPREAD"
    HEIKIN_ASHI_HYBRID = "HEIKIN_ASHI_HYBRID"
    HEIKIN_ASHI_REGRESSION = "HEIKIN_ASHI_REGRESSION"

    # layer 1 strategy for index bot
    INDEX_24HRS_AVERAGE = "INDEX_24HRS_AVERAGE"

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
        ne.set_vml_num_threads(8)
        # initialize running metrics
        self.metrics = {}
        self.buffer = 5 # default buffer
        self.buffers = {}
        logging.info(f"[strategy] Strategy initialized {self.name}  ({self.strategy}), "
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
        result = predict(endpoint=self.endpoint, base=base, quote=quote, data=data)
        # if prediction fails return nones
        if not result: return None, None, None, base, quote
        # retrieve the predictions dictionary only
        pred = result['predictions']
        # conduct strategy based upon strategy type
        if 'MAX_DRAWDOWN' in str(self.strategy).upper():
            # ensure that there's percentage array in the prediction
            # note that only some of the models has this integrated
            if 'percentage_arr' not in pred: return None, None, None, base, quote
            percentage_spread, direction = self.select_direction(pred['percentage_arr'])
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
            logging.error(f"[layer2] strategy: {self.strategy} is not implemented yet!")
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
        if (margin_long > 0) and (margin_short > 0): direction = 'long'
        # Option: SHORT only
        elif (margin_long < 0) and (margin_short < 0): direction = 'short'
        # Option: Available for both LONG and SHORT
        elif margin_long > 0 and margin_short < 0:
            direction = 'short' if np.abs(margin_short) > np.abs(margin_long) else 'long'
        else: direction = None
        # ensure that within predicted spreads it will not move
        # above the allowed maximum drawdowns from both directions
        if direction == 'long' and all(predicted_spread > (-1 * self.long_max_drawdown)):
            return float(np.abs(max_pred)), 1
        elif direction == 'short' and all(predicted_spread < self.short_max_drawdown):
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
            total_execution_time = self.metrics[symbol]['total_execution_time'] + execution_time
            self.metrics[symbol].update(
                dict(
                    last_execution_time=execution_time,
                    last_utc_timestamp=utctime,
                    count=count,
                    average_execution_time=round(total_execution_time/count, 2),
                    total_execution_time=round(total_execution_time, 2)
                )
            )
        if self.metrics[symbol]['count'] % self.log_every == 0:
            metrics = ", ".join([f"{key} : {value}" 
                for key, value in self.metrics[symbol].items()])
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
            self.buffers[symbol] = datetime.utcnow() + timedelta(seconds=self.buffer)
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
        self.ha_trend, self.ha_candle = {}, {}
        for _, v in pairs.items():
            for p in v:
                if p not in self.ha_trend: self.ha_trend[p] = 0
                if p not in self.ha_candle: self.ha_candle[p] = 0
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
        clean_df = clean_df[['open', 'high', 'low', 'close', 'volume', 'close_time',
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

    def calculate_heikin_ashi_trend(self, message: pubsub_v1.subscriber.message.Message):
        """ This function calculates the buy and sell signals based on Coinsspor Heikin Ashi Tradingview signals.

            Parameters
            ----------
            message: `pubsub_v1.subscriber.message.Message`
                message containing the klines which will be analysed using Heikin-Ashi

        """
        if message.attributes and 'timestamp' in message.attributes:
            # get the attributes of the message
            symbol = message.attributes.get("symbol")
            timestamp = int(message.attributes.get("timestamp"))
            klines_time = datetime.utcfromtimestamp(timestamp / 1000)
            seconds_passed = (datetime.utcnow() - klines_time).total_seconds()
            # only accept messages within 1 seconds latency
            if 100 >= seconds_passed >= 0:
                # get the symbol of the klines
                base = message.attributes.get('base')
                quote = message.attributes.get('quote')
                # only run strategy if symbol is currently
                # not an ongoing signal and also not currently
                # awaiting for a result from running strategy

                # run a separate thread to run startegy
                klines = pd.DataFrame(json.loads(message.data.decode('utf-8'))['data'])
                ha_dataframe = self.format_dataframe(klines)

                # ohlc4 is a tradingview variable. It is the average of the OHLC value
                ohlc4 = (ha_dataframe['open'] + ha_dataframe['high'] + ha_dataframe['low'] + ha_dataframe[
                    'close']) / 4

                # calculate the heiken ashi candles and rename the columns so that we can calculate the EMA
                heikin_ashi_klines = ha_dataframe.ta.ha()
                heikin_ashi_klines = heikin_ashi_klines.rename(
                    columns={'HA_open': 'open', 'HA_high': 'high', 'HA_low': 'low', 'HA_close': 'close'})

                # get the latest heikin-ashi candle of the specific symbol
                ha_candles = heikin_ashi_klines["close"] - heikin_ashi_klines["open"]
                self.ha_candle[symbol] = 1 if ha_candles.iloc[-1] > 0 else -1

                # start calculation. See tradingview for reference
                hac = pd.DataFrame((ohlc4 + heikin_ashi_klines['open'].fillna(0)
                                    + pd.concat([ha_dataframe['high'], heikin_ashi_klines['open'].fillna(0)]).max(
                            level=0)
                                    + pd.concat([ha_dataframe['low'], heikin_ashi_klines['open'].fillna(0)]).min(
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
                hlc3 = pd.DataFrame((ha_dataframe['high'] + ha_dataframe['low'] + ha_dataframe['close']) / 3,
                                    columns=['close'])
                ema7 = pd.DataFrame(hlc3.ta.ema(length=self.ha_ema_len)).rename(
                    columns={f'EMA_{self.ha_ema_len}': 'close'})
                ema8 = pd.DataFrame(ema7.ta.ema(length=self.ha_ema_len)).rename(
                    columns={f'EMA_{self.ha_ema_len}': 'close'})
                ema9 = pd.DataFrame(ema8.ta.ema(length=self.ha_ema_len)).rename(
                    columns={f'EMA_{self.ha_ema_len}': 'close'})
                tma3 = 3 * ema7 - 3 * ema8 + ema9
                ema10 = pd.DataFrame(tma3.ta.ema(length=self.ha_ema_len)).rename(
                    columns={f'EMA_{self.ha_ema_len}': 'close'})
                ema11 = pd.DataFrame(ema10.ta.ema(length=self.ha_ema_len)).rename(
                    columns={f'EMA_{self.ha_ema_len}': 'close'})
                ema12 = pd.DataFrame(ema11.ta.ema(length=self.ha_ema_len)).rename(
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
                trend = np.array(np.array(mavi) > np.array(kirmizi))

                self.ha_trend[symbol] = 1 if np.all(trend[-2:]) else -1

        # acknowledge the message
        message.ack()

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
            #if 'percentage_arr' not in pred: return None, None, None, base, quote
            #_spread, _direction = self.select_direction(pred['percentage_arr'])
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
            description=self.description, ta=self._technical_analysis)
    
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
        clean_df = clean_df[['open', 'high', 'low', 'close', 'volume', 'close_time',
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
        clean_df = clean_df[['open', 'high', 'low', 'close', 'volume', 'close_time',
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
            else: return None
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
            description=self.description, ta=self._technical_analysis)
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
        clean_df = clean_df[['open', 'high', 'low', 'close', 'volume', 'close_time',
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
            else: return None
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
            description=self.description, ta=self._technical_analysis)
    
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
        clean_df = clean_df[['open', 'high', 'low', 'close', 'volume', 'close_time',
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
            if _direction == 1 and pair in self.pairs['long']: ttp = self.long_ttp
            elif _direction == 0 and pair in self.pairs['short']: ttp = self.short_ttp
            else: return None
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


class Index24HrsAverage(Strategy):
    """ Index24HrsAverage strategy's implementation checks
        if the average of price difference of all assets of an index
        is below the threshold value.
    """
    def __init__(self):
        self.threshold = -10

    def scout(self, base: str, quote: str, dataframe: pd.DataFrame,
              callback: callable) -> Union[Signal, None]:
        pass

    def send_index_signal(self,
                          exchange: ccxt.Exchange,
                          symbols: list,
                          quote: str) -> bool:
        """ Send the index signal if the criteria is met.

            Parameters
            ----------
            exchange: `ccxt.Exchange`
                The exchange class.
            symbols: `list`
                A list of the assets' symbols for an index.
            quote: `str`
                The quote instrument for the assets.

            Return
            ------
            `bool`
                True if criteria is met.
        """
        # calculate the closing price difference for each asset
        diff_percent = {}
        for symbol in symbols:
            _symbol = symbol + f"{quote}"
            # get hourly ohlcv data for 1 day
            ohlcv = exchange.fetch_ohlcv(symbol=_symbol, timeframe="1h",
                                         limit=24)
            # calculate 24hrs closing price difference in percentage value
            first_close_price = ohlcv[0][-2]
            last_close_price = ohlcv[-1][-2]
            close_diff_percent = (last_close_price - first_close_price) / \
                                 first_close_price * 100
            diff_percent[symbol] = close_diff_percent

        # the average
        avg_diff_percent = sum(list(diff_percent.values())) / len(diff_percent)
        logging.info(f"inside send_index_signal") # TODO debug
        logging.info(f"diff_percent: {diff_percent}")  # TODO debug
        logging.info(f"avg_diff_percent: {avg_diff_percent}")  # TODO debug
        return avg_diff_percent <= self.threshold

__REGISTRY = {
    str(StrategyType.SUPER_TREND_SQUEEZE): SuperTrendSqueeze,
    str(StrategyType.MAX_DRAWDOWN_SQUEEZE): MaxDrawdownSqueeze,
    str(StrategyType.MAX_DRAWDOWN_SPREAD): MaxDrawdownSpread,
    str(StrategyType.MAX_DRAWDOWN_SUPER_TREND_SPREAD): MaxDrawdownSuperTrendSpread,
    str(StrategyType.HEIKIN_ASHI_HYBRID): HeikinAshiHybrid,
    str(StrategyType.HEIKIN_ASHI_REGRESSION): HeikinAshiRegression,
    str(StrategyType.INDEX_24HRS_AVERAGE): Index24HrsAverage
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
    if strategy in __REGISTRY: return __REGISTRY[strategy]
    else: raise KeyError(f"Strategy {strategy} not found, only: "
        f"{__REGISTRY.keys()} available!")
