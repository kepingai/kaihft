import time, logging
import pandas as pd
import numexpr as ne
import pandas_ta as ta
from typing import Union
from .signal import Signal
from .predict import predict
from datetime import datetime
from abc import abstractmethod

class Strategy():
    """ Abstract strategy class. """
    def __init__(self, 
                 name: str, 
                 description: str, 
                 long_spread: float,
                 long_ttp: float,
                 short_spread: float,
                 short_ttp: float,
                 log_every: int):
        self.name = name
        self.description = description
        self.long_spread = long_spread
        self.long_ttp = long_ttp
        self.short_spread = short_spread
        self.short_ttp = short_ttp
        self.log_every = log_every
        # initialize multi-core threads
        ne.set_vml_num_threads(8)
        # initialize running metrics
        self.metrics = {}

    @abstractmethod
    def scout(self, dataframe: pd.DataFrame) -> Union[Signal, None]:
        raise NotImplementedError()
    
    def layer2(self, base: str, quote: str, data: dict) :
        """ Connect to layer 2 and inference to specific model. 

            Parameters
            ----------
            base: `str`
                The base symbol of the ticker.
            quote: `str`
                The quote symbol of the ticker.
            data: `dict`
                Dictionary containing list of klines.
        """
        result = predict(base=base, quote=quote, data=data)
        if not result: return None, None, None, base, quote
        pred = result['predictions']
        return (float(pred['percentage_spread']), 
            int(pred['direction']), 
            int(pred['n_tick_forward']),
            str(result['base']), 
            str(result['quote']))
    
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
                for key, value in self.metrics[symbol].items()
                if key != 'count'])
            logging.info(f"[metrics] strategy-{self.name}, symbol: {symbol}, "
                f"metrics: {metrics}")
            # reset the count to 1 to avoid memory leak
            self.metrics[symbol]['count'] = 1

class SuperTrendSqueeze(Strategy):
    def __init__(self, 
                long_spread: float,
                long_ttp: float,
                short_spread: float,
                short_ttp: float,
                log_every: int):
        """ SuperTrend Squeeze strategy implementation
            will scout for potential actionable
            intelligence based on a specific market behavior. 
        """
        super().__init__(
            name="SUPERTREND_SQUEEZE", 
            description="SuperTrend x Squeeze Long vs. Short strategy.",
            long_spread=long_spread,
            long_ttp=long_ttp,
            short_spread=short_spread,
            short_ttp=short_ttp,
            log_every=log_every)
        # in this class we will be using
        # lazybear's momentum squeeze, ema 99
        # supertrend and sma for technical analysis
        self.ema, self.supertrend_len = 99, 24
        self.supertrend_mul, self.sma = 1.0, 20
        self.technical_analysis = [
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
        self.strategy = ta.Strategy(name=self.name,
            description=self.description, ta=self.technical_analysis)
        logging.info(f"[strategy] Strategy initialized {self.name}, "
            f"long_spread: {self.long_spread}, long_ttp: {self.long_ttp}, "
            f"short_spread: {self.short_spread}, short_ttp: {self.short_ttp}")
    
    def scout(self, 
              base: str, 
              quote: str,
              dataframe: pd.DataFrame, 
              callback: callable) -> Union[Signal, None]:
        """ Will scout for potential market trigger from  
            SuperTrend and Momentum Squeeze, if triggered
            run spread and direction forecast from Layer 2.

            Note
            ----
            Signal will be created if technical analysis 
            is triggered and a "green light" given from Layer 2.
            In this specific strategy, Layer 2's forecasted 
            potential spread in the next n-tick interval and 
            direction would be the final decision mechanism.

            Parameters
            ----------
            symbol: `str`
                The ticker symbol.
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
        # print(clean_df.tail(20))
        ta_dataframe = dataframe.copy()
        # retrieve the technical indicators
        ta_dataframe.ta.strategy(self.strategy)
        # run technical analysis for long and short strategy
        # retrieve the necessary indicators
        supertrend = f"SUPERTd_{self.supertrend_len}_{self.supertrend_mul}"
        direction = ta_dataframe.iloc[-1][supertrend]
        squeeze = ta_dataframe.iloc[-1].SQZ_OFF
        last_price = ta_dataframe.iloc[-1].close
        ttp = 0
        # if direction is long and squeeze is off
        if direction == 1 and squeeze == 1:
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
        # else if direction is short and squeeze is off
        elif direction == -1 and squeeze == 1:
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
            purchase_price=float(last_price),
            last_price=float(last_price),
            direction=_direction,
            callback=callback,
            n_tick_forward=_n_tick) if signal else None

__REGISTRY = {
    "STS": SuperTrendSqueeze
}

def get_strategy(id: str) -> Union[Strategy, None]:
    """ Will retrieve the strategy class from string_id 
        
        Parameters
        ----------
        id: `str`
            The string id for strategy.
        
        Returns
        -------
        `Union[Strategy, None]`
            A strategy that inherits `Strategy` class.
    """
    if id in __REGISTRY: return __REGISTRY[id]
    else: raise KeyError(f"Strategy {id} not found, only: "
        f"{__REGISTRY.keys()} available!")