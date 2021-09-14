import pandas as pd
from typing import Union
from .signal import Signal
from .predict import predict
from abc import abstractmethod
from publishers.client import KaiPublisherClient

class Strategy():
    """ Abstract strategy class. """
    def __init__(self, name: str, description: str, spread: float):
        self.name = name
        self.description = description
        self.spread = spread

    @abstractmethod
    def scout(self, dataframe: pd.DataFrame) -> Union[Signal, None]:
        raise NotImplementedError()
    
    def layer2(self, symbol: str, data: list) :
        """ Connect to layer 2 and inference to specific model. 

            Parameters
            ----------
            symbol: `str`
                The ticker symbol.
            data: `list`
                Dictionary containing list of klines.
        """
        result = predict(symbol=symbol, data=data.to_dict('list'))
        if not result: return None, None, None, None, None
        pred = result['predictions']
        return (float(pred['percentage_spread']), 
            int(pred['direction']), 
            int(pred['n_tick_forward']),
            str(result['base']), 
            str(result['quote']))

class SuperTrendSqueeze(Strategy):
    def __init__(self):
        """ SuperTrend Squeeze strategy implementation
            will scout for potential actionable
            intelligence based on a specific market behavior. 
        """
        super().__init__(
            name="SUPERTREND_SQUEEZE", 
            description="SuperTrend x Squeeze Long vs. Short strategy.",
            spread=0.75)
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
    
    def scout(self, 
              symbol: str, 
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
        signal = False
        clean_df = dataframe.copy()
        # retrieve the technical indicators
        dataframe.ta.strategy(self.technical_analysis)
        # run technical analysis for long and short strategy
        # retrieve the necessary indicators
        supertrend = f"SUPERTd_{self.supertrend_len}_{self.supertrend_mul}"
        direction = dataframe.iloc[-1][supertrend]
        squeeze = dataframe.iloc[-1].SQZ_OFF
        last_price = dataframe.iloc[-1].close
        # if direction is long and squeeze is off
        if direction == 1 and squeeze == 1:
            # inference to layer 2
            _spread, _direction, _n_tick, base, quote= self.layer2(
                symbol=symbol, data=clean_df.to_dict('list'))
            if not _spread or not _direction: return None
            # ensure that spread is above threshold and direction matches.
            if _spread >= self.spread and _direction == 1: signal = True
        # else if direction is short and squeeze is off
        elif direction == -1 and squeeze == 1:
            # inference to layer 2
            _spread, _direction, _n_tick, base, quote = self.layer2(
                symbol=symbol, data=clean_df.to_dict('list'))
            if not _spread or not _direction: return None
            # ensure that spread is above threshold and direction matches.
            if _spread >= self.spread and _direction == 0: signal = True
        return Signal(
            base=base,
            quote=quote,
            spread=_spread,
            last_price=last_price,
            direction=_direction,
            n_tick_forward=_n_tick,
            callback=callback) if signal else None

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
    else: raise KeyError(f"Strategy {id} not found, only: {__REGISTRY.keys()} available!")