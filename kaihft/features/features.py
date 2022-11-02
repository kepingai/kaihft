from .market_feature import MarketFeature
from sklearn.preprocessing import StandardScaler
import pandas as pd
import numpy as np


class OnBalanceVolume(MarketFeature):
    """
    Calculates the OnBalanceVolume.
    
    Parameters
    ----------
    feature_name: `str`
        The name of the feature. Default: 'OnBalanceVolume'
    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'
    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn’s StandardScaler()
    """

    def __init__(self, feature_name='OnBalanceVolume',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler(), ):
        super(OnBalanceVolume, self).__init__(feature_name, feature_type, scaler)

    def preprocess(self, dataframe):
        assert isinstance(dataframe, pd.DataFrame)
        obv = dataframe.ta.obv()
        return obv.values
      
        
class UlcerIndex(MarketFeature):
    """
    Calculates the UlcerIndex.
    
    Parameters
    ----------
    feature_name: `str`
        The name of the feature. Default: 'UlcerIndex'
    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'
    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn’s StandardScaler()
    """

    def __init__(self, feature_name='UlcerIndex',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler(), ):
        super(UlcerIndex, self).__init__(feature_name, feature_type, scaler)

    def preprocess(self, dataframe):
        assert isinstance(dataframe, pd.DataFrame)
        ui = dataframe.ta.ui()
        return ui.values


class LogReturn(MarketFeature):
    """
    Calculates the logarithmic return of an asset.

    Parameters
    ----------
    feature_name: `str`
    The name of the feature. Default: 'LogReturn'

    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'

    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn’s StandardScaler()

    diff_period: `int`
        It's period. Default: 20

    cumulative: `bool`
        If True, returns the cumulative returns. Default: False

    raw_feature: `bool`
        If to output raw feature without scaling & normalization.
        Default: False.
    """

    def __init__(self, feature_name='LogReturn',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler(),
                 raw_feature=True,
                 diff_period=20,
                 cumulative=False):
        super(LogReturn, self).__init__(feature_name, feature_type, scaler)
        self._raw_feature = raw_feature
        self._diff_period = diff_period
        self._cumulative = cumulative
        self._pre_multiplier = 1000
        self._norm_power = 5e-1
        self._norm_multiplier = 1000

    def preprocess(self, dataframe):
        log_return = dataframe.ta.log_return(length=self._diff_period,
                                             cumulative=self._cumulative)
        if self._raw_feature:
            return log_return.values
        log_return *= self._pre_multiplier
        transformed_data = ((pow(log_return.abs() + 1, self._norm_power) - 1)
                            * np.sign(log_return)
                            * self._norm_multiplier)
        return transformed_data.values

    def reverse_transform(self, data):
        assert isinstance(data, np.ndarray), "Input is not numpy array"
        if self._raw_feature:
            return data.values
        reversed_data = (((pow((np.abs(data) / self._norm_multiplier + 1),
                               1 / self._norm_power) - 1) * np.sign(data))
                         / self._pre_multiplier)
        return reversed_data


class PercentReturn(MarketFeature):
    """
    Calculates the percent return of an asset.

    Parameters
    ----------
    feature_name: `str`
        The name of the feature. Default: 'PercentReturn'

    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'

    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn's StandardScaler()

    raw_feature: `bool`
        If to output raw feature without scaling & normalization.
        Default: False.
    """

    def __init__(self, feature_name='PercentReturn',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler(),
                 raw_feature=True):
        super(PercentReturn, self).__init__(feature_name, feature_type, scaler)
        self._raw_feature = raw_feature
        # attributes for transformation, may not be modified outside the class
        self._pre_multiplier = 100
        self._norm_power = 1e-1
        self._norm_multiplier = 1000

    def preprocess(self, dataframe):
        assert isinstance(dataframe, pd.DataFrame), 'input is not a pandas dataframe'
        percent_return = dataframe[['close']].ta.percent_return(
            cumulative=False)
        if self._raw_feature:
            return percent_return.values
        percent_return *= self._pre_multiplier
        transformed_data = ((pow(percent_return.abs() + 1, self._norm_power) - 1)
                            * np.sign(percent_return)
                            * self._norm_multiplier)
        return transformed_data.values

    def reverse_transform(self, data):
        assert isinstance(data, np.ndarray), "Input is not numpy array"
        if self._raw_feature:
            return data
        reversed_data = ((pow(((data / self._norm_multiplier / np.sign(data)) + 1),
                              1 / self._norm_power) - 1)
                         * np.sign(data)
                         / self._pre_multiplier)

        # Make sure it does not return nan values except for the first element
        reversed_data[1:][np.isnan(reversed_data[1:])] = 0.0
        return reversed_data


class RawPrice(MarketFeature):
    """
    Returns the raw close price of an asset.

    Parameters
    ----------

    feature_name: `str`
        The name of the feature. Default: 'RawPrice'

    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'

    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn’s StandardScaler()
    """

    def __init__(self, name='RawPrice',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler()):
        super(RawPrice, self).__init__(name, feature_type, scaler)
        # attributes for transformation, may not be modified outside the class
        self._pre_multiplier = None
        self._norm_power = None
        self._norm_multiplier = None

    def preprocess(self, dataframe):
        assert isinstance(dataframe, pd.DataFrame)
        return dataframe['close'].values


class PriceToVWAP(MarketFeature):
    """
    Calculates the difference between price and VWAP of an asset.

    Parameters
    ----------
    feature_name: `str`
        The name of the feature. Default: 'PriceToVWAP'

    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'

    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn's StandardScaler()

    raw_feature: `bool`
        If to output raw feature without scaling & normalization.
        Default: False.
    """

    def __init__(self, feature_name='PriceToVWAP',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler(),
                 raw_feature=False):
        super(PriceToVWAP, self).__init__(feature_name, feature_type, scaler)
        self._raw_feature = raw_feature
        # attributes for transformation, may not be modified outside the class
        self._pre_multiplier = 1
        self._norm_power = 7e-1
        self._norm_multiplier = 1000

    def preprocess(self, dataframe):
        assert isinstance(dataframe, pd.DataFrame), "Input is not a pandas dataframe"
        dataframe.set_index(
            pd.DatetimeIndex(pd.DatetimeIndex(dataframe["datetime"])), inplace=True)
        vwap = dataframe.ta.vwap(high=dataframe['high'],
                                 low=dataframe['low'],
                                 close=dataframe['close'],
                                 volume=dataframe['volume'])
        price_to_vwap = (dataframe['close'] - vwap)
        if self._raw_feature:
            return price_to_vwap.values
        price_to_vwap *= self._pre_multiplier
        transformed_data = ((pow(price_to_vwap.abs() + 1, self._norm_power) - 1)
                            * np.sign(price_to_vwap)
                            * self._norm_multiplier)
        return transformed_data.values


class VWAP(MarketFeature):
    """
    Calculates VWAP of an asset.

    Parameters
    ----------
    feature_name: `str`
        The name of the feature. Default: 'VWAP'

    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'

    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn's StandardScaler()
    """

    def __init__(self, feature_name='VWAP',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler()):
        super(VWAP, self).__init__(feature_name, feature_type, scaler)

    def preprocess(self, dataframe):
        assert isinstance(dataframe, pd.DataFrame), "Input is not a pandas dataframe"
        dataframe.set_index(
            pd.DatetimeIndex(pd.DatetimeIndex(dataframe["datetime"])), inplace=True)
        vwap = dataframe.ta.vwap(high=dataframe['high'],
                                 low=dataframe['low'],
                                 close=dataframe['close'],
                                 volume=dataframe['volume'])
        return vwap.values


class RSI(MarketFeature):
    """
    Calculates the RSI of an asset.

    Parameters
    ----------
    feature_name: `str`
        The name of the feature. Default: 'RSI'

    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'

    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn's StandardScaler()

    raw_feature: `bool`
        If to output raw feature without scaling & normalization.
        Default: False.
    """

    def __init__(self, feature_name='RSI',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler(),
                 raw_feature=True):
        super(RSI, self).__init__(feature_name, feature_type, scaler)
        self._raw_feature = raw_feature
        # attributes for transformation, may not be modified outside the class
        self._pre_multiplier = 1
        self._norm_power = 2
        self._norm_multiplier = 1000

    def preprocess(self, dataframe):
        assert isinstance(dataframe, pd.DataFrame)
        rsi = dataframe.ta.rsi()
        if self._raw_feature:
            return rsi.values
        rsi *= self._pre_multiplier
        transformed_data = ((pow(rsi.abs() + 1, self._norm_power) - 1)
                            * np.sign(rsi)
                            * self._norm_multiplier)
        return transformed_data.values


class CCI(MarketFeature):
    """
    Calculates the CCI of an asset.

    Parameters
    ----------
    feature_name: `str`
        The name of the feature. Default: 'CCI'

    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'

    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn’s StandardScaler()
    """

    def __init__(self, feature_name='CCI',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler(), ):
        super(CCI, self).__init__(feature_name, feature_type, scaler)

    def preprocess(self, dataframe):
        assert isinstance(dataframe, pd.DataFrame)
        cci = dataframe.ta.cci(high=dataframe['high'],
                               low=dataframe['low'],
                               close=dataframe['close'])
        return cci.values


class MACD(MarketFeature):
    """
    Calculates the Keping's Proprietary Normalized MACD of an asset.

    Parameters
    ----------
    feature_name: `str`
        The name of the feature. Default: 'MACD'

    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'

    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn’s StandardScaler()

    fast: `int`
        The short period. Default: 12

    slow: `int`:
        The long period. Default: 26

    signal: `int`
        The signal period. Default: 9

    offset: `int`
        How many periods to offset the result. Default: 0

    raw_feature: `bool`
        If to output raw feature without scaling & normalization.
        Default: False.

    """

    def __init__(self, feature_name='MACD',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler(),
                 raw_feature=True,
                 fast=12,
                 slow=26,
                 signal=9,
                 offset=0):
        super(MACD, self).__init__(feature_name, feature_type, scaler)
        self._raw_feature = raw_feature
        self._fast = fast
        self._slow = slow
        self._signal = signal
        self._offset = offset
        self._pre_multiplier = 1000
        self._norm_power = 2e-1
        self._norm_multiplier = 1000

    def preprocess(self, dataframe):
        macd_hist = dataframe.ta.macd(
            fast=self._fast,
            slow=self._slow,
            signal=self._signal,
            offset=self._offset,
        )[f"MACDh_{self._fast}_{self._slow}_{self._signal}"]
        if self._raw_feature:
            return macd_hist.values
        norm_macd_hist = macd_hist / dataframe['close'] * self._pre_multiplier
        transformed_data = ((pow(norm_macd_hist.abs() + 1, self._norm_power) - 1)
                            * np.sign(norm_macd_hist)
                            * self._norm_multiplier)
        return transformed_data.values


class StochasticDLine(MarketFeature):
    """
    Calculates the Stochastic D-Line of an asset.

    Parameters
    ----------
    feature_name: `str`
        The name of the feature. Default: 'Stochastic-DLine'

    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'

    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn’s StandardScaler()

    k: `int`
        The Fast %K period. Default: 14

    d: `int`
        The Slow %K period. Default: 3

    smooth_k: `int`
        The Slow %D period. Default: 3

    offset: `int`
        How many periods to offset the result. Default: 0

    """

    def __init__(self, feature_name='StochasticDLine',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler(),
                 k=14,
                 d=3,
                 smooth_k=3,
                 offset=0):
        super(StochasticDLine, self).__init__(feature_name, feature_type, scaler)
        self._k = k
        self._d = d
        self._smooth_k = smooth_k
        self._offset = offset

    def preprocess(self, dataframe):
        stoch_dline = dataframe.ta.stoch(
            k=self._k,
            d=self._d,
            smooth_k=self._smooth_k,
            offset=self._offset,
        )[f"STOCHd_{self._k}_{self._d}_{self._smooth_k}"]
        return stoch_dline.values


class AverageDirectionalIndex(MarketFeature):
    """
    Calculates the ADX of an asset.

    Parameters
    ----------
    feature_name: `str`
        The name of the feature. Default: 'ADX'

    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'

    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn's StandardScaler()

    raw_feature: `bool`
        If to output raw feature without scaling & normalization.
        Default: False.
    """

    def __init__(self, feature_name='ADX',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler(),
                 raw_feature=True):
        super(AverageDirectionalIndex, self).__init__(feature_name,
                                                      feature_type,
                                                      scaler)
        self._raw_feature = raw_feature
        # attributes for transformation, may not be modified outside the class
        self._pre_multiplier = 1
        self._norm_power = None
        self._norm_multiplier = None

    def preprocess(self, dataframe):
        assert isinstance(dataframe, pd.DataFrame), "Input is not a pandas dataframe"
        adx = dataframe.ta.adx()["ADX_14"]
        if self._raw_feature:
            return adx.values
        adx *= self._pre_multiplier
        transformed_data = ((np.log(adx.abs())) * np.sign(adx))
        return transformed_data.values


class DirectionalMovement(MarketFeature):
    """
    Calculates the DM of an asset.

    Parameters
    ----------
    feature_name: `str`
        The name of the feature. Default: 'DM'

    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'

    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn's StandardScaler()

    raw_feature: `bool`
        If to output raw feature without scaling & normalization.
        Default: False.
    """

    def __init__(self, feature_name='DM',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler(),
                 raw_feature=True):
        super(DirectionalMovement, self).__init__(feature_name,
                                                  feature_type,
                                                  scaler)
        self._raw_feature = raw_feature
        # attributes for transformation, may not be modified outside the class
        self._pre_multiplier = 0.001
        self._norm_power = 1.2
        self._norm_multiplier = 1

    def preprocess(self, dataframe):
        assert isinstance(dataframe, pd.DataFrame), "input is not a pandas dataframe"
        adx_result = dataframe.ta.adx()
        directional_movement = (adx_result["DMP_14"] - adx_result["DMN_14"])
        if self._raw_feature:
            return directional_movement.values
        directional_movement *= self._pre_multiplier
        transformed_dm = ((pow(directional_movement.abs() + 1, self._norm_power) - 1)
                          * np.sign(directional_movement)
                          * self._norm_multiplier)
        return transformed_dm.values


class StochasticKDDiff(MarketFeature):
    """
    Calculates the Stochastic K Line and D Line Difference of an asset.

    Parameters
    ----------

    feature_name: `str`
        The name of the feature. Default: 'StochasticKDDiff'

    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'

    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn’s StandardScaler()

    k: `int`
        The Fast %K period. Default: 14

    d: `int`
        The Slow %K period. Default: 3

    smooth_k: `int`
        The Slow %D period. Default: 3

    offset: `int`
        How many periods to offset the result. Default: 0

    """

    def __init__(self, feature_name='StochasticKDDiff',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler(),
                 k=14,
                 d=3,
                 smooth_k=3,
                 offset=0):
        super(StochasticKDDiff, self).__init__(feature_name, feature_type, scaler)
        self._k = k
        self._d = d
        self._smooth_k = smooth_k
        self._offset = offset

    def preprocess(self, dataframe):
        stochastic = dataframe.ta.stoch(
            k=self._k,
            d=self._d,
            smooth_k=self._smooth_k,
            offset=self._offset,
        )
        k_line_column = f"STOCHk_{self._k}_{self._d}_{self._smooth_k}"
        d_line_column = f"STOCHd_{self._k}_{self._d}_{self._smooth_k}"
        stoch_kd_diff = stochastic[k_line_column] - stochastic[d_line_column]
        return stoch_kd_diff.values


class ATR(MarketFeature):
    """
    Calculates Volatility Measure Average True Range(ATR) of an asset.

    Parameters
    ----------
    feature_name: `str`
        The name of the feature. Default: 'ATR'

    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'

    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn’s StandardScaler()

    length: `int`
        It's period. Default: 14

    mamode: `str`
        "sma", "ema", "wma" or "rma". Default: "rma"

    drift: `int`
        The difference period. Default: 1

    offset: `int`
        How many periods to offset the result. Default: 0

    raw_feature: `bool`
        If to output raw feature without scaling & normalization.
        Default: False.
    """

    def __init__(self, feature_name='ATR',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler(),
                 raw_feature=True,
                 log_norm=False,
                 length=14,
                 multiplier=1.0,
                 mamode="sma",
                 drift=1,
                 offset=0):
        super(ATR, self).__init__(feature_name, feature_type, scaler)
        self._raw_feature = raw_feature
        self._log_norm = log_norm
        self._length = length
        self._multiplier = multiplier
        self._mamode = mamode
        self._drift = drift
        self._offset = offset

    def preprocess(self, dataframe):
        atr = dataframe.ta.atr(length=self._length,
                               mamode=self._mamode,
                               drift=self._drift,
                               offset=self._offset)
        if self._raw_feature:
            return atr.values
        norm_atr = atr / dataframe['close'] * self._multiplier
        if self._log_norm:
            norm_atr = np.log(norm_atr)
        return norm_atr.values


class BollingerLowBand(MarketFeature):
    """
    Calculates the price position in relative to Bollinger Lower Band.

    Parameters
    ----------
    feature_name: `str`
        The name of the feature. Default: 'BollingerLowBand'

    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'

    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn’s StandardScaler()

    length: `int`
        The short period. Default: 20

    std: `int`
        The long period. Default: 2

    mamode: `str`
        Two options: "sma" or "ema". Default: "sma"

    ddof: `int`
        Degrees of Freedom to use. Default: 0

    offset: `int`
        How many periods to offset the result. Default: 0

    raw_feature: `bool`
        If to output raw feature without scaling & normalization.
        Default: False.
    """

    def __init__(self, feature_name='BollingerLowBand',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler(),
                 raw_feature=True,
                 length=20,
                 std=2,
                 mamode="sma",
                 ddof=0,
                 offset=0):
        super(BollingerLowBand, self).__init__(feature_name, feature_type, scaler)
        self._raw_feature = raw_feature
        self._length = length
        self._std = std
        self._mamode = mamode
        self._ddof = ddof
        self._offset = offset

    def preprocess(self, dataframe):
        bb = dataframe.ta.bbands(length=self._length,
                                 std=self._std,
                                 mamode=self._mamode,
                                 ddof=self._ddof,
                                 offset=self._offset)
        bb_low = bb[f'BBL_{self._length}_{self._std}.0']
        if self._raw_feature:
            return bb_low.values
        price2bb_low = (dataframe['close'] - bb_low) / bb_low
        return price2bb_low.values


class BollingerUpBand(MarketFeature):
    """
    Calculates the price position in relative to Bollinger Upper Band.

    Parameters
    ----------

    feature_name: `str`
        The name of the feature. Default: 'BollingerUpBand'

    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'

    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn’s StandardScaler()

    length: `int`
        The short period. Default: 20

    std: `int`
        The long period. Default: 2

    mamode: `str`
        Two options: "sma" or "ema". Default: "sma"

    ddof: `int`
        Degrees of Freedom to use. Default: 0

    offset: `int`
        How many periods to offset the result. Default: 0

    raw_feature: `bool`
        If to output raw feature without scaling & normalization.
        Default: False.
    """

    def __init__(self, feature_name='BollingerUpBand',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler(),
                 raw_feature=True,
                 length=20,
                 std=2,
                 mamode="sma",
                 ddof=0,
                 offset=0):
        super(BollingerUpBand, self).__init__(feature_name, feature_type, scaler)
        self._raw_feature = raw_feature
        self._length = length
        self._std = std
        self._mamode = mamode
        self._ddof = ddof
        self._offset = offset

    def preprocess(self, dataframe):
        bb = dataframe.ta.bbands(length=self._length,
                                 std=self._std,
                                 mamode=self._mamode,
                                 ddof=self._ddof,
                                 offset=self._offset)
        bb_up = bb[f'BBU_{self._length}_{self._std}.0']
        if self._raw_feature:
            return bb_up.values
        price2bb_up = (dataframe['close'] - bb_up) / bb_up
        return price2bb_up.values


class BollingerMidBand(MarketFeature):
    """
    Calculates the price position in relative to Bollinger Upper Band.

    Parameters
    ----------

    feature_name: `str`
        The name of the feature. Default: 'BollingerMidBand'

    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'

    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn’s StandardScaler()

    length: `int`
        The short period. Default: 20

    std: `int`
        The long period. Default: 2

    mamode: `str`
        Two options: "sma" or "ema". Default: "sma"

    ddof: `int`
        Degrees of Freedom to use. Default: 0

    offset: `int`
        How many periods to offset the result. Default: 0

    raw_feature: `bool`
        If to output raw feature without scaling & normalization.
        Default: False.
    """

    def __init__(self, feature_name='BollingerMidBand',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler(),
                 raw_feature=True,
                 length=20,
                 std=2,
                 mamode="sma",
                 ddof=0,
                 offset=0):
        super(BollingerMidBand, self).__init__(feature_name, feature_type, scaler)
        self._raw_feature = raw_feature
        self._length = length
        self._std = std
        self._mamode = mamode
        self._ddof = ddof
        self._offset = offset

    def preprocess(self, dataframe):
        bb = dataframe.ta.bbands(length=self._length,
                                 std=self._std,
                                 mamode=self._mamode,
                                 ddof=self._ddof,
                                 offset=self._offset)
        bb_mid = bb[f'BBM_{self._length}_{self._std}.0']
        if self._raw_feature:
            return bb_mid.values
        price2bb_mid = (dataframe['close'] - bb_mid) / bb_mid
        return price2bb_mid.values


class BollingerBandPercent(MarketFeature):
    """
    Calculates Bollinger Band percent bandwidth (%b) metric.

    Parameters
    ----------

    feature_name: `str`
        The name of the feature. Default: 'BollingerBandPercent'

    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'

    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn’s StandardScaler()

    length: `int`
        The short period. Default: 20

    std: `int`
        The long period. Default: 2

    mamode: `str`
        Two options: "sma" or "ema". Default: "sma"

    ddof: `int`
        Degrees of Freedom to use. Default: 0

    offset: `int`
        How many periods to offset the result. Default: 0

    raw_feature: `bool`
        If to output raw feature without scaling & normalization.
        Default: False.
    """

    def __init__(self, feature_name='BollingerBandPercent',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler(),
                 raw_feature=True,
                 length=20,
                 std=2,
                 mamode="sma",
                 ddof=0,
                 offset=0):
        super(BollingerBandPercent, self).__init__(feature_name, feature_type, scaler)
        self._raw_feature = raw_feature
        self._length = length
        self._std = std
        self._mamode = mamode
        self._ddof = ddof
        self._offset = offset

    def preprocess(self, dataframe):
        bb = dataframe.ta.bbands(length=self._length,
                                 std=self._std,
                                 mamode=self._mamode,
                                 ddof=self._ddof,
                                 offset=self._offset)
        bb_percent = bb[f'BBP_{self._length}_{self._std}.0']
        if self._raw_feature:
            return bb_percent.values
        price2bb_percent = dataframe['close'] * bb_percent
        return price2bb_percent.values


class BollingerBandBandwidth(MarketFeature):
    """
    Calculates Bollinger Band bandwidth (bandwidth delta) metric.

    Parameters
    ----------

    feature_name: `str`
        The name of the feature. Default: 'BollingerBandBandwidth'

    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'

    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn’s StandardScaler()

    length: `int`
        The short period. Default: 20

    std: `int`
        The long period. Default: 2

    mamode: `str`
        Two options: "sma" or "ema". Default: "sma"

    ddof: `int`
        Degrees of Freedom to use. Default: 0

    offset: `int`
        How many periods to offset the result. Default: 0
    """

    def __init__(self, feature_name='BollingerBandBandwidth',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler(),
                 length=20,
                 std=2,
                 mamode="sma",
                 ddof=0,
                 offset=0):
        super(BollingerBandBandwidth, self).__init__(feature_name, feature_type, scaler)
        self._length = length
        self._std = std
        self._mamode = mamode
        self._ddof = ddof
        self._offset = offset

    def preprocess(self, dataframe):
        bb = dataframe.ta.bbands(length=self._length,
                                 std=self._std,
                                 mamode=self._mamode,
                                 ddof=self._ddof,
                                 offset=self._offset)
        bb_bandwidth = bb[f'BBB_{self._length}_{self._std}.0']
        return bb_bandwidth.values


class PriceToEMA(MarketFeature):
    """
    Calculates the price relative position to EMA.

    Parameters
    ----------
    feature_name: `str`
        The name of the feature. Default: 'PriceToEMA'

    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'

    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn’s StandardScaler()

    length: `int`
        The period for EMA. Default: 20.

    offset: `int`
        The offset for EMA. Default: 0.

    raw_feature: `bool`
        If to output raw feature without scaling & normalization.
        Default: False.
    """

    def __init__(self, feature_name='PriceToEMA',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler(),
                 raw_feature=True,
                 length=20,
                 offset=0):
        super(PriceToEMA, self).__init__(feature_name, feature_type, scaler)
        self._raw_feature = raw_feature
        self._length = length
        self._offset = offset
        self._pre_multiplier = 1000
        self._norm_power = 5e-1
        self._norm_multiplier = 1000

    def preprocess(self, dataframe):
        ema = dataframe.ta.ema(length=self._length,
                               offset=self._offset)
        price_to_ema = (dataframe['close'] - ema) / ema
        if self._raw_feature:
            return price_to_ema.values
        price_to_ema *= self._pre_multiplier
        transformed_data = ((pow(price_to_ema.abs() + 1, self._norm_power) - 1)
                            * np.sign(price_to_ema)
                            * self._norm_multiplier)
        return transformed_data.values

    def reverse_transform(self, data):
        assert isinstance(data, np.ndarray), "Input is not numpy array"
        if self._raw_feature:
            return data
        reversed_data = (((pow((np.abs(data) / self._norm_multiplier + 1),
                               1 / self._norm_power) - 1) * np.sign(data))
                         / self._pre_multiplier)
        return reversed_data


class EMASlope(MarketFeature):
    """
    Calculates the slope of EMA for retreiving trend information.

    Parameters
      ----------
    feature_name: `str`
        The name of the feature. Default: 'EMASlope'

    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'

    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn’s StandardScaler()

    periods: `int`
        The period of calculating the EMA Slope.

    length: `int`
        The period for EMA. Default: 20.

    offset: `int`
        The offset for EMA. Default: 0.

    raw_feature: `bool`
        If to output raw feature without scaling & normalization.
        Default: False.
    """

    def __init__(self, feature_name='EMASlope',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler(),
                 raw_feature=True,
                 periods=5,
                 length=20,
                 offset=0):
        super(EMASlope, self).__init__(feature_name, feature_type, scaler)
        self._raw_feature = raw_feature
        self._periods = periods
        self._length = length
        self._offset = offset
        self._pre_multiplier = 1000
        self._norm_power = 6e-1
        self._norm_multiplier = 1000

    def preprocess(self, dataframe):
        ema = dataframe.ta.ema(length=self._length,
                               offset=self._offset)
        ema_slope = ((ema - ema.shift(periods=self._periods))
                     / self._periods)
        if self._raw_feature:
            return ema_slope.values
        ema_slope *= self._pre_multiplier
        transformed_data = ((pow(ema_slope.abs() + 1, self._norm_power) - 1)
                            * np.sign(ema_slope)
                            * self._norm_multiplier)
        return transformed_data.values


class EMARelationship(MarketFeature):
    """
    Calculates the difference of two EMAs for retreiving trend information.

    Parameters
    ----------
    feature_name: `str`
        The name of the feature. Default: 'EMASlope'

    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'

    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn’s StandardScaler()

    fast_length: `int`
        The length for fast EMA.

    slow_length: `int`
        The length for slow EMA.

    raw_feature: `bool`
        If to output raw feature without scaling & normalization.
        Default: False.
    """

    def __init__(self, feature_name='EMARelationship',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler(),
                 raw_feature=True,
                 fast_length=20,
                 slow_length=50, ):
        super(EMARelationship, self).__init__(feature_name, feature_type, scaler)
        self._raw_feature = raw_feature
        self._fast_length = fast_length
        self._slow_length = slow_length
        self._pre_multiplier = 1000
        self._norm_power = 5e-1
        self._norm_multiplier = 1000

    def preprocess(self, dataframe):
        fast_ema = dataframe.ta.ema(length=self._fast_length)
        slow_ema = dataframe.ta.ema(length=self._slow_length)
        ema_diff = (fast_ema - slow_ema) / slow_ema
        if self._raw_feature:
            return ema_diff.values
        ema_diff *= self._pre_multiplier
        transformed_data = ((pow(ema_diff.abs() + 1, self._norm_power) - 1)
                            * np.sign(ema_diff)
                            * self._norm_multiplier)
        return transformed_data.values


class EMA(MarketFeature):
    """
    Calculates the Exponential Moving Average.

    Parameters
    ----------
    feature_name: `str`
        The name of the feature. Default: 'EMA'

    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'

    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn’s StandardScaler()

    length: `int`
        The length for EMA.
    """

    def __init__(self, feature_name='EMA',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler(),
                 length=20, ):
        super(EMARelationship, self).__init__(feature_name, feature_type, scaler)
        self._length = length
        self._pre_multiplier = 1000
        self._norm_power = 5e-1
        self._norm_multiplier = 1000

    def preprocess(self, dataframe):
        ema = dataframe.ta.ema(length=self._length)
        return ema.values


class RVOL(MarketFeature):
    """
    Calculates the Realized Volatility of an asset.

    Parameters
    ----------
    feature_name: `str`
        The name of the feature. Default: 'RVOL'
    feature_type: `str`
        The type of the feature. Default: 'time_varying_unknown_reals'

    scaler: `sklearn object`
        The scikitlearn scaler object for
        feature scaling & normalization.
        Defaults effectively to sklearn’s StandardScaler()
    """

    def __init__(self, feature_name='RVOL',
                 feature_type='time_varying_unknown_reals',
                 scaler=StandardScaler(),
                 basic_period=1,
                 std_period=4,
                 scaling_period=24):
        super(RVOL, self).__init__(feature_name, feature_type, scaler)
        self._basic_period = basic_period
        self._std_period = std_period
        self._scaling_period = scaling_period

    def preprocess(self, dataframe):
        log_return = dataframe.ta.log_return(length=self._basic_period,
                                             cumulative=False)
        return_std = log_return.rolling(self._std_period).std()
        rvol = return_std * self._scaling_period
        return rvol.values
