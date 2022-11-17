from abc import abstractmethod


class MarketFeature():
    def __init__(self, feature_name, feature_type, scaler=None):
      # make sure that the feature name is a string
      assert isinstance(feature_name, str), "feature name is not a string"

      feature_types = {'time_varying_unknown_categoricals',
                       'time_varying_unknown_reals',
                       'time_varying_known_reals',
                       'time_varying_known_categoricals',
                       'static_categoricals',
                       'static_reals'}      
      assert feature_type in feature_types, 'given feature type is not available'

      self._feature_name = feature_name
      self._feature_type = feature_type
      self._scaler = scaler

    @abstractmethod
    def preprocess(self, dataframe):
        """ This method will do preprocessing operation
            on the input dataframe.
            Raises
            ------
            `NotImplementedError`
                Will raise this exception if inherit class 
                did not implement this method.
        """
        raise NotImplementedError()

    @property
    def feature_name(self) -> str:
        """ The column name for the market feature.
            Returns
            -------
            `str`
                the feature name.
        """
        return self._feature_name

    @property
    def feature_type(self) -> str:
        """ The pytorch forecasting feature type.
            Returns
            -------
            `str`
                the feature type.
        """
        return self._feature_type

    @property
    def scaler(self):
        """ The scaler object of this market feature.
            Returns
            -------
            `scaler object, could be sklearn or pytorch object`
        """
        return self._scaler

    @scaler.setter
    def scaler(self, scaler_object):
        """ Specify the market feature scaler  
        """
        self._scaler = scaler_object