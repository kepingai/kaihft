from typing import Union, Optional
import requests, json, logging
import google.auth.transport.requests
import os
from typing import Tuple
import numpy as np


__CRED = 'credentials.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials.json'


def fetch_id_token(audience: str) -> Union[str, None]:
    """ Will return the id token for specific service url. 

        Parameters
        ----------
        audience: `str`
          The url to invoke.
        
        Returns
        -------
        `Union[str, None]`
          The ID Token, will return `None` if exception is caught.
    """
    request = google.auth.transport.requests.Request()
    try:
        with open(__CRED, "r") as f:
            info = json.load(f)
            credentials_content = (
                (info.get("type") == "service_account") and info or None)
            from google.oauth2 import service_account
            credentials = service_account.IDTokenCredentials.from_service_account_info(
                credentials_content, target_audience=audience)
    except ValueError as caught_exc:
        logging.error(f"[predict] exception caught generating id token "
            f"from cred: {__CRED}, audience: {audience}, "
            f"error: {caught_exc}")
        return None
    credentials.refresh(request)
    return credentials.token


def predict(endpoint: str, base: str, quote: str, data: dict) -> Union[dict, None]:
    """ Forecast the price spread in percentage and direction of
        a generic Coin n ticks to the future and its direction.

        Parameters
        ----------
        endpoint: `str`
            The endpoint of request to layer 2.
        base: `str`
            The base symbol of the ticker.
        quote: `str`
            The quote symbol of the ticker.
        data: `dict`
            A dictionary of klines lists data.
        
        Example
        -------
        Acceptable keys in `data` dictionary
        >>> [
        ...    'open', 'high', 'low', 'close', 'volume', 'close_time',
        ...    'quote_asset_volume','number_of_trades','taker_buy_asset_vol',
        ...    'taker_buy_quote_vol','datetime','ticker','interval', 'timeframe'
        ... ]
            
        `data` should be formatted as follows
        >>> {
        ...    "instances": {
        ...        "data": {
        ...            'close': [
        ...                2.924000024795532,
        ...                2.969899892807007,
        ...                2.9554998874664307,
        ...                ...
        ...            ],
        ...            ...
        ...            'high': [
        ...                2.9398000240325928,
        ...                2.970000028610229,
        ...                2.97189998626709,
        ...            ],
        ...            ...
        ...        }
        ...    }
        ... }

        Returns
        -------
        `dict`
            A dictionary containing forecasted percentage spread
            of n-tick forward and the direction.

        Example
        -------
        >>> {
        ...        'base': 'UNI',                               # base pair
        ...        'interval': '15m',                           # interval timeframe
        ...        'predictions': {                     
        ...            'direction': 0,                          # 1 = long and 0 = short
        ...            'n_tick_forward': 4,                     # forecasted n forward
        ...            'percentage_arr': [                      # series predictions
        ...                -0.29120445251464844,
        ...                -0.30862805247306824,
        ...                -0.3237372040748596,
        ...                -0.3499438464641571
        ...            ],
        ...            'percentage_spread': 0.3499438464641571  # spread in percentage
        ...       },
        ...        'quote': 'USDT',                             # quote pair
        ...        'success': True,                             # validator    
        ...        'timestamp': 1639512483.083822               # utc timestamp
        ...    }
    """
    job_id = "0A"
    base_url = f"https://us-central1-keping-ai-continuum.cloudfunctions.net/{endpoint}"
    model_url = f"{base_url}_{base.lower()}_{quote.lower()}_{job_id}"
    # fetch id token first
    id_token = fetch_id_token(model_url)
    if id_token is None: return None
    # authorization in headers
    headers = {"Authorization": f'bearer {id_token}', "content-type" : 'application/json'}
    params = dict(instances=dict(data=data))
    result = requests.post(model_url, data=json.dumps(params), headers=headers)
    # TODO - R: debug mode data
    data_report = {k: (np.array(v).shape, v[0], v[1]) for k, v in data.items()}
    if result.status_code == 200 and result.content:
        return json.loads(result.content)
    logging.warn(f"[predict] failed inferring to layer 2, status-code: "
                 f"{result.status_code}, symbol: {base}{quote}, "
                 f"content: {result.content}, data shapes: {data_report}")
    return None


def predict_cloud_run(base: str,
                      quote: str,
                      data: dict,
                      mode: str,
                      kaiforecast_version: str,
                      ha_trend: int,
                      timeframe: str = '1m'
                      ) -> Tuple[Optional[dict], Optional[dict]]:
    """ Function to perform prediction using regression and classification TFT models served on Google
        Cloud Run. The data is similar to the predict function.

        Parameters
        ----------
        base: `str`
            the pair's base
        quote: `str`
            the pair's quote
        data: `dict`
            the data as the input into the model
        mode: `str`
            prod or dev
        kaiforecast_version: `str`
            kaiforecast version used. Necessary for the cloud run
        ha_trend: `int`
            current heikin ashi trend, to determine long and short
        timeframe: `str`
            timeframe used for the model

        Returns
        -------
        `Tuple[Optional[dict], Optional[dict]]`
            A dictionary containing forecasted percentage spread
            of n-tick forward and the direction.

        Example
        -------
        Regression output
        >>> {
        ...        'base': 'UNI',                               # base pair
        ...        'interval': '15m',                           # interval timeframe
        ...        'predictions': {
        ...            'direction': 0,                          # 1 = long and 0 = short
        ...            'n_tick_forward': 4,                     # forecasted n forward
        ...            'percentage_arr': [                      # series predictions
        ...                -0.29120445251464844,
        ...                -0.30862805247306824,
        ...                -0.3237372040748596,
        ...                -0.3499438464641571
        ...            ],
        ...            'percentage_spread': 0.3499438464641571  # spread in percentage
        ...       },
        ...        'quote': 'USDT',                             # quote pair
        ...        'success': True,                             # validator
        ...        'timestamp': 1639512483.083822               # utc timestamp
        ...    }

        Classificationn output
        >>> {
        ...        'base': 'UNI',                               # base pair
        ...        'interval': '15m',                           # interval timeframe
        ...        'predictions': [0.6, 0.4],                   # prediction probability
        ...        'quote': 'USDT',                             # quote pair
        ...        'success': True,                             # validator
        ...        'timestamp': 1639512483.083822               # utc timestamp
        ...    }

    """
    _kaiforecast_version = kaiforecast_version.replace(".", "-")
    if ha_trend == 1:
        direction = 'long'
    elif ha_trend == -1:
        direction = 'short'
    else:
        direction = None

    if direction is not None:
        cls_endpoint = f"https://{mode}-{_kaiforecast_version}---"\
                       f"{base.lower()}-predict-classification-{direction}-{timeframe}-wvgsvdm4ya-uc.a.run.app"
        reg_endpoint = f"https://{mode}-{_kaiforecast_version}---"\
                       f"{base.lower()}-predict-regression-{timeframe}-wvgsvdm4ya-uc.a.run.app"

        # inference regression
        # id_token = fetch_id_token(audience=reg_endpoint)
        id_token = os.popen('gcloud auth print-identity-token').read().strip()
        headers = {"Authorization": f'bearer {id_token}', "content-type": 'application/json'}
        params = dict(instances=dict(data=data))
        reg_result = requests.post(reg_endpoint, data=json.dumps(params), headers=headers)

        if reg_result.status_code == 200 and reg_result.content:
            reg_result = reg_result.json()
        else:
            logging.warning(f"[predict] failed inferencing to layer 2 regression model, "
                            f"status-code:{reg_result.status_code}, symbol: {base}{quote}")
            return None, None

        # inference classification
        # id_token = fetch_id_token(audience=cls_endpoint)
        id_token = os.popen('gcloud auth print-identity-token').read().strip()
        headers = {"Authorization": f'bearer {id_token}', "content-type": 'application/json'}
        params = dict(instances=dict(data=data))
        cls_result = requests.post(cls_endpoint, data=json.dumps(params), headers=headers)

        if cls_result.status_code == 200 and cls_result.content:
            cls_result = cls_result.json()
        else:
            logging.warning(f"[predict] failed inferencing to layer 2 classification model, "
                            f"status-code:{cls_result.status_code}, symbol: {base}{quote}")
            return None, None

        return reg_result, cls_result

    else:
        return None, None


def predict_cloud_run_regression(base: str,
                                 quote: str,
                                 data: dict,
                                 mode: str,
                                 kaiforecast_version: str,
                                 ha_trend: int,
                                 timeframe: str = '1m'
                                 ) -> Optional[dict]:
    """ Function to perform prediction using regression TFT models,
        served on Google Cloud Run.
        The data is similar to the predict function.

        Parameters
        ----------
        base: `str`
            the pair's base
        quote: `str`
            the pair's quote
        data: `dict`
            the data as the input into the model
        mode: `str`
            prod or dev
        kaiforecast_version: `str`
            kaiforecast version used. Necessary for the cloud run
        ha_trend: `int`
            current heikin ashi trend, to determine long and short
        timeframe: `str`
            timeframe used for the model

        Returns
        -------
        `Optional[dict]`
            A dictionary containing forecasted percentage spread
            of n-tick forward and the direction.

        Example
        -------
        Regression output
        >>> {
        ...        'base': 'UNI',                               # base pair
        ...        'interval': '15m',                           # interval timeframe
        ...        'predictions': {
        ...            'direction': 0,                          # 1 = long and 0 = short
        ...            'n_tick_forward': 4,                     # forecasted n forward
        ...            'percentage_arr': [                      # series predictions
        ...                -0.29120445251464844,
        ...                -0.30862805247306824,
        ...                -0.3237372040748596,
        ...                -0.3499438464641571
        ...            ],
        ...            'percentage_spread': 0.3499438464641571  # spread in percentage
        ...       },
        ...        'quote': 'USDT',                             # quote pair
        ...        'success': True,                             # validator
        ...        'timestamp': 1639512483.083822               # utc timestamp
        ...    }
    """
    _kaiforecast_version = kaiforecast_version.replace(".", "-")
    if ha_trend == 1:
        direction = 'long'
    elif ha_trend == -1:
        direction = 'short'
    else:
        direction = None

    if direction is not None:
        reg_endpoint = f"https://{mode}-{_kaiforecast_version}---" \
                       f"{base.lower()}-predict-regression-{timeframe}-" \
                       f"wvgsvdm4ya-uc.a.run.app"

        # inference regression
        # id_token = fetch_id_token(audience=reg_endpoint)
        id_token = os.popen('gcloud auth print-identity-token').read().strip()
        headers = {"Authorization": f'bearer {id_token}', "content-type": 'application/json'}
        params = dict(instances=dict(data=data))
        reg_result = requests.post(reg_endpoint, data=json.dumps(params), headers=headers)

        if reg_result.status_code == 200 and reg_result.content:
            reg_result = reg_result.json()
        else:
            logging.warning(f"[predict] failed inferencing to layer 2 regression model, "
                            f"status-code:{reg_result.status_code}, symbol: {base}{quote}")
            return None

        return reg_result
    else:
        return None
