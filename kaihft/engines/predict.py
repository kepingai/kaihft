from typing import Union
import requests, json, logging
import google.auth.transport.requests

__CRED = 'credentials.json'

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

def predict(base: str, quote: str, data: dict) -> Union[dict, None]:
    """ Forecast the price spread in percentage and direction of
        a generic Coin n ticks to the future and its direction.

        Parameters
        ----------
        base: `str`
            The base symbol of the ticker.
        quote: `str`
            The quote symbol of the ticker.
        data: `dict`
            A dictionary of klines lists data.
            
        Acceptable keys in data dictionary
        [
            'open', 'high', 'low', 'close', 'volume', 'close_time',
            'quote_asset_volume','number_of_trades','taker_buy_asset_vol',
            'taker_buy_quote_vol','datetime','ticker','interval', 'timeframe
        ]
            
        ..code-block:: python
        {
            "instances": {
                "data": {
                    'close': [
                        2.924000024795532,
                        2.969899892807007,
                        2.9554998874664307,
                        ...
                    ],
                    ...
                    'high': [
                        2.9398000240325928,
                        2.970000028610229,
                        2.97189998626709,
                    ],
                    ...
                }
            }
        }

        Returns
        -------
        `dict`
            A dictionary containing forecasted percentage spread
            of n-tick forward and the direction.

        .. code-block:: python
        {
            'base': 'UNI',                                      # base symbol
            'interval': '15m',                                  # interval of symbol
            'predictions': {        
                'direction': 0,                                 # 1 = long and 0 = short
                'n_tick_forward': 8,                            # forecasted n forward
                'percentage_spread': 0.16193726658821106        # spread in percentage
            },
            'success': True,                                    # validator    
            'timestamp': 1631646484.79271                       # utc timestamp
        }
    """
    job_id = "0A"
    base_url = "https://us-central1-keping-ai-continuum.cloudfunctions.net/predict_15m"
    model_url = f"{base_url}_{base.lower()}_{quote.lower()}_{job_id}"
    # fetch id token first
    id_token = fetch_id_token(model_url)
    if id_token is None: return None
    # authorization in headers
    headers = {"Authorization": f'bearer {id_token}', "content-type" : 'application/json'}
    params = dict(instances=dict(data=data))
    result = requests.post(model_url, data=json.dumps(params), headers=headers)
    if result.status_code == 200 and result.content:
        return json.loads(result.content)
    else: 
        logging.warn(f"[predict] failed inferencing to layer 2, "
        f"status-code:{result.status_code}, symbol: {base}{quote}")
        return None