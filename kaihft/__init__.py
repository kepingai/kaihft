"""
    Welcome to KepingAI Signal LSTF Signal Engine (Layer 1).

    This library will run a specific technical analysis strategy on
    specified cryptocurrency markets. The purpose of the technical analysis
    is to scout for potential market volatility that can be triggered as signal
    opportunity. If the specific technical analysis strategy is met, signal engine
    will communicate to layer 2 (intelligence engine) to ensure that the specified
    signal opportunity matches our trained AI models.

    To run this library you will need:
    - `credential.json`: This credential file should be given to you from a supervisor.
    
    Example
    -------
    >>> python3 runner.py --help

    Running ticker binance service that will start websocket to binance exchange
    and publish the ticker data to cloud pub/sub.
    
    >>> GOOGLE_APPLICATION_CREDENTIALS="credential.json" python3 runner.py ticker-binance-spot
   
    Warning
    -------
    *Do not run with `--production` flag*. Only run this layer with the production
    flag from a docker image specified for running in a Kubernetes Cluster.
"""
from . import *