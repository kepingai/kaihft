"""
    This module defines the services of KepingAI LSTF layer 1. Run each service
    with a separate virtual environment / machine dedicated for each usage.
"""
from .ticker_binance_spot import *
from .ticker_binance_futures import *
from .ticker_binance_usdm import *
from .klines_binance_spot import *
from .klines_binance_usdm import *
from .klines_binance_futures import *
from .signal_engine import *
