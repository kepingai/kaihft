import click
import logging
import sys
import json
import kaihft.services as services
from kaihft.alerts import notify_failure
from google.cloud import pubsub_v1
from google.api_core import bidi
import yaml
import os


# logging verbose mode
logging.basicConfig(
    level=logging.INFO,       
    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",       
    style="{", handlers=[logging.StreamHandler(sys.stdout)])
# only print pubsub_v1 and bidi logging with at least WARNING level
logging.getLogger(pubsub_v1.__name__).setLevel(logging.WARNING)
logging.getLogger(bidi.__name__).setLevel(logging.WARNING)

__MARKETS = {
    '1inchusdt','aaveusdt', 'adausdt', 'algousdt', 'atomusdt', 
    'audiousdt', 'avaxusdt', 'batusdt', 'bnbusdt', 'btcusdt', 
    'compusdt', 'crvusdt', 'dogeusdt', 'dotusdt', 'egldusdt', 
    'ethusdt', 'ftmusdt', 'grtusdt', 'hbarusdt', 'icxusdt', 
    'iotausdt', 'ksmusdt', 'linkusdt', 'ltcusdt', 'maticusdt', 
    'mkrusdt', 'nearusdt', 'neousdt', 'oneusdt', 'runeusdt', 
    'solusdt', 'stxusdt','sxpusdt', 'thetausdt', 'tkousdt', 
    'uniusdt', 'vetusdt', 'xlmusdt', 'xmrusdt', 'xrpusdt', 
    'xtzusdt', 'wavesusdt', 'zecusdt', 'zilusdt','zrxusdt'
}

@click.group()
def cli():
    """CLI tool"""
    pass

@cli.command()
@click.option('--production', is_flag=True, help='publish messages to production topic.')
@notify_failure
def ticker_binance_spot(production):
    services.ticker_binance_spot.main(
        production=production)

@cli.command()
@click.option('--production', is_flag=True, help='publish messages to production topic.')
@click.option('--exp0a', is_flag=True, help='publish & subscribe messages to exp0a topic.')
@click.option('--exp1a', is_flag=True, help='publish & subscribe messages to exp1a topic.')
@click.option('--restart-every', '-r', default=60, help='restart the pod every X minute(s)')
@notify_failure
def ticker_binance_futures(production, exp0a, exp1a, restart_every):
    services.ticker_binance_futures.main(
        markets=__MARKETS,
        production=production,
        exp0a=exp0a,
        exp1a=exp1a,
        restart_every=restart_every)

@cli.command()
@click.option('--klines', default=250, help='the length of historical klines back.')
@click.option('--production', is_flag=True, help='publish messages to production topic.')
@click.option('--exp0a', is_flag=True, help='publish & subscribe messages to exp0a topic.')
@click.option('--exp1a', is_flag=True, help='publish & subscribe messages to exp1a topic.')
@click.option('--timeframe', default=15, help='market timeframe to stream')
# @notify_failure
def klines_binance_spot(klines, production, exp0a, exp1a, timeframe):
    services.klines_binance_spot.main(
        n_klines=klines,
        production=production,
        timeframe=timeframe, 
        exp0a=exp0a, 
        exp1a=exp1a)

@cli.command()
@click.option('--klines', default=250, help='the length of historical klines back.')
@click.option('--production', is_flag=True, help='publish messages to production topic.')
@click.option('--exp0a', is_flag=True, help='publish & subscribe messages to exp0a topic.')
@click.option('--exp1a', is_flag=True, help='publish & subscribe messages to exp1a topic.')
@notify_failure
def klines_binance_futures(klines, production, exp0a, exp1a):
    services.klines_binance_futures.main(
        n_klines=klines,
        markets=__MARKETS,
        production=production,
        exp0a=exp0a,
        exp1a=exp1a)

@cli.command()
@click.option('--klines', default=250, help='the length of historical klines back.')
@click.option('--production', is_flag=True, help='publish messages to production topic.')
@click.option('--timeframe', default=15, help='market timeframe to stream')
@click.option('--restart-every', '-r', default=60, help='restart the pod every X minute(s)')
@notify_failure
def klines_binance_usdm(klines, production, timeframe, restart_every):
    services.klines_binance_usdm.main(
        n_klines=klines,
        production=production,
        timeframe=timeframe,
        restart_every=restart_every)


@cli.command()
@click.option('--production', is_flag=True, help='publish messages to production topic.')
@notify_failure
def ticker_binance_usdm(production):
    services.ticker_binance_usdm.main(
        production=production)


@cli.command()
@click.option('--strategy', default=None, help="Available strategies: [SUPERTREND_SQUEEZE, MAX_DRAWDOWN_SQUEEZE, MAX_DRAWDOWN_SPREAD, MAX_DRAWDOWN_SUPER_TREND_SPREAD]")
@click.option('--version', default='v0', help="The version of signal engine.")
@click.option('--log-every', default=1000, help="Log cloud pub/sub messages every.")
@click.option('--log-metrics-every', default=100, help="Log layer2 metrics every.")
@click.option('--production', is_flag=True, help='Publish & subscribe messages to production topic.')
@click.option('--exp0a', is_flag=True, help='Publish & subscribe messages to exp0a topic.')
@click.option('--exp1a', is_flag=True, help='Publish & subscribe messages to exp1a topic.')
@click.option('--strategy-params-path', default='', help='The path to json file which contains the params for the strategy')
# @notify_failure
def signal_binance_futures(strategy, version, log_every, log_metrics_every,
                           production, strategy_params_path, exp0a, exp1a):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"
    if strategy_params_path == '':
        strategy_params = {}
    elif strategy_params_path.endswith(".yaml"):
        with open(strategy_params_path, 'r') as file:
            strategy_params = yaml.safe_load(file)["STRATEGY_PARAMS"]
    elif strategy_params_path.endswith(".json"):
        with open(strategy_params_path, 'r') as fp:
            strategy_params = json.load(fp)

    services.signal_engine.main(
        exchange="binance",
        strategy=strategy,
        production=production,
        exp0a=exp0a,
        exp1a=exp1a,
        version=version,
        log_every=log_every,
        log_metrics_every=log_metrics_every,
        strategy_params=strategy_params)


if __name__ == "__main__":
    cli()