import click, logging, sys
import kaihft.services as services
from kaihft.alerts import notify_failure

# logging verbose mode
logging.basicConfig(
    level=logging.INFO,       
    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",       
    style="{", handlers=[logging.StreamHandler(sys.stdout)])

__MARKETS = {
    '1inchusdt','aaveusdt', 'adausdt', 'algousdt', 'atomusdt', 
    'audiousdt', 'avaxusdt', 'batusdt', 'bnbusdt', 'btcusdt', 
    'compusdt', 'crvusdt', 'dogeusdt', 'dotusdt', 'egldusdt', 
    'enjusdt', 'ethusdt', 'ftmusdt', 'iotausdt' 'ksmusdt', 
    'linkusdt', 'ltcusdt', 'lunausdt', 'maticusdt', 'mkrusdt', 
    'neousdt', 'oneusdt', 'solusdt', 'sxpusdt', 'thetausdt', 
    'tkousdt', 'uniusdt', 'vetusdt', 'xlmusdt', 'xmrusdt', 
    'xtzusdt', 'wavesusdt', 'zecusdt', 'zilusdt'
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
        markets=__MARKETS,
        production=production)

@cli.command()
@click.option('--klines', default=250, help='the length of historical klines back.')
@click.option('--production', is_flag=True, help='publish messages to production topic.')
@notify_failure
def klines_binance_spot(klines, production):
    services.klines_binance_spot.main(
        n_klines=klines,
        markets=__MARKETS,
        production=production)

@cli.command()
@click.option('--strategy', default="STS", help="available strategies: 'STS'")
@click.option('--version', default='v0', help="the version of signal engine.")
@click.option('--log-every', default=1000, help="log cloud pub/sub messages every.")
@click.option('--log-metrics-every', default=100, help="log layer2 metrics every.")
@click.option('--production', is_flag=True, help='publish & subscribe messages to production topic.')
@notify_failure
def signal_binance_spot(strategy, version, log_every, log_metrics_every, production):
    services.signal_engine.main(
        exchange='binance',
        strategy=strategy,
        production=production,
        version=version,
        log_every=log_every,
        log_metrics_every=log_metrics_every)

if __name__ == "__main__":
    cli()