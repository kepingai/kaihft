import click, logging, os
import kaihft.services as services
from kaihft.alerts import notify_failure
from kaihft.databases.database import KaiRealtimeDatabase
from kaihft.publishers.client import KaiPublisherClient
from kaihft.subscribers.client import KaiSubscriberClient

# logging verbose mode
logging.basicConfig(level=logging.INFO,       
                    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",       
                    style="{")
# initialize publisher
__PUBLISHER = KaiPublisherClient()
__SUBSCRIBER = KaiSubscriberClient()
# initiate access to database
__DATABASE = KaiRealtimeDatabase()
__MARKETS = {'btcusdt','ethusdt', 'adausdt', 'dogeusdt', 'dotusdt', 'uniusdt'}

@click.group()
def cli():
    """CLI tool"""
    pass

@cli.command()
@click.option('--production', is_flag=True, help='publish messages to production topic.')
@notify_failure
def ticker_binance_spot(production):
    services.ticker_binance_spot.main(
        publisher=__PUBLISHER, 
        markets=__MARKETS,
        production=production)

@cli.command()
@click.option('--klines', default=250, help='the length of historical klines back.')
@click.option('--production', is_flag=True, help='publish messages to production topic.')
@notify_failure
def klines_binance_spot(klines, production):
    services.klines_binance_spot.main(
        publisher=__PUBLISHER,
        n_klines=klines,
        markets=__MARKETS,
        production=production)

@cli.command()
@click.option('--strategy', default="STS", help="available strategies: 'STS'")
@click.option('--version', default='v0', help="the version of signal engine.")
@click.option('--production', is_flag=True, help='publish & subscribe messages to production topic.')
@notify_failure
def signal_binance_spot(strategy, version, production):
    services.signal_engine.main(
        exchange='binance',
        database=__DATABASE,
        publisher=__PUBLISHER,
        subscriber=__SUBSCRIBER,
        strategy=strategy,
        production=production,
        version=version)

if __name__ == "__main__":
    cli()