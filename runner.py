from functools import wraps
import click, logging, os
import kaihft.services as services
from kaihft.databases.database import KaiRealtimeDatabase
from kaihft.publishers.client import KaiPublisherClient
from kaihft.subscribers.client import KaiSubscriberClient

# logging verbose mode
log_filename = 'logs/' + os.path.basename(__file__) + '.log'
logging.basicConfig(level=logging.INFO,       
                    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",       
                    style="{")
# set environment credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials.json'
# initialize publisher
__PUBLISHER = KaiPublisherClient()
__SUBSCRIBER = KaiSubscriberClient()
# initiate access to database
__DATABASE = KaiRealtimeDatabase()
__MARKETS = {'btcusdt','ethusdt', 'adausdt', 'dogeusdt', 'dotusdt', 'uniusdt'}

def notify_failure(fn: callable):
    """ This decorator will output the traceback of a 
        raised exception to slack and the log.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        try: return fn(*args, **kwargs)
        except Exception as error:
            logging.error(error)
            # TODO: send exception error to slack
            raise error
    return wrapper

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