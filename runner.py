from functools import wraps
from logging import handlers
import click, services, logging, os
from logging.handlers import RotatingFileHandler
from publishers.client import KaiPublisherClient

# logging verbose mode
log_filename = 'logs/' + os.path.basename(__file__) + '.log'
logging.basicConfig(level=logging.INFO,       
                    filename=log_filename,       
                    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",       
                    style="{")
# set environment credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials.json'
# initialize publisher
__PUBLISHER = KaiPublisherClient()
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
@click.option('--production', is_flag=True, help='publish & subscribe messages to production topic.')
@notify_failure
def signal_binance_spot(strategy, production):
    services.signal_binance_spot.main(
        strategy=strategy,
        production=production)

if __name__ == "__main__":
    cli()