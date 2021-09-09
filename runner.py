from functools import wraps
import click, services, logging, os
from publishers.client import KaiPublisherClient

# logging verbose mode
logging.basicConfig(level=logging.INFO,       
                    filename='logs/' + os.path.basename(__file__) + '.log',       
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
            logging.info(error)
            # TODO: send exception error to slack
            raise error
    return wrapper

@click.group()
def cli():
    """CLI tool"""
    pass

@cli.command()
@click.option('--topic', default='ticker-binance-v0', help='topic path name in cloud pub/sub.')
@notify_failure
def ticker_binance_spot(topic):
    services.ticker_binance_spot.main(
        publisher=__PUBLISHER, 
        markets=__MARKETS,
        topic_path=topic)

@cli.command()
@click.option('--klines', default=250, help='the length of historical klines back.')
@click.option('--topic', default='klines-binance-v0', help='topic path name in cloud pub/sub.')
@notify_failure
def klines_binance_spot(klines, topic):
    services.klines_binance_spot.main(
        publisher=__PUBLISHER,
        n_klines=klines,
        markets=__MARKETS,
        topic_path=topic)

if __name__ == "__main__":
    cli()