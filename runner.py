from functools import wraps
import click, services, logging, os
from publishers.client import KaiPublisherClient

# logging verbose mode
logging.basicConfig(level=logging.INFO,       
                    filename='logs/' + os.path.basename(__file__) + '.log',       
                    format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",       
                    style="{")
# initialize publisher
__PUBLISHER = KaiPublisherClient()
__MARKETS = {'btcusdt','ethusdt', 'adausdt', 'dogeusdt', 'dotusdt', 'uniusdt'}
# set environment credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials.json'

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
@notify_failure
def ticker_binance_spot():
    services.ticker.binance_spot.main(__PUBLISHER, __MARKETS)

if __name__ == "__main__":
    cli()