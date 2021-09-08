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

def notify_failure(fn: callable):
    """ This decorator will output the traceback of a 
        raised exception to slack and the log.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        try: return fn(*args, **kwargs)
        except Exception as error:
            print(error)
            raise error
    return wrapper

@click.group()
def cli():
    """CLI tool"""
    pass

@cli.command()
@notify_failure
def ticker_binance_spot():
    services.ticker.binance_spot.main(__PUBLISHER)

if __name__ == "__main__":
    cli()