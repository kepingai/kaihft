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

@click.group()
def cli():
    """CLI tool"""
    pass

@cli.command()
def ticker_binance_spot():
    services.ticker.binance_spot.main(__PUBLISHER)

if __name__ == "__main__":
    cli()