from publishers.exchanges import BinanceTickerPublisher
from publishers.client import KaiPublisherClient
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager

# binance only allows 1024 subscriptions in one stream
# channels and markets and initiate multiplex stream
# channels x markets = (total subscription)
channels = {'ticker'}
markets = {'ethusdt'}

def main(publisher: KaiPublisherClient):
    """ Retrieve real-time binance data via websocket &
        then publish binance tickers to Cloud Pub/Sub. 
    """
    # connect to binance.com and create the stream
    # the stream id is returned after calling `create_stream()`
    binance_websocket_api_manager = BinanceWebSocketApiManager(exchange="binance.com")
    binance_websocket_api_manager.create_stream(
        channels=channels, 
        markets=markets, 
        output="UnicornFly")
    # initialize binance ticker publisher
    # and run the publisher.
    ticker_publisher = BinanceTickerPublisher(
        websocket=binance_websocket_api_manager,
        publisher=publisher)
    ticker_publisher.run()