import logging
from publishers.exchanges import BinanceTickerPublisher
from publishers.client import KaiPublisherClient
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager

def main(
    publisher: KaiPublisherClient, 
    markets: dict, 
    production: bool):
    """ Retrieve real-time binance data via websocket &
        then publish binance tickers to Cloud Pub/Sub. 

        Parameters
        ----------
        publisher: `KaiPublisherClient`
            Is a Cloud Pub/Sub Client.
        markets: `dict`
            A dictionary containing the symbols to 
            retrieve data from websocket.
        production: `bool`
            if `True` publisher will publish to production topic.
    """
    if production:
        topic_path = 'ticker-binance-v0'
        logging.warn(f"[production-mode] tickers-BINANCE-SPOT, markets: {markets}, topic: {topic_path}")
    else: topic_path = 'dev-ticker-binance-v0'
    # binance only allows 1024 subscriptions in one stream
    # channels and markets and initiate multiplex stream
    # channels x markets = (total subscription)
    channels = {'ticker'}
    # connect to binance.com and create the stream
    # the stream id is returned after calling `create_stream()`
    binance_websocket_api_manager = BinanceWebSocketApiManager(
        exchange="binance.com",
        throw_exception_if_unrepairable=True)
    stream_id = binance_websocket_api_manager.create_stream(
        channels=channels, 
        markets=markets)
    # initialize binance ticker publisher
    # and run the publisher.
    ticker_publisher = BinanceTickerPublisher(
        websocket=binance_websocket_api_manager,
        stream_id=stream_id,
        publisher=publisher,
        topic_path=topic_path)
    ticker_publisher.run()