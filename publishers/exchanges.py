import time, json, logging
from .client import KaiPublisherClient
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager

class BinanceTickerPublisher():
    def __init__(self, 
            websocket: BinanceWebSocketApiManager,
            stream_id: str,
            publisher: KaiPublisherClient,
            topic_path: str = 'ticker-binance-v0'):
        """ Publish ticker data to defined topic. 

            Parameters
            ----------
            websocket: `BinanceWebSocketApiManager`
                The websocket to retrieve data.
            stream_id: `str`
                The websocket stream id.
            publisher: `PublisherClient`
                The Cloud Pub/Sub client.
            topic_path: `str`
                The topic path to publish data.
        """
        self.name = 'BINANCE'
        self.websocket = websocket
        self.stream_id = stream_id
        self.publisher = publisher
        self.topic_path = topic_path
        self.report_every = 1000
    
    def run(self):
        """ Load,format data from websocket manager & publish
            it to the topic specified during initialization.
        """
        count = 0
        while True:
            # binance spot will only allow 24h max stream
            # connection, this will automatically close the script
            if self.websocket.is_manager_stopping(): exit(0)
            # get and remove the oldest entry from the `stream_buffer` stack
            oldest_stream_data_from_stream_buffer = self.websocket.pop_stream_data_from_stream_buffer()
            # print the stream data from stream buffer
            if oldest_stream_data_from_stream_buffer is False: time.sleep(0.01)
            else:
                stream = json.loads(oldest_stream_data_from_stream_buffer)
                if 'data' not in stream: continue
                data = self.format_ticker_to_dict(stream['data'])
                self.publisher.publish(
                    origin=self.__class__.__name__,
                    topic_path=self.topic_path,
                    data=data,
                    attributes=dict(symbol=data['symbol']))
            count += 1
            if count % self.report_every == 0:
                logging.info(self.websocket.print_summary(disable_print=True))
                count = 0
                
    def format_ticker_to_dict(self, data) -> dict:
        """ Will format websocket format to dictionary. 
            Parameters
            ----------
            data: `dict`
                Data containing ticker information.
            Returns
            -------
            `dict`
                A newly formatted dictionary.
        """
        return dict(
            exchange=self.name,
            symbol=data["s"],
            timestamp=int(data["E"]),
            price_change=float(data["p"]),
            price_change_percent=float(data["P"]),
            first_trade_price=float(data.get("x")) if data.get("x") else None,
            last_price=float(data["c"]),
            last_quantity=float(data["Q"]),
            best_bid_price=float(data.get("b")) if data.get("b") else None,
            best_bid_quantity=float(data.get("B")) if data.get("B") else None,
            best_ask_price=float(data.get("a")) if data.get("a") else None,
            best_ask_quantity=float(data.get("A")) if data.get("A") else None,
            open_price=float(data["o"]),
            high_price=float(data["h"]),
            low_price=float(data["l"]),
            total_traded_base_asset_volume=float(data["v"]),
            total_traded_quote_asset_volume=float(data["q"]),
        )