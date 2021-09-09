import time, json, logging
from .client import KaiPublisherClient
from typing import Tuple
from binance.client import Client
from binance.exceptions import BinanceAPIException
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager

class BaseTickerPublisher():
    def __init__(self, 
                 name: str,
                 websocket: any,
                 stream_id: any,
                 publisher: KaiPublisherClient,
                 topic_path: str,
                 log_every: int = 1000):
        """ Base ticker publisher subclass. """
        self.name = name
        self.websocket = websocket
        self.stream_id = stream_id
        self.publisher = publisher
        self.topic_path = topic_path
        self.log_every = log_every
    
    def format_binance_ticker_to_dict(self, data) -> dict:
        """ Will format binance ticker websocket data to dictionary. 
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
            total_traded_quote_asset_volume=float(data["q"]))

    def format_binance_kline_to_dict(self, data) -> Tuple[dict, bool]:
        """ Will format binance kline websocket data to dictionary. 
            Parameters
            ----------
            data: `dict`
                Data containing kline information.
            Returns
            -------
            `Tuple[dict, bool]`
                A newly formatted dictionary kline, and boolean
                `True` if the kline is closed, else it's still open.
        """
        return dict(
            symbol=data["s"],
            timestamp=int(data["T"]),
            interval=data["i"],
            open=float(data["o"]),
            close=float(data["c"]),
            high=float(data["h"]),
            low=float(data["l"]),
            base_asset_volume=float(data["v"]) if data.get("v") else None,
            number_of_trades=int(data.get("n")) if data.get("n") else None,
            quote_asset_volume=float(data["q"]) if data.get("q") else None,
            taker_buy_base_vol=float(data["V"]) if data.get("V") else None,
            taker_buy_asset_vol=float(data["Q"]) if data.get("Q") else None,
        ), bool(data['x'])

class BinanceTickerPublisher(BaseTickerPublisher):
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
        super(BinanceTickerPublisher, self).__init__(
            name='BINANCE', websocket=websocket, stream_id=stream_id, 
            publisher=publisher, topic_path=topic_path)
    
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
                data = self.format_binance_ticker_to_dict(stream['data'])
                self.publisher.publish(
                    origin=self.__class__.__name__,
                    topic_path=self.topic_path,
                    data=data,
                    attributes=dict(symbol=data['symbol']))
            count += 1
            if count % self.log_every == 0:
                logging.info(self.websocket.print_summary(disable_print=True))
                count = 0

class BinanceKlinesPublisher(BaseTickerPublisher):
    def __init__(self, 
            client: Client,
            websocket: BinanceWebSocketApiManager,
            stream_id: str,
            publisher: KaiPublisherClient,
            topic_path: str = 'klines-binance-v0',
            n_klines: int = 250,
            markets: list = ['BTCUSDT']):
        """ Publish klines data to defined topic. As
            of right now the default kline is 15m.

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
            n_klines: `int`
                The number of klines to publish.
            markets: `list`
                A list containing the symbols.
        """
        super(BinanceTickerPublisher, self).__init__(
            name='BINANCE', websocket=websocket, stream_id=stream_id, 
            publisher=publisher, topic_path=topic_path)
        assert n_klines <= 1000
        self.client = client
        self.n_klines = n_klines
        self.markets = markets
        self.sleep = 0.05
        self.markets_klines = self.initialize_klines()
    
    def initialize_klines(self):
        """Initialize all historical n-klines for the specified markets."""
        markets_klines = {}
        for market in self.markets:
            start = time.time()
            try:
                # retrieve the last historical n-klines 
                klines = self.client.get_historical_klines(
                    market.upper(),
                    interval=self.client.KLINE_INTERVAL_15MINUTE,
                    limit=self.n_klines)
            except BinanceAPIException as e:
                if e.status_code == 400: continue
                else: logging.info(f"Exception caught retrieving historical klines: {e}")
            # ensure that klines requests are successful
            if klines is None or len(klines) == 0: continue
            try:
                markets_klines[market.upper()] = self.to_dataframe(klines)
            except Exception as e:
                logging.info(f"Exception caught converting to df: {e}")
            # if all successful calculate the
            # the overall execution time and delay if needed
            self.delay(start)
    
    def delay(self, start: time):
        """ Get the difference between now and
            starting time. If time is below
            expected buffer delay the thread.

            Parameters
            ----------
            start: `time`
                The starting time.
        """
        end = time.time() - start
        if end <= self.sleep: time.sleep(abs(self.sleep - end))

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
                data = self.format_binance_kline_to_dict(stream['k'])
                # if the kline retrieve is closed
                # TODO append new kline to market klines dataframe
                # and remove the first row to keep n_klines

                # if the kline retrieve is not closed
                # replace the last row of the market_klines dataframe

                self.publisher.publish(
                    origin=self.__class__.__name__,
                    topic_path=self.topic_path,
                    data=data,
                    attributes=dict(symbol=data['symbol']))
            count += 1
            if count % self.log_every == 0:
                logging.info(self.websocket.print_summary(disable_print=True))
                count = 0