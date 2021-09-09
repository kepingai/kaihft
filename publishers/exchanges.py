import pandas as pd
import time, json, logging
from .client import KaiPublisherClient
from enum import Enum
from typing import Tuple
from pytz import timezone
from datetime import datetime, timedelta
from binance.client import Client
from binance.exceptions import BinanceAPIException
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager

class KlineStatus(Enum):
    """ An enum object representing kline status. """
    OPEN = 'OPEN'
    CLOSED = 'CLOSED'
    CLOSING = 'CLOSING'
    def __str__(self):
        return str(self.value)

class BaseTickerPublisher():
    def __init__(self, 
                 name: str,
                 websocket: any,
                 stream_id: any,
                 publisher: KaiPublisherClient,
                 topic_path: str,
                 log_every: int = 100000):
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
            symbol=data["s"].upper(),
            close_time=int(data["T"]),
            timeframe=data["i"],
            open=float(data["o"]),
            close=float(data["c"]),
            high=float(data["h"]),
            low=float(data["l"]),
            volume=float(data["v"]),
            number_of_trades=int(data.get("n")) if data.get("n") else None,
            quote_asset_volume=float(data["q"]) if data.get("q") else None,
            taker_buy_base_vol=float(data["V"]) if data.get("V") else None,
            taker_buy_quote_vol=float(data["Q"]) if data.get("Q") else None,
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
    # initialize globals that will be used
    # throughout the exchanges class here.
    __FLOATINGS = [
            "open", "high", "close", "low", 
            "volume", "quote_asset_volume",
            "taker_buy_base_vol", "taker_buy_quote_vol"]
    __COLUMNS = [
        "timestamp", "open", "high", "low", "close", 
        "volume", "close_time","quote_asset_volume", "number_of_trades", 
        "taker_buy_base_vol", "taker_buy_quote_vol",  "ignore"] 
    __DROP = ["timestamp", "ignore"]
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
        super().__init__(
            name='BINANCE', websocket=websocket, stream_id=stream_id, 
            publisher=publisher, topic_path=topic_path)
        assert n_klines <= 1000
        self.client = client
        self.n_klines = n_klines
        self.markets = markets
        self.sleep = 0.05
        self.markets_klines, self.kline_status = self.initialize_klines()
    
    def initialize_klines(self):
        """Initialize all historical n-klines for the specified markets."""
        # initialize market klines, kline status and
        # specified interval for the hitorical klines
        markets_klines = {}
        kline_status = {}
        _interval = 15
        interval = self.client.KLINE_INTERVAL_15MINUTE
        start_timestamp = (datetime.utcnow() - 
            timedelta(minutes=_interval * self.n_klines)).timestamp()
        for market in self.markets:
            start = time.time()
            try:
                # retrieve the last historical n-klines 
                klines = self.client.get_historical_klines(
                    market.upper(),
                    start_str=str(start_timestamp),
                    interval=interval)
            except BinanceAPIException as e:
                if e.status_code == 400: continue
                else: logging.info(f"Exception caught retrieving historical klines: {e}")
            # ensure that klines requests are successful
            if klines is None or len(klines) == 0: continue
            symbol = market.upper()
            markets_klines[symbol] = self.to_dataframe(
                symbol=symbol, interval=str(interval), klines=klines)
            # check if the market kline have closed
            kline_status[symbol] = (KlineStatus.CLOSED if 
                datetime.utcnow().minute % _interval == 0 else KlineStatus.OPEN)
            # if all successful calculate the
            # the overall execution time and delay if needed
            self.delay(start)
        return markets_klines, kline_status

    def to_dataframe(self, symbol: str, interval: str, klines: list) -> pd.DataFrame:
        """Will convert klines from binance to dataframe.

            Parameters
            ----------
            symbol: `str`
                The symbol of instrument.
            interval: `str`
                The interval of the symbol.
            klines: `list`
                A list of list containing binance formatted klines.
            
            Returns
            -------
            `pd.DataFrame`
                The dataframe formatted klines.
        """
        dataframe = pd.DataFrame(klines, columns=self.__COLUMNS)
        dataframe['symbol'] = symbol.upper()
        dataframe['timeframe'] = interval
        dataframe[self.__FLOATINGS] = dataframe[self.__FLOATINGS].astype('float32')
        dataframe.drop(columns=self.__DROP, inplace=True)
        return dataframe
    
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
                data, closed = self.format_binance_kline_to_dict(stream['data']['k'])
                symbol = data['symbol'].upper()
                # if kline is not closed update the current
                # status of the kline either closing ot still open
                if self.kline_status[symbol] != KlineStatus.CLOSED:
                    self.kline_status[symbol] = (KlineStatus.CLOSING 
                        if closed else KlineStatus.OPEN)
                # update the dataframe appropriately
                klines = self.update_klines(symbol, data)
                # publish klines
                self.publisher.publish(
                    origin=self.__class__.__name__,
                    topic_path=self.topic_path, 
                    data=klines,
                    attributes=dict(symbol=symbol))
            count += 1
            if count % self.log_every == 0:
                logging.info(self.websocket.print_summary(disable_print=True))
                count = 0
    
    def update_klines(self, symbol: str, data: dict) -> dict:
        """ Update symbol klines with the new data

            Parameters
            ----------
            symbol: `str`
                The symbol of the market.
            data: `dict`
                Dictionary formatted data.
            
            Returns
            -------
            `dict`
                Dataframe of klines represented
                into a dictionary format.

            Example
            -------
            .. code-block:: python
            {
                'open': [25.989999771118164, 25.920000076293945, 25.920000076293945], 
                'high': [26.020000457763672, 26.0, 25.96999931335449], 
                'low': [25.79999923706055, 25.84000015258789, 25.739999771118164], 
                'close': [25.93000030517578, 25.920000076293945, 25.76000022888184], 
                'volume': [20038.5703125, 22381.650390625, 12299.23046875], 
                'quote_asset_volume': [518945.0625, 580255.5, 317816.84375], 
                'number_of_trades': [733, 759, 619], 
                'taker_buy_base_vol': [9005.06, 12899.83, 3608.93], 
                'taker_buy_quote_vol': [233168.995, 334395.2921, 93283.808], 
                'close_time': [1630031399999, 1630032299999, 1630033199999], 
                'symbol': ['UNIUSDT', 'UNIUSDT', 'UNIUSDT'], 
                'timeframe': ['15m', '15m', '15m']
            }
        """
        # retrieve the specific klines
        # of the specific symbol and status
        status = self.kline_status[symbol]
        # update the kline appropriately        
        if status == KlineStatus.CLOSING: 
            # update the last row kline and close the kline
            self.markets_klines[symbol].at[len(self.markets_klines[symbol]) 
                - 1, list(data.keys())] = list(data.values())
            self.kline_status[symbol] = KlineStatus.CLOSED
        elif status == KlineStatus.CLOSED:
            # append the klines dataframe with new kline
            # remove the first row of the kline for memory
            self.markets_klines[symbol].at[len(self.markets_klines[symbol]), 
                list(data.keys())] = list(data.values())
            self.markets_klines[symbol].drop(
                self.markets_klines[symbol].head(1).index, inplace=True)
            self.kline_status[symbol] = KlineStatus.OPEN
        else:
            # kline is still open so update the last row
            self.markets_klines[symbol].at[len(self.markets_klines[symbol]) - 1, 
                list(data.keys())] = list(data.values())
        # return the klines into dictionary format
        return self.markets_klines[symbol].to_dict('list')