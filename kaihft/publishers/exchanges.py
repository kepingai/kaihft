import pandas as pd
import time, json, logging, asyncio
from typing import Dict
from .client import KaiPublisherClient
from enum import Enum
from typing import Tuple
from datetime import datetime, timedelta
from kaihft.alerts import RestartPodException
from binance.client import Client
from binance.exceptions import BinanceAPIException
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager
from kaihft.databases import KaiRealtimeDatabase


class KlineStatus(Enum):
    """ An enum object representing kline status. """
    OPEN = 'OPEN'
    CLOSED = 'CLOSED'
    def __str__(self):
        return str(self.value)
        
    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, str): 
            return str(self.value).upper() == __o.upper()
        return super().__eq__(__o)


class BaseTickerKlinesPublisher():
    """ Base ticker publisher subclass. """
    def __init__(self,
                 name: str,
                 websocket: any,
                 stream_id: any,
                 publisher: KaiPublisherClient,
                 topic_path: str,
                 quotes: list,
                 log_every: int = 100000):
        """ A helper class with helper functions for publishing ticker & 
            klines messages. 

            Parameters
            ----------
            name: `str`
                The name of the publisher
            websocket: `any`
                The websocket manager.
            stream_id: `any`
                The id of the stream.
            publisher: `KaiPublisherClient`
                The cloud pub/sub publisher.
            topic_path: `str`
                The topic to publish messages to
            quotes: `list`
                The cryptocurrency quotes to listen to.
            log_every: `int`
                Log ticker and klines every n-iteration.
        """
        self.name = name
        self.websocket = websocket
        self.stream_id = stream_id
        self.publisher = publisher
        self.topic_path = topic_path
        self.log_every = log_every
        self.quotes = quotes

        logging.info(f"[init] ticker publisher initialized {self.name}, {self.websocket}, "
            f"from: {self.stream_id}, to: {self.topic_path}, log update every: {self.log_every} messages.")

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
            interval=data["k"]["i"],
            open_price=float(data["k"]["o"]),
            last_price=float(data["k"]["c"]),
            high_price=float(data["k"]["h"]),
            low_price=float(data["k"]["l"]),
            base_asset_volume=float(data["k"]["v"]),
            number_of_trades=float(data["k"]["n"]),
            is_closed=data["k"]["x"],
            quote_asset_volume=float(data["k"]["q"]),
            taker_buy_base_asset_volume=float(data["k"]["V"]),
            taker_buy_quote_asset_volume=float(data["k"]["Q"]))

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

    def get_base_quote(self, symbol: str) -> Tuple[str, str]:
        """ Will return the allowed base and quote from
            current signal enginge.

            Parameters
            ----------
            symbol: `str`
                The full basequote symbol.
            
            Returns
            -------
            `Tuple[str, str]` 
                A tuple of base, quote.
        """
        _symbol = symbol.upper()
        for quote in self.quotes:
            _quote = quote.upper()
            if symbol.upper().endswith(_quote):
                return _symbol.split(quote)[0], _quote
        raise ValueError(f"base/quote from symbol: {symbol}"
                         f"is not applicable to signal engine!")


class BinanceUSDMKlinesPublisher(BaseTickerKlinesPublisher):
    __FLOATINGS = [
        "open", "high", "close", "low",
        "volume", "quote_asset_volume",
        "taker_buy_base_vol", "taker_buy_quote_vol"]
    __COLUMNS = [
        "timestamp", "open", "high", "low", "close",
        "volume", "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_vol", "taker_buy_quote_vol", "ignore"]
    __DROP = ["timestamp", "ignore"]

    def __init__(self,
                 client: Client,
                 websocket: BinanceWebSocketApiManager,
                 markets: list,
                 stream_id: str,
                 publisher: KaiPublisherClient,
                 database: KaiRealtimeDatabase,
                 pairs_ref: str,
                 quotes: list = ['USDT'],
                 topic_path: str = 'klines-binance-v0',
                 n_klines: int = 250,
                 timeframe: int = 1,
                 ):
        """ This class streams klines data from Binance websocket API
            for BinanceUSDM

            Parameters
            ----------
            client: `binance.Client`
                we will get the historical klines from this object
            websocket: `BinanceWebSocketApiManager`
                The websocket to retrieve data.
            stream_id: `str`
                the stream id
            publisher: `KaiPublisherClient`
                publisher client to publish data
            topic_path: `str`
                the topic to publish the data to
            n_klines: `int`
                the number of candles to publish
            timeframe: `int`
                market timeframe in minutes. If using 1h, please input 60, etc
        """
        super(BinanceUSDMKlinesPublisher, self).__init__(
            name='BINANCEUSDM', websocket=websocket, stream_id=stream_id,
            publisher=publisher, topic_path=topic_path, quotes=quotes)
        self.client = client
        assert n_klines <= 1000
        self.n_klines = n_klines
        self.timeframe = timeframe
        self._kline_intervals = {1: self.client.KLINE_INTERVAL_1MINUTE,
                                 3: self.client.KLINE_INTERVAL_3MINUTE,
                                 5: self.client.KLINE_INTERVAL_15MINUTE,
                                 15: self.client.KLINE_INTERVAL_15MINUTE,
                                 30: self.client.KLINE_INTERVAL_30MINUTE,
                                 60: self.client.KLINE_INTERVAL_1HOUR,
                                 120: self.client.KLINE_INTERVAL_2HOUR,
                                 240: self.client.KLINE_INTERVAL_4HOUR}
        assert self.timeframe in self._kline_intervals.keys()
        self.sleep = 0.05
        self.markets = markets
        self.database = database
        self.pairs_ref = pairs_ref
        self.listener_pairs = None

        # initialize the market klines and status. Each time we get a new data, we will either update or replace the
        # latest value
        self.markets_klines, self.kline_status = self.initialize_klines()

    def initialize_klines(self) -> Tuple[Dict, Dict]:
        """ Initialize all historical n-klines for the specified markets.
        """
        markets_klines = {}
        kline_status = {}
        interval = self._kline_intervals[self.timeframe]

        for market in self.markets:
            start = time.time()
            try:
                # retrieve the last historical n-klines using function for futures market
                klines = self.client.futures_klines(symbol=market,
                                                    interval=interval,
                                                    limit=self.n_klines)
            except BinanceAPIException as e:
                if e.status_code == 400:
                    continue
                else:
                    logging.error(f"Exception caught retrieving historical klines: {e}")

            if klines is None or len(klines) == 0:
                continue
            symbol = market.upper()
            markets_klines[symbol] = self.to_dataframe(
                symbol=symbol, interval=str(interval), klines=klines)
            # check if the market kline have closed
            kline_status[symbol] = (KlineStatus.CLOSED if
                                    datetime.utcnow().minute % self.timeframe == 0 else KlineStatus.OPEN)
            # if all successful calculate the
            # the overall execution time and delay if needed
            logging.info(f"initialized klines: {market}-{interval} - duration: {time.time() - start} seconds")
            self.delay(start)
        logging.info(f"Successful kline initializations: with interval={interval}")
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

    def run(self):
        """ Run the _run function with asyncio
        """
        asyncio.run(self._run())

    async def _run(self):
        """ This function runs stream klines and listen_pairs concurrently.
        """
        await asyncio.gather(self.listen_pairs(self.restart_pod),
                             self.stream_klines()
                             )

    async def stream_klines(self):
        """ Use the binance websocket to stream the latest candle data from binance.
            The latest candle data will either replace the latest candle or
            update it.
        """
        count = 0
        while True:
            # binance spot will only allow 24h max stream
            # connection, this will automatically close the script
            if self.websocket.is_manager_stopping():
                exit(0)
            # get and remove the oldest entry from the `stream_buffer` stack
            oldest_stream_data_from_stream_buffer = self.websocket.pop_stream_data_from_stream_buffer(mode="LIFO")
            # print the stream data from stream buffer
            if oldest_stream_data_from_stream_buffer is False:
                time.sleep(0.01)
            else:
                stream = json.loads(oldest_stream_data_from_stream_buffer)
                if 'data' not in stream:
                    continue

                # format the data from websocket, to make sure all exchanges have the same final format
                data, closed = self.format_binance_kline_to_dict(stream['data']['k'])
                symbol = data['symbol'].upper()
                # if kline is closed change the kline status
                self.kline_status[symbol] = (KlineStatus.CLOSED
                                             if closed else KlineStatus.OPEN)
                # update the dataframe appropriately
                klines = self.update_klines(symbol, data)
                base, quote = self.get_base_quote(symbol)

                self.publisher.publish(
                    origin=self.__class__.__name__,
                    topic_path=self.topic_path,
                    data=klines,
                    attributes=dict(
                        base=base,
                        quote=quote,
                        symbol=symbol))

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
        # TODO: FIX THIS KLINE UPDATE MECHANISM
        # retrieve the specific klines
        # of the specific symbol and status
        status = self.kline_status[symbol]
        if status == KlineStatus.CLOSED:
            # append the klines dataframe with new kline
            # remove the first row of the kline for memory
            self.markets_klines[symbol] = self.markets_klines[symbol].append(data, ignore_index=True)
            self.markets_klines[symbol] = self.markets_klines[symbol].iloc[1:]
            self.kline_status[symbol] = KlineStatus.OPEN
        else:
            # kline is still open so update the last row
            self.markets_klines[symbol].at[self.markets_klines[symbol].index[-1],
                                           list(data.keys())] = list(data.values())
        # return the klines into dictionary format
        return self.markets_klines[symbol].to_dict('list')

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
        if end <= self.sleep:
            time.sleep(abs(self.sleep - end))

    async def listen_pairs(self, callback: callable):
        """ Will begin subscription to pairs in database.

            Parameters
            ----------
            callback: `callable`
                A function to callback to listen for events.
        """
        self.listener_pairs = self.database.listen(
            reference=self.pairs_ref,
            callback=callback
            )

    def restart_pod(self, event):
        """ This function checks the event every time listen pairs is called.
            If the pairs changed, we restart the pod

            Parameters
            ----------
            event: `db.event`
                firebase event, from which we can get the firebase data

            Raises
            -------
            `RestartPodException`
                if a change is detected in the database

        """
        # get the pairs from the event and combine the long and short pairs.
        if str(event.event_type) != 'put' or event.data is None:
            pass
        elif str(event.path) == '/':
            logging.info("First event. Pairs not updated!")
            pass
        else:
            logging.info(f"Pairs {event.data} updated")
            logging.info("Updated trading pairs. Restarting pod")
            raise RestartPodException


class BinanceUSDMTickerPublisher(BaseTickerKlinesPublisher):
    def __init__(self,
                 websocket: BinanceWebSocketApiManager,
                 stream_id: str,
                 publisher: KaiPublisherClient,
                 topic_path: str = 'ticker-binance-v0'):
        """ Publish ticker data to a defined topic.

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
        super(BinanceUSDMTickerPublisher, self).__init__(
            name='BINANCEUSDM', websocket=websocket, stream_id=stream_id,
            publisher=publisher, topic_path=topic_path, quotes=['USDT'])

    def run(self):
        """ This function streams the binance usdm market for all available tickers
            and publish the data to pub/sub.
            Example of streamer output:
            {'stream': '!markPrice@arr@1s',
             'data': [{'e': 'markPriceUpdate', 'E': 1645063614000, 's': 'BTCUSDT', 'p': '44023.90000000',
                       'P': '44008.08594154', 'i': '44027.24044522', 'r': '0.00010000', 'T': 1645084800000},
                      {'e': 'markPriceUpdate', 'E': 1645063614000, 's': 'ETHUSDT', 'p': '3141.27000000',
                       'P': '3142.68209814', 'i': '3142.91636241', 'r': '0.00007725', 'T': 1645084800000}]
            This function will take the output, format it using from_mark_price function, then publish it
            to our ticker topic.
        """
        count = 0
        start = time.time()
        while True:
            # binance spot will only allow 24h max stream
            # connection, this will automatically close the script
            # if the last message published was above 30s ago, raise exception
            last_published = time.time() - start
            if self.websocket.is_manager_stopping() or last_published >= 30:
                raise RestartPodException(f"Websocket manager stopped, last message "
                                          f"published: {round(last_published, 2)} seconds ago, restarting pod!")
            # get and remove the oldest entry from the `stream_buffer` stack
            oldest_stream_data_from_stream_buffer = self.websocket.pop_stream_data_from_stream_buffer(mode="LIFO")

            if oldest_stream_data_from_stream_buffer is False:
                time.sleep(0.01)
            else:
                stream = json.loads(oldest_stream_data_from_stream_buffer)
                if 'data' not in stream:
                    continue

                datas = {}
                # format data to uniform ticker futures
                for _data in stream['data']:
                    data = self.from_mark_price_stream(_data)
                    datas[data["symbol"]] = data

                self.publisher.publish(
                    origin=self.__class__.__name__,
                    topic_path=self.topic_path,
                    data=datas,
                    attributes=dict(timestamp=str(datetime.utcnow())))

                # restart the time
                start = time.time()

            count += 1
            if count % self.log_every == 0:
                logging.info(self.websocket.print_summary(disable_print=True))
                count = 0

    def from_mark_price_stream(self, data: dict) -> dict:
        """ Format the mark price stream data to match message
            to_ticker_futures formatting.

            https://binance-docs.github.io/apidocs/futures/en/#mark-price-stream

            Parameters
            ----------
            data: `dict`
                data stream from Binance-USDM

            Returns
            -------
            `dict`
                The dictionary uniform formatted.
        """
        return dict(timestamp=data['E'], symbol=data['s'],
                    mark_price=data['p'], index_price=data['i'],
                    settle_price=data['P'], funding_rate=data['r'],
                    next_funding_time=data['T'])

    
class BinanceSpotKlinesPublisher(BaseTickerKlinesPublisher):
    __FLOATINGS = [
        "open", "high", "close", "low",
        "volume", "quote_asset_volume",
        "taker_buy_base_vol", "taker_buy_quote_vol"]
    __COLUMNS = [
        "timestamp", "open", "high", "low", "close",
        "volume", "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_vol", "taker_buy_quote_vol", "ignore"]
    __DROP = ["timestamp", "ignore"]

    def __init__(self,
                 client: Client,
                 websocket: BinanceWebSocketApiManager,
                 markets: list,
                 stream_id: str,
                 publisher: KaiPublisherClient,
                 database: KaiRealtimeDatabase,
                 pairs_ref: str,
                 quotes: list = ['USDT'],
                 topic_path: str = 'klines-binance-v0',
                 n_klines: int = 250,
                 timeframe: int = 1,
                 ):
        """ This class streams klines data from Binance websocket API
            for Binance Spot

            Parameters
            ----------
            client: `binance.Client`
                we will get the historical klines from this object
            websocket: `BinanceWebSocketApiManager`
                The websocket to retrieve data.
            stream_id: `str`
                the stream id
            publisher: `KaiPublisherClient`
                publisher client to publish data
            topic_path: `str`
                the topic to publish the data to
            n_klines: `int`
                the number of candles to publish
            timeframe: `int`
                market timeframe in minutes. If using 1h, please input 60, etc
        """
        super().__init__(
            name='BINANCESPOT', websocket=websocket, stream_id=stream_id,
            publisher=publisher, topic_path=topic_path, quotes=quotes)
        self.client = client
        assert n_klines <= 1000
        self.n_klines = n_klines
        self.timeframe = timeframe
        self._kline_intervals = {1: self.client.KLINE_INTERVAL_1MINUTE,
                                 3: self.client.KLINE_INTERVAL_3MINUTE,
                                 5: self.client.KLINE_INTERVAL_15MINUTE,
                                 15: self.client.KLINE_INTERVAL_15MINUTE,
                                 30: self.client.KLINE_INTERVAL_30MINUTE,
                                 60: self.client.KLINE_INTERVAL_1HOUR,
                                 120: self.client.KLINE_INTERVAL_2HOUR,
                                 240: self.client.KLINE_INTERVAL_4HOUR}
        assert self.timeframe in self._kline_intervals.keys()
        self.sleep = 0.05
        self.markets = markets
        self.database = database
        self.pairs_ref = pairs_ref
        self.listener_pairs = None

        # initialize the market klines and status. Each time we get a new data, we will either update or replace the
        # latest value
        self.markets_klines, self.kline_status = self.initialize_klines()

    def initialize_klines(self) -> Tuple[Dict, Dict]:
        """ Initialize all historical n-klines for the specified markets.
        """
        markets_klines = {}
        kline_status = {}
        interval = self._kline_intervals[self.timeframe]

        start_timestamp = (datetime.utcnow() -
                           timedelta(minutes=self.timeframe * self.n_klines)).timestamp()

        for market in self.markets:
            start = time.time()
            try:
                # retrieve the last historical n-klines using function for spot market
                klines = self.client.get_historical_klines(symbol=market,
                                                           start_str=str(start_timestamp),
                                                           interval=interval,
                                                           limit=self.n_klines)
            except BinanceAPIException as e:
                if e.status_code == 400:
                    continue
                else:
                    logging.error(f"Exception caught retrieving historical klines: {e}")

            if klines is None or len(klines) == 0:
                continue
            symbol = market.upper()
            markets_klines[symbol] = self.to_dataframe(
                symbol=symbol, interval=str(interval), klines=klines)
            # check if the market kline have closed
            kline_status[symbol] = (KlineStatus.CLOSED if
                                    datetime.utcnow().minute % self.timeframe == 0 else KlineStatus.OPEN)

            # if all successful calculate the
            # the overall execution time and delay if needed
            logging.info(f"initialized klines: {market}-{interval} - duration: {time.time() - start} seconds")
            self.delay(start)
        logging.info(f"Successful kline initializations: with interval={interval}")
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

    def run(self):
        """ Run the _run function with asyncio
        """
        asyncio.run(self._run())

    async def _run(self):
        """ This function runs stream klines and listen_pairs concurrently.
        """
        await asyncio.gather(self.listen_pairs(self.restart_pod),
                             self.stream_klines()
                             )

    async def stream_klines(self):
        """ Use the binance websocket to stream the latest candle data from binance.
            The latest candle data will either replace the latest candle or
            update it.
        """
        count = 0
        while True:
            # binance spot will only allow 24h max stream
            # connection, this will automatically close the script
            if self.websocket.is_manager_stopping():
                exit(0)
            # get and remove the oldest entry from the `stream_buffer` stack
            oldest_stream_data_from_stream_buffer = self.websocket.pop_stream_data_from_stream_buffer(mode="LIFO")
            # print the stream data from stream buffer
            if oldest_stream_data_from_stream_buffer is False:
                time.sleep(0.01)
            else:
                stream = json.loads(oldest_stream_data_from_stream_buffer)
                if 'data' not in stream:
                    continue

                # format the data from websocket, to make sure all exchanges have the same final format
                data, closed = self.format_binance_kline_to_dict(stream['data']['k'])
                symbol = data['symbol'].upper()
                # if kline is closed change the kline status
                self.kline_status[symbol] = (KlineStatus.CLOSED
                                             if closed else KlineStatus.OPEN)
                # update the dataframe appropriately
                klines = self.update_klines(symbol, data)
                base, quote = self.get_base_quote(symbol)

                self.publisher.publish(
                    origin=self.__class__.__name__,
                    topic_path=self.topic_path,
                    data=klines,
                    attributes=dict(
                        base=base,
                        quote=quote,
                        symbol=symbol))

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
        if status == KlineStatus.CLOSED:
            # append the klines dataframe with new kline
            # remove the first row of the kline for memory
            self.markets_klines[symbol] = self.markets_klines[symbol].append(data, ignore_index=True)
            self.markets_klines[symbol] = self.markets_klines[symbol].iloc[1:]
            self.kline_status[symbol] = KlineStatus.OPEN
        else:
            # kline is still open so update the last row
            self.markets_klines[symbol].at[self.markets_klines[symbol].index[-1],
                                           list(data.keys())] = list(data.values())
        # return the klines into dictionary format
        return self.markets_klines[symbol].to_dict('list')

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
        if end <= self.sleep:
            time.sleep(abs(self.sleep - end))

    async def listen_pairs(self, callback: callable):
        """ Will begin subscription to pairs in database.

            Parameters
            ----------
            callback: `callable`
                A function to callback to listen for events.
        """
        self.listener_pairs = self.database.listen(
            reference=self.pairs_ref,
            callback=callback
        )

    def restart_pod(self, event):
        """ This function checks the event every time listen pairs is called.
            If the pairs changed, we restart the pod

            Parameters
            ----------
            event: `db.event`
                firebase event, from which we can get the firebase data

            Raises
            -------
            `RestartPodException`
                if a change is detected in the database

        """
        # get the pairs from the event and combine the long and short pairs.
        if str(event.event_type) != 'put' or event.data is None:
            pass
        elif str(event.path) == '/':
            logging.info("First event. Pairs not updated!")
            pass
        else:
            logging.info(f"Pairs {event.data} updated")
            logging.info("Updated trading pairs. Restarting pod")
            raise RestartPodException


class BinanceSpotTickerPublisher(BaseTickerKlinesPublisher):
    def __init__(self,
                 websocket: BinanceWebSocketApiManager,
                 stream_id: str,
                 publisher: KaiPublisherClient,
                 topic_path: str = 'ticker-binance-v0'):
        """ Publish ticker data to a defined topic.

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
        super().__init__(
            name='BINANCESPOT', websocket=websocket, stream_id=stream_id,
            publisher=publisher, topic_path=topic_path, quotes=['USDT'])

    def run(self):
        """ This function streams the binance spot market for all available tickers
            and publish the data to pub/sub.
            Example of streamer output:
            {'stream': '!!miniTicker@arr',
             'data': [{"e": "24hrMiniTicker", "E": 123456789, "s": "BNBBTC", "c": "0.0025",
                       "o": "0.0010", "h": "0.0025", "l": "0.0010", "v": "10000",  "q": "18"},
                      {"e": "24hrMiniTicker", "E": 123456789, "s": "ETHBTC", "c": "0.0025",
                       "o": "0.0010", "h": "0.0025", "l": "0.0010", "v": "10000",  "q": "18"}]
            This function will take the output, format it using format_binance_spot_ticker_to_dict function, 
            then publish it to our ticker topic.
        """
        count = 0
        start = time.time()
        while True:
            # binance spot will only allow 24h max stream
            # connection, this will automatically close the script
            # if the last message published was above 30s ago, raise exception
            last_published = time.time() - start
            if self.websocket.is_manager_stopping() or last_published >= 30:
                raise RestartPodException(f"Websocket manager stopped, last message "
                                          f"published: {round(last_published, 2)} seconds ago, restarting pod!")
            # get and remove the oldest entry from the `stream_buffer` stack
            oldest_stream_data_from_stream_buffer = self.websocket.pop_stream_data_from_stream_buffer(mode="LIFO")

            if oldest_stream_data_from_stream_buffer is False:
                time.sleep(0.01)
            else:
                stream = json.loads(oldest_stream_data_from_stream_buffer)

                # format data to uniform
                for data_sample in stream:
                    data = self.format_binance_spot_ticker_to_dict(data_sample)
                    stream_time = data['timestamp']

                    self.publisher.publish(
                        origin=self.__class__.__name__,
                        topic_path=self.topic_path,
                        data=data,
                        attributes=dict(timestamp=str(stream_time), 
                                        symbol=data['symbol']))

                # restart the time
                start = time.time()

            count += 1
            if count % self.log_every == 0:
                logging.info(self.websocket.print_summary(disable_print=True))
                count = 0

    def format_binance_spot_ticker_to_dict(self, data: dict) -> dict:
        """ Format the stream data to message.

            https://binance-docs.github.io/apidocs/spot/en/#all-market-mini-tickers-stream

            Parameters
            ----------
            data: `dict`
                data stream from Binance-USDM

            Returns
            -------
            `dict`
                The dictionary uniform formatted.
        """

        return dict(timestamp=data['E'], symbol=data['s'],
                    close_price=data['c'], open_price=data['o'],
                    high_price=data['h'], low_price=data['l']
                    )         

    
class BinanceTickerPublisher(BaseTickerKlinesPublisher):
    """ Binance ticker publisher class."""
    def __init__(self,
            websocket: BinanceWebSocketApiManager,
            stream_id: str,
            publisher: KaiPublisherClient,
            quotes: list = ["USDT"],
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
            quotes: `list`
                A list containing allowed quotes.
            topic_path: `str`
                The topic path to publish data.
        """
        super(BinanceTickerPublisher, self).__init__(
            name='BINANCE', websocket=websocket, stream_id=stream_id,
            publisher=publisher, topic_path=topic_path, quotes=quotes)

    def run(self):
        """ Load,format data from websocket manager & publish
            it to the topic specified during initialization.
        """
        count = 0
        start = time.time()
        while True:
            # binance spot will only allow 24h max stream
            # connection, this will automatically close the script
            # if the last message published was above 30s ago, raise exception
            last_published = time.time() - start
            if self.websocket.is_manager_stopping() or last_published >= 30: 
                raise RestartPodException(f"Websocket manager stopped, last message "
                    f"published: {round(last_published, 2)} seconds ago, restarting pod!")
            # get and remove the oldest entry from the `stream_buffer` stack
            oldest_stream_data_from_stream_buffer = self.websocket.pop_stream_data_from_stream_buffer()
            if oldest_stream_data_from_stream_buffer is False: time.sleep(0.01)
            else:
                stream = json.loads(oldest_stream_data_from_stream_buffer)
                if 'data' not in stream: continue
                data = self.format_binance_ticker_to_dict(stream['data'])
                base, quote = self.get_base_quote(data['symbol'])
                self.publisher.publish(
                    origin=self.__class__.__name__,
                    topic_path=self.topic_path,
                    data=data,
                    attributes=dict(
                        base=base,
                        quote=quote,
                        symbol=data['symbol'],
                        timestamp=str(data["timestamp"])))
                # restart the time
                start = time.time()
            count += 1
            if count % self.log_every == 0:
                logging.info(self.websocket.print_summary(disable_print=True))
                count = 0

class BinanceKlinesPublisher(BaseTickerKlinesPublisher):
    """ Binance Klines publisher class. """
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
            quotes: list = ["USDT"],
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
            publisher=publisher, topic_path=topic_path, quotes=quotes)
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
                else: logging.error(f"Exception caught retrieving historical klines: {e}")
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
            logging.info(f"[init] initialized klines: {market}-{interval} - with duration: {time.time() - start} seconds")
            self.delay(start)
        logging.info(f"[init] successful kline initializations: {self.markets}-{interval}")
        return markets_klines, kline_status

    def to_dataframe(self, symbol: str, interval: str, klines: list) -> pd.DataFrame:
        """ Will convert klines from binance to dataframe.

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
        start = time.time()
        while True:
            # binance spot will only allow 24h max stream
            # connection, this will automatically close the script
            # if the last message published was above 30s ago, raise exception
            last_published = time.time() - start
            if self.websocket.is_manager_stopping() or last_published >= 30: 
                raise RestartPodException(f"Websocket manager stopped, last message "
                    f"published: {round(last_published, 2)} seconds ago, restarting pod!")
            # get and remove the oldest entry from the `stream_buffer` stack
            oldest_stream_data_from_stream_buffer = self.websocket.pop_stream_data_from_stream_buffer()
            # print the stream data from stream buffer
            if oldest_stream_data_from_stream_buffer is False: time.sleep(0.01)
            else:
                stream = json.loads(oldest_stream_data_from_stream_buffer)
                if 'data' not in stream: continue
                data, closed = self.format_binance_kline_to_dict(stream['data']['k'])
                symbol = data['symbol'].upper()
                # if kline is closed change the kline status
                self.kline_status[symbol] = (KlineStatus.CLOSED
                    if closed else KlineStatus.OPEN)
                # update the dataframe appropriately
                klines = self.update_klines(symbol, data)
                base, quote = self.get_base_quote(symbol)
                # publish klines
                self.publisher.publish(
                    origin=self.__class__.__name__,
                    topic_path=self.topic_path,
                    data=klines,
                    attributes=dict(
                        base=base,
                        quote=quote,
                        symbol=symbol,
                        timestamp=str(stream['data']["E"])))
                # restart the time
                start = time.time()
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
            >>> {
            ...    'open': [25.989999771118164, 25.920000076293945, 25.920000076293945], 
            ...    'high': [26.020000457763672, 26.0, 25.96999931335449], 
            ...    'low': [25.79999923706055, 25.84000015258789, 25.739999771118164], 
            ...    'close': [25.93000030517578, 25.920000076293945, 25.76000022888184], 
            ...    'volume': [20038.5703125, 22381.650390625, 12299.23046875], 
            ...    'quote_asset_volume': [518945.0625, 580255.5, 317816.84375], 
            ...    'number_of_trades': [733, 759, 619], 
            ...    'taker_buy_base_vol': [9005.06, 12899.83, 3608.93], 
            ...    'taker_buy_quote_vol': [233168.995, 334395.2921, 93283.808], 
            ...    'close_time': [1630031399999, 1630032299999, 1630033199999], 
            ...    'symbol': ['UNIUSDT', 'UNIUSDT', 'UNIUSDT'], 
            ...    'timeframe': ['15m', '15m', '15m']
            ... }
        """
        # retrieve the specific klines
        # of the specific symbol and status
        status = self.kline_status[symbol]
        if status == KlineStatus.CLOSED:
            # append the klines dataframe with new kline
            # remove the first row of the kline for memory
            self.markets_klines[symbol] = self.markets_klines[symbol].append(data, ignore_index=True)
            self.markets_klines[symbol] = self.markets_klines[symbol].iloc[1:]
            self.kline_status[symbol] = KlineStatus.OPEN
        else:
            # kline is still open so update the last row
            self.markets_klines[symbol].at[self.markets_klines[symbol].index[-1],
                list(data.keys())] = list(data.values())
        # return the klines into dictionary format
        return self.markets_klines[symbol].to_dict('list')