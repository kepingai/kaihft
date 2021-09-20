import pandas as pd
import logging, json, threading
from datetime import datetime
from google.cloud import pubsub_v1
from kaihft.databases import KaiRealtimeDatabase
from kaihft.publishers.client import KaiPublisherClient
from kaihft.subscribers.client import KaiSubscriberClient
from .strategy import get_strategy, Strategy
from .signal import init_signal_from_rtd, Signal

class SignalEngine():
    def __init__(
        self,
        database: KaiRealtimeDatabase,
        database_ref: str, 
        topic_path: str,
        publisher: KaiPublisherClient,
        subscriber: KaiSubscriberClient,
        subscriptions_params: dict,
        strategy: str = 'STS'):
        """ Will initialize the signal engine
            to scout for potential actionable intelligence
            from ticker, klines data subscriber. This signal
            engine is also responsible to call layer 2 to 
            the specific trained KAIHFT model for inference.

            Parameters
            ----------
            database: `KaiRealtimeDatabase`
                real-time database containing the most-recent signals.
            database_ref: `str`
                A string of database reference path.
            topic_path: `str`
                Is the distribute-signal topic path for new signals.
            subscriber: `KaiSubscriberClient`
                The subscriber client.
            subscriptions_params: `dict`
                A dictionary containing the subscription id and timeout.
            strategy: `str`
                The strategy to run, default to STS.
            
            Example
            -------
            subscription_params example please follow:
            .. code-block:: python
            {
                "ticker": {
                    "id": "dev-ticker-binance-spot",
                    "timeout": None # this means to listen for data indefinitely
                },
                "klines": {
                    "id": "dev-klines-binance-spot",
                    "timeout": 10 # this means to listen for data for 10s only.
                }
            }
        """
        self.database = database
        self.database_ref = database_ref
        self.topic_path = topic_path
        self.publisher = publisher
        self.subscriber = subscriber
        self.subscriptions_params = self.validate_params(subscriptions_params)
        self.strategy = self._get_strategy(strategy)
    
    def run(self):
        """ Will run signal engine concurrent threads asynchronously. """
        logging.info(f"[start] signal engine starts - strategy: {self.strategy}, subscriber: {self.subscriber}")
        # retrive the most recent signal data from subscriber
        self.signals = self.get_signals()
        # log the signals that is currently running in pub/sub
        for symbol, signal in self.signals.items():
            logging.info(f"*running-signal* - symbol: {symbol}, symbol: {signal.symbol}, direction: {signal.direction}, spread: {signal.spread}")
        # run engine concurrently in separate threads
        # create separate thread to update signals
        # with the most recent ticker data from subscriber.
        ticker_thread = threading.Thread(
                target=self.subscribe,
                args=('ticker', self.update_signals), 
                name=f"thread-ticker-subscription")
        # create separate thread to subscribe with the latest
        # klines from subscription client. Use the klines data 
        # to run strategy and inference to layer 2.
        klines_thread = threading.Thread(
                target=self.subscribe,
                args=('klines', self.scout_signals), 
                name=f"thread-klines-subscription")
        # run the threads together
        ticker_thread.start()
        klines_thread.start()
        ticker_thread.join()
        klines_thread.join()
        logging.info(f"[synced] ticker-klines signal engine synced! - strategy: {self.strategy}, subscriber: {self.subscriber}")
            
    def update_signals(self, message: pubsub_v1.subscriber.message.Message):
        """ Update signals with the most recent data. 
            
            Parameters
            ----------
            message: `pubsub_v1.subscriber.message.Message`
                The message from Cloud pub/sub.
        """
        if message.attributes:
            # get the symbol of the ticker
            symbol = message.attributes.get("symbol")
            if symbol in self.signals:
                # begin update to signal object
                last_price = json.loads(message.data
                    .decode('utf-8'))['data']['last_price']
                # update signal with the lastest price
                self.signals[symbol].update(last_price=last_price)
        # acknowledge the message only if
        message.ack()

    def scout_signals(self, message: pubsub_v1.subscriber.message.Message):
        """ Will run the strategy from given klines data. 

            Parameters
            ----------
            message: `pubsub_v1.subscriber.message.Message`
                The message from Cloud pub/sub.
        """
        if message.attributes:
            # get the symbol of the klines
            base = message.attributes.get('base')
            quote = message.attributes.get('quote')
            symbol = message.attributes.get("symbol")
            tag = "[thread]"
            thread_name = f"{tag}-{symbol}"
            threads = [thread.name for thread in threading.enumerate() if thread.name.startswith(tag)]
            if symbol not in self.signals and thread_name not in threads:
                # run a separate thread to run startegy
                klines = json.loads(message.data.decode('utf-8'))['data']
                scout_thread = threading.Thread(
                    target=self.run_strategy,
                    args=(base, quote, symbol, klines),
                    name=thread_name)
                # start the scouting thread
                scout_thread.start()
                scout_thread.join()
            # else: 
            #     print(f"not running scout, symbol: {symbol}, signals: {self.signals.keys()}, threads: {threads}")
        # acknowledge the message
        message.ack()

    def run_strategy(self, 
                     base: str, 
                     quote: str, 
                     symbol: str,
                     klines: dict):
        """ Run strategy to the given klines data. 

            Parameters
            ----------
            base: `str`
                The base symbol of the ticker.
            quote: `str`
                The quote symbol of the ticker.
            symbol: `str`
                The basequote symbol of the ticker.
            klines: `dict`
                The klines data from Cloud pub/sub.
            
            Example
            -------
            .. code-block:: python
            {
                "data": {
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
            }
        """
        try:
            # will convert klines to dataframe and
            # format the dataframe appropriately
            dataframe = pd.DataFrame(klines)
            dataframe = self.format_dataframe(dataframe)
            # run technical analysis & inference to layer 2
            signal = self.strategy.scout(
                base=base, 
                quote=quote,
                dataframe=dataframe, 
                callback=self.close_signal)
            # if signal is triggered
            if signal: 
                # save the signal to class attrs
                self.signals[symbol] = signal
                # distribute the signal
                self.distribute_signal(signal)
        except Exception as e:
            logging.error(f"[strategy] Exception caught running-strategy, "
                f"symbol:-{symbol}, error: {e}")

    def close_signal(self, signal: Signal):
        """ A callback function that will 
            delete signal from engine state and 
            update state to database.

            Parameters
            ----------
            signal: `Signal`
                A signal object to delete from state.
        """
        logging.info(f"[closing] signal symbol: {signal.symbol} from engine state.")
        # distribute the completed / expired signal to topic
        self.distribute_signal(signal)
        # delete the signal from signals dictionary
        if signal.symbol in self.signals: del self.signals[signal.symbol]
        logging.info(f"current active signals: {self.signals.keys()}")
        # update the real-time database with newly updated dictionary
        self.database.set(reference=self.database_ref, data=self.signals)
        logging.info(f"[update] update-engine-state to "
            f"database:{self.database_ref}, n-signals: {len(self.signals)}")
    
    def distribute_signal(self, signal: Signal):
        """ Will send new signal to publisher topic & update
            current engine state to database.

            Parameters
            ----------
            signal: `Signal`
                A signal object publish to topic.
        """
        logging.info(f"[distribute] signal-distributing to "
            f"topic:{self.topic_path}, symbol: {signal.symbol}")
        logging.info(f"current active signals: {self.signals.keys()}")
        # publish the newly created signal
        # to dedicated distributed topic
        self.publisher.publish(
            origin=self.__class__.__name__,
            topic_path=self.topic_path,
            data=signal.to_dict(),
            attributes=dict(
                symbol=signal.symbol,
                base=signal.base,
                quote=signal.quote,
                status=str(signal.status)))
        logging.info(f"[distribute] signal-distributed to "
            f"topic:{self.topic_path}, symbol: {signal.symbol}")
        # set the current engine state to database 
        self.database.set(reference=self.database_ref, data=self.signals)
        logging.info(f"[update] update-engine-state to "
            f"database:{self.database_ref}, n-signals: {len(self.signals)}")
    
    def format_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """ Will format the dataframe to ensure appropriate format for
            inferencing to layer 2 without errors.

            Parameters
            ----------
            dataframe: `pd.DataFrame`
                The dataframe to format.
            
            Returns
            -------
            `pd.DataFrame`
                Newly formatted dataframe.
        """
        ohlcv = ['open', 'high', 'close', 'low', 'volume']
        # convert the close time to utc string datetime
        dataframe['datetime'] = dataframe.close_time.apply(
            lambda x: str(pd.to_datetime(datetime.fromtimestamp(
                (x / 1000)).strftime('%c'))))
        dataframe[ohlcv] = dataframe[ohlcv].astype('float32')
        return dataframe
        
    def subscribe(self, 
                  subscription: str, 
                  callback: callable):
        """ Will subscribe to topic and initialize a callback
            everytime a message is published to this topic.

            Parameters
            ----------
            subscription: `str`
                The subscription topic
            callback: `callable`
                The callback function for every message.

        """
        self.subscriber.subscribe(
            subscription_id=self.subscriptions_params[subscription]['id'],
            callback=callback,
            timeout=self.subscriptions_params[subscription]['timeout'])
    
    def get_signals(self) -> dict:
        """ Will return the last signals sent to topic. 

            Returns
            -------
            `dict`
                A dictionary containing signal of the strategy
                assigned to this engine in dictionary format.
        """
        _signals = {}
        logging.info(f"[get] retrieving-signal from signal database.")
        signals = self.database.get(self.database_ref)
        if not signals: return _signals
        for _, signal in signals.items():
            _signal = init_signal_from_rtd(signal, self.close_signal)
            _signals[_signal.symbol] = _signal
        logging.info(f"[get] retrieved {len(_signals)} from database.")
        return _signals

    def validate_params(self, params: dict) -> dict:
        """ Will return the subscription params if its valid.

            Parameters
            ----------
            params: `dict`
                The parameters to validate from.
            
            Returns
            -------
            params: `dict`
                The parameters in dict format.
            
            Raise
            -----
            `AssertionError`
                If params badly formatted.
        """
        assert ('ticker' in params and 'id' in params['ticker'] and
            'timeout' in params['ticker']), "Subscription ticker param badly formatted."
        assert ('klines' in params and 'id' in params['klines'] and
            'timeout' in params['klines']), "Subscription klines param badly formatted."
        return params
    
    def _get_strategy(self, strategy: str) -> Strategy:
        """ Will return the strategy chosen.

            Parameters
            ----------
            strategy: `str`
                The strategy to run.
            Returns
            -------
            `Strategy`
                A strategy inherited instance.
            Raises
            ------
            `KeyError`
                If key id not match or strategy not available.
        """
        return get_strategy(strategy)()