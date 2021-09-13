import pandas as pd
import logging, json, threading
from google.cloud import pubsub_v1
from publishers.client import KaiPublisherClient
from subscribers.client import KaiSubscriberClient

class SignalEngine():
    def __init__(
        self,
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
        self.topic_path = topic_path
        self.publisher = publisher
        self.subscriber = subscriber
        self.subscriptions_params = self.validate_params(subscriptions_params)
        self.strategy = self.validate_strategy(strategy)
        # initialize running async threads registry
        self.async_registry = {}
    
    def run(self):
        """ Will run signal engine concurrent threads asynchronously. """
        logging.info(f"[start] signal engine starts - strategy: {self.strategy}, subscriber: {self.subscriber}")
        # retrive the most recent signal data from subscriber
        self.signals = self.get_signals()
        # log the signals that is currently running in pub/sub
        for symbol, signal in self.signals.items():
            logging.info(f"*running-signal* - symbol: {symbol}, signal: {signal.to_dict()}")
        # run engine concurrently in separate threads
        try:
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
        finally:
            logging.warn(f"[stops] signal engine stops! - strategy: {self.strategy}, subscriber: {self.subscriber}")
            
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
                self.signals['symbol'].update(
                    ticker=json.loads(message.data.decode('utf-8'))['data'])
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
            symbol = message.attributes.get("symbol")
            if symbol not in self.signals and symbol not in self.async_registry:
                # run a separate thread to run startegy
                klines = json.loads(message.data.decode('utf-8'))['data']
                scout_thread = threading.Thread(
                    target=self.run_strategy,
                    args=(symbol, klines),
                    name=f'[scout] potential-signal-run: symbol: {symbol}')
                # the current symbol in registry
                self.async_registry[symbol] = scout_thread
                scout_thread.start()
                message.ack()

    def run_strategy(self, symbol: str, klines: dict):
        """ Run strategy to the given klines data. 

            Parameters
            ----------
            symbol: `str`
                The ticker symbol.
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
        logging.info(f"[strategy] running strategy - {self.strategy} symbol: {klines['symbol']} - n_klines: {len(klines['open'])}")
        del self.async_registry[symbol]
        
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
        logging.info(f"[get] retrieving-signal from signal topic...")
        return {}

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
    
    def validate_strategy(self, strategy: str) -> bool:
        """ Will return true if strategy is supported. 
            Parameters
            ----------
            strategy: `str`
                The strategy to run.
            Returns
            -------
            `bool`
                True if strategy is supported.
        """
        return strategy == 'STS'
