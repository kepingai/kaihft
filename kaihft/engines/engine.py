import pandas as pd
import logging, json, asyncio
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
        thresholds_ref: str,
        archive_topic_path: str,
        dist_topic_path: str,
        publisher: KaiPublisherClient,
        ticker_subscriber: KaiSubscriberClient,
        klines_subscriber: KaiSubscriberClient,
        subscriptions_params: dict,
        log_every: int,
        log_metrics_every: int,
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
            archive_topic_path: `str`
                Is the signal archiving topic path for new and closed signals.
            dist_topic_path: `str`
                Is the distribute-signal topic path for new and closed signals.
            ticker_subscriber: `KaiSubscriberClient`
                The subscriber client for ticker.
            klines_subscriber: `KaiSubscriberClient`
                The subscriber client for klines.
            subscriptions_params: `dict`
                A dictionary containing the subscription id and timeout.
            strategy: `str`
                The strategy to run, default to STS.
            log_every: `int`
                Log ticker and klines messages every.
            log_metrics_every: `int`
                Log layer 2 inference metrics every.

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
        self.thresholds_ref = thresholds_ref
        self.archive_topic_path = archive_topic_path
        self.dist_topic_path = dist_topic_path
        self.publisher = publisher
        self.ticker_subscriber = ticker_subscriber
        self.klines_subscriber = klines_subscriber
        self.subscriptions_params = self.validate_params(subscriptions_params)
        self.scouts = []
        self.log_every = log_every
        self.log_metrics_every = log_metrics_every
        self.ticker_counts = 1
        self.klines_counts = 1
        self.thresholds = self._get_thresholds()
        self.strategy = self._get_strategy(strategy, self.thresholds)
    
    def run(self):
        """ Will run signal engine concurrent threads asynchronously. """
        logging.warn(f"[start] signal engine starts - strategy: {self.strategy}")
        # retrive the most recent signal data from subscriber
        self.signals = self.get_signals()
        # log the signals that is currently running in pub/sub
        for symbol, signal in self.signals.items():
            logging.info(f"*running-signal* - symbol: {symbol}, "
                f"symbol: {signal.symbol}, direction: {signal.direction}, "
                f"spread: {signal.spread}")
        # begin coroutines and concurrency control
        asyncio.run(self._run())
        # engine stopped!
        logging.warn(f"[stop] signal engine stops -  strategy {self.strategy} ")
    
    async def _run(self):
        """ Subscribe to ticker and klines subscription 
            and begin the signal engine on scouting and monitoring
            ongoing signals concurrently.
        """
        await asyncio.gather(
            self.subscribe(self.ticker_subscriber, 'ticker', self.update_signals),
            self.subscribe(self.klines_subscriber, 'klines', self.scout_signals)
        )
        with self.ticker_subscriber.client, self.klines_subscriber.client:
            try:
                # When `timeout` is not set, result() will block indefinitely,
                # unless an exception is encountered first.
                self.ticker_subscriber.streaming_pull_future.result(
                    timeout=self.subscriptions_params['ticker']['timeout'])
                self.klines_subscriber.streaming_pull_future.result(
                    timeout=self.subscriptions_params['klines']['timeout'])
            except TimeoutError as e:
                # Trigger the shutdown.
                self.ticker_subscriber.streaming_pull_future.cancel()  
                self.klines_subscriber.streaming_pull_future.cancel()
                # Block until the shutdown is complete.
                self.ticker_subscriber.streaming_pull_future.result()  
                self.klines_subscriber.streaming_pull_future.result()
                logging.error(f"Exception caught subscription, Error: {e}")
    
    async def subscribe(self,
                        subscriber: KaiSubscriberClient,
                        subscription: str,
                        callback: callable):
        # start subscription path and futures
        subscriber.subscribe(
            subscription_id=self.subscriptions_params[subscription]['id'],
            timeout=self.subscriptions_params[subscription]['timeout'],
            callback=callback,
            single_stream=False)
            
    def update_signals(self, message: pubsub_v1.subscriber.message.Message):
        """ Update signals with the most recent data. 
            
            Parameters
            ----------
            message: `pubsub_v1.subscriber.message.Message`
                The message from Cloud pub/sub.
        """
        if message.attributes:
            message_time = datetime.utcfromtimestamp(message.publish_time.timestamp())
            seconds_passed = (datetime.utcnow() - message_time).total_seconds()
            # get the symbol
            symbol = message.attributes.get("symbol")
            # only accept messages within 30 seconds
            if seconds_passed <= 30:
                if symbol in self.signals and self.signals[symbol].is_open():
                    # begin update to signal object
                    last_price = json.loads(message.data
                        .decode('utf-8'))['data']['last_price']
                    # update signal with the lastest price
                    self.signals[symbol].update(last_price)
            if self.ticker_counts % self.log_every == 0:
                logging.info(f"[ticker] cloud pub/sub messages running, "
                    f"latency: {seconds_passed}s, last-symbol: {symbol}")
                # reset the signal counts to 1
                self.ticker_counts = 1
            # add the counter for each message received
            self.ticker_counts += 1
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
            message_time = datetime.utcfromtimestamp(message.publish_time.timestamp())
            seconds_passed = (datetime.utcnow() - message_time).total_seconds()
            symbol = message.attributes.get("symbol")
            # only accept messages within 30 seconds
            if seconds_passed <= 30:
                # get the symbol of the klines
                base = message.attributes.get('base')
                quote = message.attributes.get('quote')
                # only run strategy if symbol is currently
                # not an ongoing signal and also not currently
                # awaiting for a result from running strategy
                if symbol not in self.signals and symbol not in self.scouts:
                    # append the symbol in scouts
                    self.scouts.append(symbol)
                    # run a separate thread to run startegy
                    klines = json.loads(message.data.decode('utf-8'))['data']
                    self.run_strategy(base, quote, symbol, klines)
            if self.klines_counts % self.log_every == 0:
                logging.info(f"[klines] cloud pub/sub messages running, "
                    f"latency: {seconds_passed}s, last-symbol: {symbol}")
                # reset the signal counts to 1
                self.klines_counts = 1
            # add the counter for each message received
            self.klines_counts += 1
        # acknowledge the message
        message.ack()

    def run_strategy(self, base: str, quote: str, symbol: str, klines: dict):
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
            # run technical analysis & inference to layer 2,
            # await for futures before the remaining tasks.
            signal = self.strategy.scout(
                base=base, 
                quote=quote,
                dataframe=dataframe, 
                callback=self.close_signal)
            # if signal is triggered
            if signal and signal.symbol not in self.signals: 
                # save the signal to class attrs
                self.signals[symbol] = signal
                # distribute the signal
                self.distribute_signal(signal)
                # archive the signal
                self.archive_signal(signal)
                # update/set engine state in real-time
                self.set_enging_state()
        except Exception as e:
            logging.error(f"[strategy] Exception caught running-strategy, "
                f"symbol:-{symbol}, error: {e}")
        finally:
            # ensure that the symbol is removed
            # from scouts so that there will be no locks.
            self.scouts.remove(symbol)

    def close_signal(self, signal: Signal):
        """ A callback function that will 
            delete signal from engine state and 
            update state to database.

            Parameters
            ----------
            signal: `Signal`
                A signal object to delete from state.
        """
        if signal.symbol in self.signals: 
            logging.info(f"[closing] signal symbol: {signal.symbol} from engine state.")
            # distribute the completed / expired signal to topic
            self.distribute_signal(signal)
            # archive the completed / expired signal to topic
            self.archive_signal(signal)
            # delete the signal from signals dictionary
            del self.signals[signal.symbol]
            logging.info(f"current active signals: {self.signals.keys()}")
            # update the real-time database with newly updated dictionary
            self.set_enging_state()
    
    def set_enging_state(self):
        """ Will update database with the current engine state. """
        # set the current engine state to database 
        self.database.set(reference=self.database_ref, data=self.signals)
        logging.info(f"[update] update-engine-state to "
            f"database:{self.database_ref}, n-signals: {len(self.signals)}")

    def archive_signal(self, signal: Signal):
        """ Will send new signal and closed signal to
            cloud pub/sub archiving topic.

            Parameters
            ----------
            signal: `Signal`
                A signal object publish to topic.
        """
        logging.info(f"[archiving] signal-archive to "
            f"topic:{self.archive_topic_path}, symbol: {signal.symbol}")
        # publish the newly created signal
        # to dedicated archiving topic
        self.publisher.publish(
            origin=self.__class__.__name__,
            topic_path=self.archive_topic_path,
            data=signal.to_dict(),
            attributes=dict(
                symbol=signal.symbol,
                base=signal.base,
                quote=signal.quote,
                status=str(signal.status)))
    
    def distribute_signal(self, signal: Signal):
        """ Will send new signal and closed signal to
            cloud pub/sub distribution topic.

            Parameters
            ----------
            signal: `Signal`
                A signal object publish to topic.
        """
        logging.info(f"[distribute] signal-distributing to "
            f"topic:{self.dist_topic_path}, symbol: {signal.symbol}")
        # publish the newly created signal
        # to dedicated distributed topic
        self.publisher.publish(
            origin=self.__class__.__name__,
            topic_path=self.dist_topic_path,
            data=signal.to_dict(),
            attributes=dict(
                symbol=signal.symbol,
                base=signal.base,
                quote=signal.quote,
                status=str(signal.status)))
    
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
    
    def _get_thresholds(self) -> dict:
        """ Will retrieve the thresholds for layer1.
            
            Returns
            -------
            `dict`
                A dictionary containing thresholds for
                long and short strategies.
        """
        logging.info(f"[get] retrieving-thresholds from signal database.")
        thresholds = self.database.get(self.thresholds_ref)
        if not thresholds: raise ValueError(f"Thresholds are not set for layer 1!")
        if 'long' not in thresholds or 'short' not in thresholds: 
            ValueError(f"Thresholds required `long` and `short`, instead got: {thresholds.keys()}")
        for dir, threshold in thresholds.items():
            if 'bet_threshold' not in threshold or 'ttp_threshold' not in threshold:
                raise ValueError(f"Missing `bet_threshold` and/or `ttp_threshold` "
                    f", instead got: {threshold}, direction: {dir}")
        logging.info(f"[get] retrieved-thresholds: {thresholds} from database.")
        return thresholds
    
    def _get_strategy(self, strategy: str, thresholds: dict) -> Strategy:
        """ Will return the strategy chosen.

            Parameters
            ----------
            strategy: `str`
                The strategy to run.
            thresholds: `dict`
                A dictionary containing long & short thresholds

            Returns
            -------
            `Strategy`
                A strategy inherited instance.

            Raises
            ------
            `KeyError`
                If key id not match or strategy not available.
        """
        return get_strategy(strategy)(
            long_spread=thresholds['long']['bet_threshold'],
            long_ttp=thresholds['long']['ttp_threshold'],
            short_spread=thresholds['short']['bet_threshold'],
            short_ttp=thresholds['short']['ttp_threshold'],
            log_every=self.log_metrics_every)