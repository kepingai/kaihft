import pandas as pd
import numpy as np
import logging, json, asyncio, time, os
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from google.cloud import pubsub_v1
from google.protobuf.duration_pb2 import Duration
from kaihft.databases import KaiRealtimeDatabase
from kaihft.publishers.client import KaiPublisherClient
from kaihft.subscribers.client import KaiSubscriberClient
from kaihft.subscribers.rabbitmq_client import KaiRabbitSubscriberClient
from .strategy import StrategyType, get_strategy, Strategy
from .signal import SignalStatus, init_signal_from_rtd, Signal
import traceback
import statistics
import aio_pika


class SignalEngine():
    """ The layer-1 system of KepingAI Signal LSTF. All communication between
        layer 1 and layer 2 is conducted concurrently in this class.
    """
    def __init__(
        self,
        database: KaiRealtimeDatabase,
        database_ref: str, 
        thresholds_ref: str,
        max_drawdowns_ref: str,
        buffers_ref: str,
        pairs_ref: str,
        archive_topic_path: str,
        dist_topic_path: str,
        publisher: KaiPublisherClient,
        subscriptions_params: dict,
        log_every: int,
        log_metrics_every: int,
        strategy: StrategyType = StrategyType.SUPER_TREND_SQUEEZE,
        endpoint: str = 'predict_15m',
        strategy_params: dict = {}):
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
                A string of database reference path to thresholds.
            pairs_ref: `str`
                A string of database reference path to allowed pairs.
            thresholds_ref: `str`
                A string of database reference path to thresholds config.
            max_drawdowns_ref: `str`
                A string of database reference path to maximum drawdowns.
            buffers_ref: `str`
                A string of database reference path to buffers.
            archive_topic_path: `str`
                Is the signal archiving topic path for new and closed signals.
            dist_topic_path: `str`
                Is the distribute-signal topic path for new and closed signals.
            subscriptions_params: `dict`
                A dictionary containing the subscription id and timeout.
            endpoint: `str`
                The endpoint to request to layer 2.
            log_every: `int`
                Log ticker and klines messages every.
            log_metrics_every: `int`
                Log layer 2 inference metrics every.
            strategy: `str`
                The strategy to run, default to STS.
            endpoint: `str`
                The end point for layer 2 connection.
            strategy_params: `dict`

            Example
            -------
            `subscription_params` example please follow:
            >>> {
            ...    "ticker": {
            ...        "id": "dev-ticker-binance-spot",
            ...        "timeout": None # this means to listen for data indefinitely
            ...    },
            ...    "klines": {
            ...        "id": "dev-klines-binance-spot",
            ...        "timeout": 10 # this means to listen for data for 10s only.
            ...    }
            ... }
        """
        self.database = database
        self.database_ref = database_ref
        self.thresholds_ref = thresholds_ref
        self.max_drawdowns_ref = max_drawdowns_ref
        self.buffers_ref = buffers_ref
        self.pairs_ref = pairs_ref
        self.archive_topic_path = archive_topic_path
        self.dist_topic_path = dist_topic_path
        self.publisher = publisher
        self.subscriptions_params = self.validate_params(subscriptions_params)
        self.scouts = []
        self.log_every = log_every
        self.log_metrics_every = log_metrics_every
        self.ticker_counts = 1
        self.klines_counts = 1
        # the initialization should be in this order
        self.strategy_type = strategy
        self.max_drawdowns = self._get_max_drawdowns()
        self.buffers = self._get_buffers()
        self.thresholds = self._get_thresholds()
        self.pairs = self._get_pairs()
        self.cooldowns = {}
        self.listener_thresholds = None
        self.listener_pairs = None
        self.listener_max_drawdowns = None
        self.endpoint = endpoint
        self.strategy = self._get_strategy(strategy, self.thresholds, strategy_params)
        self.strategy_params = strategy_params
        self.is_ready = False
        self.subscribers = {}
        for k, v in self.subscriptions_params.items():
            self.subscribers[k] = v['sub']
    
    def run(self):
        """ Will run signal engine concurrently, 
            learn more in `_run()` function in the source code.
        """
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

    async def _update_signals(self,
                              message: aio_pika.abc.AbstractIncomingMessage):
        async with message.process():
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.update_signals, message)

    async def _ha_callback(self, message: aio_pika.abc.AbstractIncomingMessage):
        async with message.process():
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                self.strategy.calculate_heikin_ashi_trend,
                message)

    async def _scout_signals(self, message: aio_pika.abc.AbstractIncomingMessage):
        async with message.process():
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.scout_signals, message)
    
    async def _run(self):
        """ Subscribe to ticker and klines subscription 
            and begin the signal engine on scouting and monitoring
            ongoing signals concurrently.
        """
        if "ha_klines" in self.subscribers:
            await asyncio.gather(
                self.subscribe('ticker', self._update_signals),
                self.subscribe('ha_klines', self._ha_callback),
                self.subscribe('klines', self._scout_signals),
                self.listen_thresholds(self._update_thresholds),
                self.listen_pairs(self._update_pairs),
                self.listen_buffers(self._update_buffers)
            )
        else:
            await asyncio.gather(
                self.subscribe('ticker', self._update_signals),
                self.subscribe('klines', self._scout_signals),
                self.listen_thresholds(self._update_thresholds),
                self.listen_pairs(self._update_pairs),
                self.listen_max_drawdowns(self._update_max_drawdowns),
                self.listen_buffers(self._update_buffers)
            )
        self.is_ready = True
        # using arbitrary context manager to run the subscription streams
        try:
            await asyncio.Future()
        except TimeoutError as e:
            if "ha_klines" in self.subscribers:
                self.subscribers["ha_klines"].streaming_pull_future.result()
            logging.error(f"Exception caught on subscription, Error: {e}")
        finally:
            # close all subscription to database
            logging.info(f"[close] proceed to close all subscriptions to database ...")
            if self.listener_thresholds: self.listener_thresholds.close()
            if self.listener_pairs: self.listener_pairs.close()
            if self.listener_max_drawdowns: self.listener_max_drawdowns.close()
    
    async def listen_thresholds(self, callback: callable):
        """ Will begin subscription to thresholds in database. 

            Parameters
            ----------
            callback: `callable`
                A function to callback to listen for events.
        """
        self.listener_thresholds = self.database.listen(
            reference=self.thresholds_ref,
            callback=callback)
    
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
    
    async def listen_max_drawdowns(self, callback: callable):
        """ Will begin subscription to max drawdowns in database. 
        
            Parameters
            ----------
            callback: `callable`
                A function to callback to listen for events.
        """
        if self.strategy_type == StrategyType.MAX_DRAWDOWN_SQUEEZE:
            self.listener_max_drawdowns = self.database.listen(
                reference=self.max_drawdowns_ref,
                callback=callback
            )
    
    async def listen_buffers(self, callback: callable):
        """ Will begin subscription to buffers in database. 
        
            Parameters
            ----------
            callback: `callable`
                A function to callback to listen for events.
        """
        self.listener_max_drawdowns = self.database.listen(
            reference=self.buffers_ref,
            callback=callback
        )
    
    def _update_thresholds(self, event):
        """ The callback to update new thresholds to strategy. 

            Parameters
            ----------
            event: `db.Event`
                This event can access the data, path & event_type
        """
        if str(event.event_type) != 'put' or event.data is None: return
        if str(event.path) != '/': 
            logging.error("Thresholds not updated! Only update threshold via "
                f"supervisor's notebook threshold injection!")
            return
        thresholds = event.data
        # ensure that both long and short thresholds in the data
        if 'long' not in thresholds or 'short' not in thresholds: 
            logging.error(f"Thresholds required `long` and `short`, instead got: {thresholds.keys()}")
            return
        # ensure that bet threshold and ttp threshold in the data
        for dir, threshold in thresholds.items():
            if 'bet_threshold' not in threshold or 'ttp_threshold' not in threshold:
                logging.error(f"Missing `bet_threshold` and/or `ttp_threshold` "
                    f", instead got: {threshold}, direction: {dir}")
                return
        # update the strategy's with new thresholds
        logging.info(f"[listen] retrieving-new-thresholds from signal database.")
        self.strategy.long_spread = thresholds['long']['bet_threshold']
        self.strategy.long_ttp = thresholds['long']['ttp_threshold']
        self.strategy.short_spread= thresholds['short']['bet_threshold']
        self.strategy.short_ttp = thresholds['short']['ttp_threshold']
        logging.info(f"[listen] updated-strategy-new-thresholds: {thresholds}")
    
    def _update_pairs(self, event):
        """ The callback to update new pairs to strategy. 

            Parameters
            ----------
            event: `db.Event`
                This event can access the data, path & event_type
        """
        if str(event.event_type) != 'put' or event.data is None: return
        if str(event.path) != '/': 
            logging.error("Pairs not updated! Only update pairs via "
                f"supervisor's notebook pairs injection!")
            return
        pairs = event.data
        # ensure that both long and short pairs in the data
        if 'long' not in pairs or 'short' not in pairs: 
            logging.error(f"Pairs required `long` and `short`, instead got: {pairs.keys()}")
            return
        # ensure that data structure matches with production
        if not isinstance(pairs['long'], list) or not isinstance(pairs['short'], list):
            logging.error(f"Pairs `long` and `short` must be a list, instead got: {pairs}")
            return
        # update the strategy's with new pairs
        logging.info(f"[listen] retrieving-new-pairs from signal database.")
        pairs['long'] = [pair.upper() for pair in pairs['long'] if pair]
        pairs['short'] = [pair.upper() for pair in pairs['short'] if pair]
        self.pairs.update(pairs)
        # update the strategy pairs changes
        self.strategy.pairs = pairs
        logging.info(f"[listen] updated-pairs in strategy: {pairs}")

    def _update_max_drawdowns(self, event):
        """ The callback to update new max drawdowns to strategy. 

            Parameters
            ----------
            event: `db.Event`
                This event can access the data, path & event_type
        """
        if str(event.event_type) != 'put' or event.data is None: return
        if str(event.path) != '/': 
            logging.error("Max drawdowns not updated! Only update it via "
                f"supervisor's notebook pairs injection!")
            return
        max_drawdowns = event.data
        # ensure that both long and short max drawdowns are in the data
        if 'long' not in max_drawdowns or 'short' not in max_drawdowns: 
            logging.error(f"Max Drawdowns required `long` and `short`, instead got: {max_drawdowns.keys()}")
            return
        # update the strategy's with new max drawdowns
        logging.info(f"[listen] retrieving-new-max drawdowns from signal database.")
        self.max_drawdowns.update(max_drawdowns)
        # update the strategy max drawdown changes
        self.strategy.long_max_drawdown = self.max_drawdowns['long']
        self.strategy.short_max_drawdown = self.max_drawdowns['short']
        logging.info(f"[listen] updated-max drawdown: {self.max_drawdowns}")
    
    def _update_buffers(self, event):
        """ The callback to update new buffers to strategy. 

            Parameters
            ----------
            event: `db.Event`
                This event can access the data, path & event_type
        """
        if str(event.event_type) != 'put' or event.data is None: return
        if str(event.path) != '/': 
            logging.error("Buffers not updated! Only update it via "
                f"supervisor's notebook pairs injection!")
            return
        keys = ["inference", "max_volatility", "rollback_volatility", "cooldown_counter", "cooldown"]
        buffers = event.data
        # ensure that inference buffer is in the dictionary
        for key in keys:
            if key not in buffers:
                logging.error(f"Buffers missing key: {key}, instead got: {buffers.keys()}")
            if not isinstance(buffers[key], float) and not isinstance(buffers[key], int):
                logging.error(f"Buffers incorrect data type key: {key}, value: {buffers[key]}")
                return
        # update the strategy with the new buffer
        logging.info(f"[listen] retrieving-buffer from signal database.")
        # update the current buffers class attribute and
        # update the strategy inference buffer 
        self.buffers.update(buffers)
        if hasattr(self.strategy, 'buffer'):
            self.strategy.buffer = self.buffers['inference']
        logging.info(f"[listen] updated buffer: {self.buffers}")
    
    async def subscribe(self,
                        subscription: str,
                        callback: callable):
        """ Will begin subscription to Cloud Pub/Sub.

            Parameters
            ----------
            subscription: `str`
                `ticker` or `klines` topic subscription.
            callback: `callable`
                The callback function to handle messages.
            single_stream: `bool`
                True if the whole engine is meant to subscribe in singular topic only.
        """
        topic_path = self.subscriptions_params[subscription]['id']

        logging.info(f"[{str(self.strategy_type)}] [subscription] purging the "
                     f"subscription path for '{topic_path}' in 30 seconds ...")
        await self.subscribers[subscription].connect()
        await self.subscribers[subscription].subscribe(
            exchange_name=topic_path,
            callback=callback,)
            
    def update_signals(self, message: pubsub_v1.subscriber.message.Message):
        """ Update signals with the most recent data. 
            
            Parameters
            ----------
            message: `pubsub_v1.subscriber.message.Message`
                The message from Cloud pub/sub.
        """
        current_trend = None
        if message.headers and 'timestamp' in message.headers:
            # get the attributes of the message
            symbol = message.headers.get("symbol")
            timestamp = float(message.headers.get("timestamp"))
            seconds_passed = (datetime.now(tz=timezone.utc).timestamp()
                              - timestamp)
            # only accept messages within 2 seconds latency
            if 2 >= seconds_passed >= 0 and self.is_valid_cooldown(symbol):
                if symbol in self.signals and self.signals[symbol].is_open():
                    # retrieve and decode the full data
                    data = json.loads(message.body.decode('utf-8'))['data']
                    # begin update to signal object
                    price_type = 'mark_price' if 'mark_price' in data else 'last_price'
                    last_price = float(data[price_type])
                    # update signal with the latest price
                    # and the local trend (if needed)
                    if 'HEIKIN_ASHI' in str(self.strategy_type) and \
                            self.strategy_params.get("use_ha_stop_dir", False):
                        current_trend = self.strategy.ha_trend.get(symbol, None)
                    self.signals[symbol].update(last_price, current_trend)

            if self.ticker_counts % self.log_every == 0:
                logging.info(f"[ticker] cloud pub/sub messages running, "
                    f"latency: sec, last-symbol: {symbol}")
                # reset the signal counts to 1
                self.ticker_counts = 1
            # add the counter for each message received
            self.ticker_counts += 1

    def scout_signals(self, message: pubsub_v1.subscriber.message.Message):
        """ Will run the strategy from given klines data. 

            Parameters
            ----------
            message: `pubsub_v1.subscriber.message.Message`
                The message from Cloud pub/sub.
        """
        if message.headers and 'timestamp' in message.headers and self.is_ready:
            # get the attributes of the message
            symbol = message.headers.get("symbol")
            # get the symbol of the klines
            base = message.headers.get('base')
            quote = message.headers.get('quote')
            # only run strategy if symbol is currently
            # not an ongoing signal and also not currently
            # awaiting for a result from running strategy
            timestamp = float(message.headers.get("timestamp"))
            seconds_passed = (datetime.now(tz=timezone.utc).timestamp()
                              - timestamp)
            # only accept messages within 2 seconds latency
            if 10 >= seconds_passed >= 0 and self.is_valid_cooldown(symbol):
                if symbol not in self.signals and symbol not in self.scouts:
                    # append the symbol in scouts
                    self.scouts.append(symbol)
                    # run a separate thread to run startegy
                    klines = json.loads(message.body.decode('utf-8'))['data']
                    self.run_strategy(base, quote, symbol, klines)
                if self.klines_counts % self.log_every == 0:
                    logging.info(f"[klines] cloud pub/sub messages running, "
                        f"latency: sec, last-symbol: {symbol}")
                    # reset the signal counts to 1
                    self.klines_counts = 1
                # add the counter for each message received
                self.klines_counts += 1

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
            Klines data should have all of the listed keys here
            >>> {
            ...     "data": {
            ...         'open': [25.989999771118164, 25.920000076293945, 25.920000076293945], 
            ...         'high': [26.020000457763672, 26.0, 25.96999931335449], 
            ...         'low': [25.79999923706055, 25.84000015258789, 25.739999771118164], 
            ...         'close': [25.93000030517578, 25.920000076293945, 25.76000022888184], 
            ...         'volume': [20038.5703125, 22381.650390625, 12299.23046875], 
            ...         'quote_asset_volume': [518945.0625, 580255.5, 317816.84375], 
            ...         'number_of_trades': [733, 759, 619], 
            ...         'taker_buy_base_vol': [9005.06, 12899.83, 3608.93], 
            ...         'taker_buy_quote_vol': [233168.995, 334395.2921, 93283.808], 
            ...         'close_time': [1630031399999, 1630032299999, 1630033199999], 
            ...         'symbol': ['UNIUSDT', 'UNIUSDT', 'UNIUSDT'], 
            ...         'timeframe': ['15m', '15m', '15m']
            ...     }
            ... }
        """
        try:
            # will convert klines to dataframe and
            # format the dataframe appropriately
            dataframe = pd.DataFrame(klines)
            dataframe = self.format_dataframe(dataframe)
            # ignore highly volatile movements
            # this will help layer2's ability for 
            # predicting stationaire market prices
            symbol = f"{base}{quote}".upper()

            if (self.is_valid_volatility(dataframe) 
                    and self.is_valid_cooldown(symbol)
                    and symbol not in self.signals):
                # run technical analysis & inference to layer 2,
                # await for futures before the remaining tasks.
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
                    # archive the signal
                    self.archive_signal(signal)
                    # update/set engine state in real-time
                    self.set_enging_state()                 
        except Exception as e:
            logging.error(f"[strategy] Exception caught running-strategy, "
                          f"symbol:-{symbol}. Unable to connect to firebase "
                          f"rtd pod need to restart, error: {e}", 
                          f"traceback: {traceback.format_exc()}")
            if os.path.exists('tmp/healthy'): os.remove('tmp/healthy')
        finally:
            # ensure that the symbol is removed
            # from scouts so that there will be no locks.
            self.scouts.remove(symbol)
    
    def is_valid_volatility(self, dataframe: pd.DataFrame) -> bool:
        """ Will check if current volatility is suitable for layer 2.

            Parameters
            ----------
            dataframe: `pd.DataFrame`
                Dataframe containing klines of n-ticks.
            
            Returns
            -------
            `bool`
                Return `True` only if volatility does not exceed
                the allowed volatility for layer2.
        """
        rollback = self.buffers['rollback_volatility']
        # retrieve the OHLC
        low = np.min(dataframe.iloc[-rollback:].low)
        high = np.max(dataframe.iloc[-rollback:].high)
        # check if the current kline spread
        # is not above maximium volatility
        return abs(high - low) / low < self.buffers['max_volatility']
    
    def is_valid_cooldown(self, symbol: str) -> bool:
        """ Will check if cooldown time have passed if a pair
            have completed up to maximum cooldown counter.

            Parameters
            ----------
            symbol: `str`
                The symbol of coin.
            
            Returns
            -------
            `bool`
                Return `False` if counter reached cooldown counter
                but current time have not surpass the cooldown time.
        """
        if symbol not in self.cooldowns:
            self.cooldowns[symbol] = dict(counter=0, cooldown=datetime.utcnow())
            return True
        elif self.cooldowns[symbol]['counter'] < self.buffers['cooldown_counter']: 
            return True
        elif self.cooldowns[symbol]['counter'] == self.buffers['cooldown_counter']:
            if datetime.utcnow() >= self.cooldowns[symbol]['cooldown']:
                # revert back the counter and cooldown
                self.cooldowns[symbol]['counter'] = 0
                return True
            logging.info(f"[cooldown] symbol: {symbol} cooldown reached! " 
                f"next cooldown reset: {self.cooldowns[symbol]}")
        return False

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
            # update cooldown if signal completed
            if signal.status in [SignalStatus.COMPLETED, SignalStatus.STOPPED]:
                self.update_cooldown(signal.symbol)
                
            if "HEIKIN_ASHI" in self.strategy.name:
                self.strategy.last_signal[signal.symbol] = datetime.now(tz=timezone.utc).timestamp() 
            
    
    def update_cooldown(self, symbol: str):
        """ Will update the cooldown counter and time of a symbol.

            Parameters
            ----------
            symbol: `str`
                The symbol to update cooldown
        """
        if symbol not in self.cooldowns:
            self.cooldowns[symbol] = dict(counter=0, cooldown=datetime.utcnow())
        self.cooldowns[symbol]['counter'] += 1
        self.cooldowns[symbol]['cooldown'] += timedelta(seconds=self.buffers['cooldown'])

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
        average_tp = statistics.mean(signal.take_profit)
        signal_to_publish = signal.to_dict().copy()
        signal_to_publish.update({"take_profit": average_tp})
        self.publisher.publish(
            origin=self.__class__.__name__,
            topic_path=self.archive_topic_path,
            data=signal_to_publish,
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
        average_tp = statistics.mean(signal.take_profit)
        signal_to_publish = signal.to_dict().copy()
        signal_to_publish.update({"take_profit": average_tp})
        self.publisher.publish(
            origin=self.__class__.__name__,
            topic_path=self.dist_topic_path,
            data=signal_to_publish,
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
            'timeout' in params['ticker'] and 'sub' in params['ticker']), \
            "Subscription ticker param badly formatted."
        assert ('klines' in params and 'id' in params['klines'] and
            'timeout' in params['klines'] and 'sub' in params['klines']), \
            "Subscription klines param badly formatted."
        if 'ha_klines' in params:
            assert ('id' in params['ha_klines'] and 'timeout' in params['ha_klines']
                    and 'sub' in params['ha_klines']), \
                "Subscription heikin-ashi klines param badly formatted."
        return params
    
    def _get_max_drawdowns(self) -> dict:
        """ Will retrieve the maximum drawdown for layer 1. 

            Returns
            -------
            `dict`
                A dictionary containing max drawdowns for 
                long and short signals.
            
            Raises
            ------
            `ValueError`
                will raise if data structure does not match production.
        """
        logging.info(f"[get] retrieving-drawdowns from signal database.")
        max_drawdowns = self.database.get(self.max_drawdowns_ref)
        if 'long' not in max_drawdowns or 'short' not in max_drawdowns:
            raise ValueError(f"[max-drawdowns] missing`long` and `short`, instead got: {max_drawdowns.keys()}")
        logging.info(f"[get] retrieved-max drawdowns: {max_drawdowns} from database.")
        return dict(long=float(max_drawdowns['long']), 
            short=float(max_drawdowns['short']))
    
    def _get_buffers(self) -> dict:
        """ Will retrieve the inference buffer time.

            Returns
            -------
            `dict`
                A dictionary of buffers as follows else default buffers

            >>> {
            ...     "inference": 360,           # the buffer time in sec to inference layer 2
            ...     "max_volatility": 0.1,      # the % max spread of rollback periods
            ...     "rollback_volatility": 5    # rollingback klines to check spread
            ...     "cooldown_counter": 2       # maximum completion before cooldown counter restart.
            ...     "cooldown": 21600           # cooldown time in sec before another signal.
            ... }

            Raises
            ------
            `ValueError`
                will raise if data structure does not match production.
        """
        keys = ["inference", "max_volatility", "rollback_volatility", "cooldown_counter", "cooldown"]
        logging.info(f"[get] retrieving-buffer from signal database.")
        default = dict(inference=360, max_volatility=0.1, 
            rollback_volatility=5, cooldown_counter=10, cooldown=21600)
        buffers = self.database.get(self.buffers_ref)
        for key in keys:
            if key not in buffers:
                raise ValueError(f"[buffers] missing buffer key: {key}, instead got: {buffers.keys()}")
            if not isinstance(buffers[key], float) and not isinstance(buffers[key], int):
                logging.warn(f"[buffer] non numeric buffer key: {key}, value: {buffers[key]} "
                    f"will use default buffers instead: {default}")
                return default
        logging.info(f"[get] retrieved buffers: {buffers} from database.")
        return buffers

    def _get_thresholds(self) -> dict:
        """ Will retrieve the thresholds for layer1.
            
            Returns
            -------
            `dict`
                A dictionary containing thresholds for
                long and short strategies.
            
            Raises
            ------
            `ValueError`
                Will raise if data structure does not match production.
        """
        logging.info(f"[get] retrieving-thresholds from signal database.")
        thresholds = self.database.get(self.thresholds_ref)
        if not thresholds: raise ValueError(f"Thresholds are not set for layer 1!")
        if 'long' not in thresholds or 'short' not in thresholds: 
            raise ValueError(f"Thresholds required `long` and `short`, instead got: {thresholds.keys()}")
        for dir, threshold in thresholds.items():
            if 'bet_threshold' not in threshold or 'ttp_threshold' not in threshold:
                raise ValueError(f"Missing `bet_threshold` and/or `ttp_threshold` "
                    f", instead got: {threshold}, direction: {dir}")
        logging.info(f"[get] retrieved-thresholds: {thresholds} from database.")
        return thresholds
    
    def _get_pairs(self) -> dict:
        """ Will retrieve the pairs for layer1. 

            Returns
            -------
            `dict`
                A dictionary containing long and short pairs
                allowed to be monitored.
            
            Raises
            ------
            `ValueError`
                Will raise if data structure does not match production.
        """
        logging.info(f"[get] retrieving-pairs from signal database.")
        pairs = self.database.get(self.pairs_ref)
        if not pairs: raise ValueError(f"Pairs are not yet set for layer 1!")
        # ensure that both long and short thresholds in the data
        if 'long' not in pairs or 'short' not in pairs: 
            raise ValueError(f"Pairs required `long` and `short`, instead got: {pairs.keys()}")
        # ensure that data structure matches with production
        if not isinstance(pairs['long'], list) or not isinstance(pairs['short'], list):
            raise ValueError(f"Pairs `long` and `short` must be a list, instead got: {pairs}")
        logging.info(f"[get] retrieved-pairs: {pairs} from database")
        # ensure that all pairs uppercase
        long = [pair.upper() for pair in pairs['long'] if pair]
        short = [pair.upper() for pair in pairs['short'] if pair]
        return dict(long=long, short=short)
        
    def _get_strategy(self,
                      strategy: str,
                      thresholds: dict,
                      strategy_params: dict) -> Strategy:
        """ Will return the strategy chosen.

            Parameters
            ----------
            strategy: `str`
                The strategy to run.
            thresholds: `dict`
                A dictionary containing long & short thresholds
            strategy_params: `dict`


            Returns
            -------
            `Strategy`
                A strategy inherited instance.

            Raises
            ------
            `KeyError`
                If key id not match or strategy not available.
        """
        strategy_class = get_strategy(strategy)
        if self.strategy_type == StrategyType.SUPER_TREND_SQUEEZE:
            return strategy_class(
                endpoint=self.endpoint,
                long_spread=thresholds['long']['bet_threshold'],
                long_ttp=thresholds['long']['ttp_threshold'],
                short_spread=thresholds['short']['bet_threshold'],
                short_ttp=thresholds['short']['ttp_threshold'],
                pairs=self.pairs,
                log_every=self.log_metrics_every,
                expiration_minutes=strategy_params.get("expiration_minutes", None)
            )
        elif self.strategy_type == StrategyType.MAX_DRAWDOWN_SQUEEZE:
            return strategy_class(
                endpoint=self.endpoint,
                long_spread=thresholds['long']['bet_threshold'],
                long_ttp=thresholds['long']['ttp_threshold'],
                long_max_drawdown=self.max_drawdowns['long'],
                short_spread=thresholds['short']['bet_threshold'],
                short_ttp=thresholds['short']['ttp_threshold'],
                short_max_drawdown=self.max_drawdowns['short'],
                pairs=self.pairs,
                log_every=self.log_metrics_every,
                expiration_minutes=strategy_params.get("expiration_minutes", None)
            )
        elif (self.strategy_type == StrategyType.MAX_DRAWDOWN_SPREAD or 
              self.strategy_type == StrategyType.MAX_DRAWDOWN_SUPER_TREND_SPREAD):
            return strategy_class(
                endpoint=self.endpoint,
                long_spread=thresholds['long']['bet_threshold'],
                long_ttp=thresholds['long']['ttp_threshold'],
                long_max_drawdown=self.max_drawdowns['long'],
                short_spread=thresholds['short']['bet_threshold'],
                short_ttp=thresholds['short']['ttp_threshold'],
                short_max_drawdown=self.max_drawdowns['short'],
                pairs=self.pairs,
                log_every=self.log_metrics_every,
                buffer=self.buffers['inference'],
                expiration_minutes=strategy_params.get("expiration_minutes", None)
            )
        elif self.strategy_type == StrategyType.HEIKIN_ASHI_HYBRID:
            return strategy_class(
                mode=strategy_params['mode'],
                kaiforecast_version=strategy_params.get("kaiforecast_version", None),
                classification_threshold=strategy_params[
                    'classification_threshold'],
                long_spread=thresholds['long']['bet_threshold'],
                short_spread=thresholds['short']['bet_threshold'],
                long_ttp=thresholds['long']['ttp_threshold'],
                short_ttp=thresholds['short']['ttp_threshold'],
                pairs=self.pairs,
                ha_timeframe=strategy_params['ha_timeframe'],
                model_timeframe=strategy_params['timeframe'],
                ha_ema_len=strategy_params['ha_ema_len'],
                log_every=self.log_metrics_every,
                expiration_minutes=strategy_params.get("expiration_minutes", None)
            )
        elif self.strategy_type == StrategyType.HEIKIN_ASHI_REGRESSION:
            return strategy_class(
                mode=strategy_params['mode'],
                kaiforecast_version=strategy_params.get("kaiforecast_version", None),
                long_spread=thresholds['long']['bet_threshold'],
                short_spread=thresholds['short']['bet_threshold'],
                long_ttp=thresholds['long']['ttp_threshold'],
                short_ttp=thresholds['short']['ttp_threshold'],
                pairs=self.pairs,
                ha_timeframe=strategy_params['ha_timeframe'],
                model_timeframe=strategy_params['timeframe'],
                ha_ema_len=strategy_params['ha_ema_len'],
                log_every=self.log_metrics_every,
                endpoint=self.endpoint,
                expiration_minutes=strategy_params.get("expiration_minutes", None)
            )
        elif self.strategy_type == StrategyType.HEIKIN_ASHI_FRAC_DIFF:
            return strategy_class(
                mode=strategy_params['mode'],
                kaiforecast_version=strategy_params.get("kaiforecast_version", None),
                long_spread=thresholds['long']['bet_threshold'],
                short_spread=thresholds['short']['bet_threshold'],
                long_ttp=thresholds['long']['ttp_threshold'],
                short_ttp=thresholds['short']['ttp_threshold'],
                pairs=self.pairs,
                ha_timeframe=strategy_params['ha_timeframe'],
                model_timeframe=strategy_params['timeframe'],
                ha_ema_len=strategy_params['ha_ema_len'],
                log_every=self.log_metrics_every,
                endpoint=self.endpoint,
                features=strategy_params["features"],
                expiration_minutes=strategy_params.get("expiration_minutes", None)
            )
        else:
            raise ValueError(f"[strategy] strategy type: {self.strategy_type} not valid!")
