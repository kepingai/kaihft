import uuid
import pandas as pd
import numpy as np
import ccxt
import logging, json, asyncio, time
from datetime import datetime, timedelta, timezone
from google.cloud import pubsub_v1
from kaihft.databases import KaiRealtimeDatabase
from kaihft.publishers.client import KaiPublisherClient
from kaihft.alerts.exceptions import RestartPodException
from .strategy import StrategyType, get_strategy, Strategy


class IndexSignalEngine():
    def __init__(self,
                 exchange: str,
                 database: KaiRealtimeDatabase,
                 index_id: str,
                 publisher: KaiPublisherClient,
                 strategy: StrategyType,
                 topic_index_signal: str
                 ):
        """ Initialize the signal engine for index bot.

            Parameters
            ----------
            exchange: `str`
                The name of the exchange.
            database: `KaiRealtimeDatabase`
                Real-time database containing the most-recent signals.
            index_id: `str`
                The index id for the signal, e.g. crypto / defi
            publisher: `KaiPublisherClient`
                The publisher.
            strategy: `StrategyType`
                The strategy to run.
            topic_index_signal: `str`
                The topic path to where signal is published.
        """
        self.exchange = exchange
        self.database = database
        self.index_id = index_id
        self.publisher = publisher
        self.strategy_type = strategy
        self.strategy = get_strategy(self.strategy_type)
        self.topic_index_signal = topic_index_signal

        self.index_id_registry = {'crypto': 'idx0',
                                  'defi': 'idx1'}
        self.index_id_ref = f"indexes/" \
                            f"{self.index_id_registry[self.index_id]}/pairs"

        self.exchange_registry = {'binance': ccxt.binance,
                                  'okx': ccxt.okx}

    def run(self):
        """ Run the signal engine for index bot.
        """
        logging.warn(
            f"[start] index signal engine starts - strategy: {self.strategy}")
        exchange = self.get_exchange()
        symbols = self.get_assets()
        quote = "USDT"

        if self.strategy.send_index_signal(exchange=exchange,
                                           symbols=symbols,
                                           quote=quote):
            index_signal = self.get_signal(index_id=self.index_id)
            self.publish_index_signal(index_signal)

    def get_exchange(self) -> ccxt.Exchange:
        """ Get the appropriate ccxt exchange class.

            Return
            ------
            `ccxt.Exchange`
                The ccxt exchange object corresponding to the exchange type.
        """
        return self.exchange_registry[self.exchange]

    def get_assets(self) -> list:
        """ Retrieve list of assets for an index.

            Return
            ------
            `list`
                A list containing all assets for an index
        """
        return self.database.get(self.index_id_ref)

    def get_signal(self, index_id: str) -> dict:
        """ Get the appropriate index signal data.

            Parameters
            ----------
            index_id: str
                The index id, e.g. crypto / defi

            Return
            ------
            `dict`
                A dictionary containing the signal id and index_id.

            Example
            -------
            >>> {
            ...     "id": 6c031f33-c209-4e35-9a1c-c02f390e15c8,
            ...     "index_id": 'crypto'
            ... }
        """
        return dict(id=uuid.uuid4(),
                    index_id=index_id)

    def publish_index_signal(self, index_signal: dict):
        """ Publish index signal.

            Parameters
            ----------
            index_signal: dict
                The index signal to trigger buy for index bot
        """
        index_id = index_signal['index_id']
        logging.info(f"[Publishing] Publishing trigger for"
                     f" index_id: {index_id}")
        # publish the newly created signal
        # to dedicated archiving topic
        logging.info(f"Publishing to: {self.topic_index_signal}")  # TODO debug
        logging.info(f"data: {index_signal}") # TODO debug
        # self.publisher.publish(
        #     origin=self.__class__.__name__,
        #     topic_path=self.topic_index_signal,
        #     data=index_signal,
        #     attributes=dict(
        #        index_id=index_id
        #     ))

