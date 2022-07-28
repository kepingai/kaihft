import logging
from kaihft.databases import KaiRealtimeDatabase
from kaihft.engines.strategy import StrategyType
from kaihft.publishers.client import KaiPublisherClient
from kaihft.subscribers.client import KaiSubscriberClient
from kaihft.engines import IndexSignalEngine


def main(exchange: str,
         strategy: str,
         production: bool,
         log_every: int,
         index_id: str,
         version: str = 'v0'):
    try: strategy = StrategyType(strategy)
    except Exception as e:
        logging.error(f"[strategy] strategy: {strategy} is not valid!")
        raise e
    db_url = 'https://keping-ai-continuum.firebaseio.com/'
    database = KaiRealtimeDatabase(database_url=db_url)
    publisher = KaiPublisherClient()

    path = "prod" if production else "dev"
    topic_index_signal = f"{path}-indexer-{version}"

    # initialize the index signal engine
    index_signal_engine = IndexSignalEngine(
        exchange=exchange,
        database=database,
        index_id=index_id,
        publisher=publisher,
        log_every=log_every,
        strategy=strategy,
        topic_index_signal=topic_index_signal)
    # run the engine
    logging.info("Running index signal engine") # TODO debug
    index_signal_engine.run()
