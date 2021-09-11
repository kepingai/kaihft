import logging
from subscribers.client import KaiSubscriberClient

"""Signal engine producing, listening and inferencing 
        predictions for potential actionable intelligence
        AI signals in Binance Spot. """

def main(strategy: str,
         production: bool):
    """ Running specified strategy to Binance Spot
        exchange and publish signal strategy to Cloud Pub/Sub.

        Parameters
        ----------
        strategy: `str`
            A limited strategy choices to run.
        production: `bool`
            if `True` publisher will publish to production topic.
    """
    if production:
        topic_path = 'distribute-signal-v0'
        logging.warn(f"[production-mode] strategy: BINANCE-SPOT-{strategy}, topic: {topic_path}")
    else: topic_path = 'dev-distribute-signal-v0'

    
