import logging
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

class KaiSubscriberClient():
    def __init__(self):
        """ Subscribing messages are handled here."""
        self.client = pubsub_v1.SubscriberClient()
        self.project_id = 'keping-ai-continuum'
    
    def subscribe(self,
                  subscription_id: str,
                  callback: callable,
                  timeout: int = None):
        """ Subscribe messages asynchronously.

            Parameters
            ----------
            subscription_id: `str`
                The subscription id to listen to.
            callback: `callable`

            timeout: `int`
                The number of seconds the subscriber should listen for messages.
        """
        # retrieve the subscription path and initialize
        # the streaming pull to begin subscription
        subscription_path = self.client.subscription_path(
            self.project_id, subscription_id)
        streaming_pull_future = self.client.subscribe(
            subscription_path, callback=callback)
        # log initialization of subscribing to subscription path
        logging.info(
            f"Listening for messages on {subscription_path}, timeout: {timeout}")
        # wrap subscriber in a 'with' block to automatically
        # call cancel() when subscription is done
        with self.client:
            try:
                # When `timeout` is not set, result() will block indefinitely,
                # unless an exception is encountered first.
                streaming_pull_future.result(timeout=timeout)
            except TimeoutError as e:
                # Trigger the shutdown.
                streaming_pull_future.cancel()  
                # Block until the shutdown is complete.
                streaming_pull_future.result()  
                logging.error(f"Exception caught {self.subscription_id}, Error: {e}")