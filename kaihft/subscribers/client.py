import logging
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

class KaiSubscriberClient():
    def __init__(self, project_id: str = 'keping-ai-continuum'):
        """ Subscribing messages are handled here."""
        self.client = pubsub_v1.SubscriberClient()
        self.project_id = project_id
    
    def subscribe(self,
                  subscription_id: str,
                  callback: callable,
                  timeout: int = None,
                  single_stream: bool = True):
        """ Subscribe messages asynchronously.

            Parameters
            ----------
            subscription_id: `str`
                The subscription id to listen to.
            callback: `callable`
                The callback function.
            timeout: `int`
                The number of seconds the subscriber should listen for messages.
            single_stream: `bool`

        """
        # retrieve the subscription path and initialize
        # the streaming pull to begin subscription
        self.subscription_path = self.client.subscription_path(
            self.project_id, subscription_id)
        self.streaming_pull_future = self.client.subscribe(
            self.subscription_path, callback=callback)
        # log initialization of subscribing to subscription path
        logging.info(
            f"[subscription] listening for messages on {self.subscription_path}, timeout: {timeout}")
        # run single stream
        if single_stream:
            # wrap subscriber in a 'with' block to automatically
            # call cancel() when subscription is done
            with self.client:
                try:
                    # When `timeout` is not set, result() will block indefinitely,
                    # unless an exception is encountered first.
                    self.streaming_pull_future.result(timeout=timeout)
                except TimeoutError as e:
                    # Trigger the shutdown.
                    self.streaming_pull_future.cancel()  
                    # Block until the shutdown is complete.
                    self.streaming_pull_future.result()  
                    logging.error(f"Exception caught {subscription_id}, Error: {e}")
        else:
            logging.warn(f"[multiple-stream] make sure to use `add_done_callback` "
                f"to ensure final done is handled properly! id: {subscription_id}")