import logging


class KaiSubscriber():
    """ The base class for all Keping AI subscription clients. """
    def __init__(
            self,
            subscriber_type: str,
            project_id: str = 'keping-ai-continuum',
    ):
        """
            Parameters
            ----------
            subscriber_type: `str`
                The type of subscription client to use.
                e.g., pubsub, kafka, rabbit_mq (check in `EventClient` enum)
            username: `str`
                Username when subscripting messages.
            project_id: `str`
                The project identifier.
        """
        self.project_id = project_id
        self.type = "RABBITMQ"
        logging.info(f"[init] subscription client: {str(self.type)} "
                     f"on project: '{project_id}'.")

    def subscribe(self, **kwargs):
        """ Subscribing the messages. """
        raise NotImplementedError("The subscribe method should be defined "
                                  "on each inheritance!")
