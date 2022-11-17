import logging


class KaiPublisher():
    """ The base class for all Keping AI publishing clients. """
    def __init__(
            self,
            publisher_type: str,
            username: str = 'service-account-kbg',
            project_id: str = 'keping-ai-continuum',
    ):
        """
            Parameters
            ----------
            publisher_type: `str`
                The type of publishing client to use.
                e.g., pubsub, kafka, rabbit_mq (check in `EventClient` enum)
            username: `str`
                Username when publishing messages.
            project_id: `str`
                The project identifier.
        """
        self.username = username
        self.project_id = project_id
        self.type = "RABBITMQ"
        logging.info(f"[init] publishing client: {str(self.type)} with "
                     f"username: '{username}' on project: '{project_id}'.")

    def publish(self, **kwargs):
        """ Publishing a message. """
        raise NotImplementedError("The publish method should be defined "
                                  "on each inheritance!")