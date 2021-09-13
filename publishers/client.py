import json
from google.cloud.pubsub import PublisherClient

class KaiPublisherClient():
    def __init__(self, 
                 username: str = 'service-account-kft',
                 project_id: str = 'keping-ai-continuum'):
        """ Publishing messages are handeled here."""
        self.client = PublisherClient()
        self.username = username
        self.project_id = project_id

    def publish(self,
                origin: str,
                topic_path: str,
                data: dict,
                attributes: dict = {}):
        """ Publish messages embedded with attributes. 

            Parameters
            ----------
            origin: `str`
                The origin path of publishing message.
            topic_path: `str`
                The path to publish message.
            data: `dict`
                The data to be published.
            attributes: `dict`
                The attributes to be embedded in the message.

            Returns
            -------
            `str`
                The published message result.
        """
        # encode the data to string utf-8 format
        # get the appropriate topic path
        _data = json.dumps(dict(data=data)).encode('utf-8')
        _topic_path = self.client.topic_path(self.project_id, topic_path)
        # add extra stuff to the origin before publishing message
        attributes.update({'origin': origin, 'username': self.username})
        future = self.client.publish(_topic_path, _data, **attributes)
        return future.result()