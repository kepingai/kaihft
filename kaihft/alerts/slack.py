import requests, json, logging
from .alert import AlertLevel
from typing import Union, Tuple
from kaihft.engines.predict import fetch_id_token

def block_creator(type: str, title: str, body: str = None) -> dict:
    """ Will create a block.

        Parameters
        ----------
        type: `str`
            The type of block
        title: `str`
            The title of the block.
        body: `str`
            The body of the block.
        
        Returns
        -------
        `dict`
            A dictionary formatted block.
    """
    text = f"*{title}*\n{body}" if body else f"{title}"
    return dict(type=type, text=text)

def get_emoji(level: AlertLevel) -> str:
    """ Get the appropriate emoji for different alert level. 

        Parameters
        ----------
        level: `AlertLevel`
            The alert level.
        
        Returns
        -------
        `str`
            Emoji code.
    """
    if level == AlertLevel.INFO: return ":information_source:"
    elif level == AlertLevel.WARNING: return ":warning:"
    elif level == AlertLevel.ERROR: return ":boom:"
    elif level == AlertLevel.CRITICAL: return ":fire:"
    else: return ":question:"

def format_blocks(title: str, 
                  message: str, 
                  level: AlertLevel,
                  product: str) -> Tuple[str, dict]:
    """ Will format error message to blocks. 

        Parameters
        ----------
        title: `str`
            The warning title of the block.
        message: `str`
            The error message.
        level: `AlertLevel`
            The alert level.
        product: `str`
            The product assosiated with this alert.
        
        Returns
        -------
        `Tuple[str, dict]`
            The text message and blocks in dictionary format.
    """
    blocks = [dict(type="header", text=block_creator(
        type="plain_text", title=title)), 
            dict(type="section", text=block_creator(
        type="mrkdwn", title="Level", body=f"`AlertLevel.{level}` {get_emoji(level)}")),
            dict(type="section", text=block_creator(
        type="mrkdwn", title="Error Message :ladybug:", body=message))]
    return f"Alert Level: {level} - {product}", blocks

def alert_slack(origin: str,
                message: str, 
                level: AlertLevel.INFO,
                module: str = "kaihft",
                product: str = "kaisignal-layer-1",
                project_id: str = "keping-ai-continuum",
                username: str = "service-account-kft") -> Union[str, None]:
    """ Will send a block of message to slack
        alert error message to kaibug channel.

        Parameters
        ----------
        origin: `str`
            The specific object name that calls this alert.
        message: `str`
            The error message to send to slack.
        username: `str`
            The assigned username that runs this code.
        project_id: `str`
            The project id of this particular sends.
        product: `str`
            The product assosiated with this alert.
        module: `str`
            The codebase that sends this alert.
        
        Returns
        -------
        `Union[str, None]`
            A string formatted response or None
    """
    post_slack_url = "https://us-central1-keping-ai-continuum.cloudfunctions.net/post_slack"
    title = f"Exception Caught! {get_emoji(level)}"
    text, blocks = format_blocks(title=title, message=message, 
        level=level, product=product)
    # fetch id token firsts
    id_token = fetch_id_token(post_slack_url)
    if id_token is None: return None
    # begin sending response to slack channel
    # authorization in headers
    headers = {"Authorization": f'bearer {id_token}', 
        "content-type" : 'application/json'}
    params = dict(origin=origin, text=text, module=module, product=product, 
        blocks=blocks, project_id=project_id, username=username)
    # begin http request to post_slack url
    result = requests.post(post_slack_url, 
        data=json.dumps(params), headers=headers)
    # return the result if successful
    if result.status_code == 200 and result.content:
        return json.loads(result.content)
    # if not log warn and return None
    logging.warn(f"Failed to send alert to slack- "
        f"code:{result.status_code}, content:{result.content}")
    return None
    