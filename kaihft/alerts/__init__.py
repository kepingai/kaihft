import os, sys
from .alert import *
from .slack import *
from functools import wraps

def notify_failure(fn: callable):
    """ This decorator will output the traceback of a 
        raised exception to slack and the log.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        try: return fn(*args, **kwargs)
        except Exception as error:
            # logging error first
            logging.error(error)
            # get the class filename and the specific
            # line that causes this exception 
            exc_type, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            origin = f"Class: {exc_type}, Filename: {fname}, Line: {exc_tb.tb_lineno}"
            # send the alert to slack
            alert_slack(origin=origin, message=str(error), level=AlertLevel.ERROR)
            # raise the error
            raise error
    return wrapper