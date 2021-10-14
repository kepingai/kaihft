import os, sys
from .alert import *
from .slack import *
from pathlib import Path
from .exceptions import *
from functools import wraps

def notify_failure(fn: callable):
    """ This decorator will output the traceback of a 
        raised exception to slack and the log.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        try: 
            health_path = 'tmp/healthy'
            # create a tmp healthy file
            # this is for liveness probe check
            # for each pod running each service
            if not os.path.exists('tmp'): os.mkdir('tmp')
            Path(health_path).touch()
            return fn(*args, **kwargs)
        except Exception as error:
            # logging error first
            logging.error(error)
            # get the class filename and the specific
            # line that causes this exception 
            exc_type, _, exc_tb = sys.exc_info()
            # if explicit restart pod exception is raised
            # delete the health path to trigger liveness probe to restart pod
            if exc_type == RestartPodException:
                if os.path.exists(health_path): os.remove(health_path)
            # else if the exception raise is not meant
            # for restarting the pod, send error to slack
            else:
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                origin = (f"Class: {exc_type}, Filename: {fname}, "
                    f"Line: {exc_tb.tb_lineno}")
                alert_slack(origin=origin, message=str(error), level=AlertLevel.ERROR)
            # raise the error
            raise error
    return wrapper