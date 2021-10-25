import os
from kaihft.alerts import alert_slack, AlertLevel

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials.json'

def test_alert_slack():
    """ Will test sending message to slack. """
    alert_slack(origin="kaihft_testing_module",
        message="testing slack debug notif",
        level=AlertLevel.INFO)
    