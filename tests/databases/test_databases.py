import os

import firebase_admin
from kaihft.databases import KaiRealtimeDatabase

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials.json'

def test_database():
    # initialize database 
    database = KaiRealtimeDatabase()
    # test get method from database
    thresholds = database.get('dev/thresholds')
    assert len(thresholds) != 0
    # test set method from database
    database.set('dev/test', {'status': True})
    status = database.get('dev/test/status')
    assert status
    # test update method from database
    database.update('dev/test', {'status': False})
    status = database.get('dev/test/status')
    assert not status
    firebase_admin.delete_app(database.application)