from abc import abstractmethod
import firebase_admin, uuid
from firebase_admin import db    

class Database():
    def __init__(self):
        self.id = str(uuid.uuid4())
    @abstractmethod
    def get(self, reference: str):
        raise NotImplementedError()
    @abstractmethod
    def update(self, reference: str, data: dict):
        raise NotImplementedError()
    @abstractmethod
    def set(self, reference: str, data: dict):
        raise NotImplementedError()
    @abstractmethod
    def clean_up(self, data: dict) -> dict:
        raise NotImplementedError()

class KaiRealtimeDatabase(Database):
    def __init__(self,
                 database_url: str = 'https://keping-ai-continuum-default-rtdb.firebaseio.com/'):
        """ This class handles read/write to KepingAI's
            real-time databases. Note: this database's
            usage is purely meant to save small-medium
            states of data. This is not a usage for archiving
            database.
        """
        super().__init__()
        self.database_url = database_url
        self.application = firebase_admin.initialize_app(
            options={'databaseURL': database_url})
    
    def get(self, reference: str) -> dict:
        """ Will retrieve the data in database reference.

            Paramaters
            ----------
            reference: `str`
                The reference to database child.
            
            Returns
            -------
            `dict`
                A dictionary containing data from database
                reference given in params.
        """
        return db.reference(reference).get()
    
    def set(self, reference: str, data: dict):
        """ Will set the referenced database path to data given.

            Parameters
            ----------
            reference: `str 
                The reference to database child.
            data: `dict`
                The dictionary containining key values
                to set reference child to.
        """
        _data = self.clean_up(data)
        return db.reference(reference).set(_data)
    
    def update(self, reference: str, data: dict):
        """ Will update the referenced database path to data given.

            Parameters
            ----------
            reference: `str 
                The reference to database child.
            data: `dict`
                The dictionary containining key values
                to set reference child to.
        """
        _data = self.clean_up(data)
        return db.reference(reference).update(_data)
    
    def clean_up(self, data: dict) -> dict:
        """ Clean up the data formatting for real-time database. """
        _data = {} 
        # create a separate dictionary 
        # that is within the format of real-time-database
        for key, value in data.items():
            if not isinstance(value, (str, bool, float, int, dict)):
                _data[key] = value.to_dict()
            else: _data[key] = value
        return _data
