from abc import abstractmethod
import firebase_admin, uuid
from firebase_admin import db    

class Database():
    """ A base abstract database class. """
    def __init__(self):
        """ Will initialize a uuid. """
        self.id = str(uuid.uuid4())
    @abstractmethod
    def get(self):
        """ Abstract method that retrieves data. """
        raise NotImplementedError()
    @abstractmethod
    def update(self):
        """ Abstract method that updates data. """
        raise NotImplementedError()
    @abstractmethod
    def set(self):
        """ Abstract method that sets data. """
        raise NotImplementedError()
    @abstractmethod
    def clean_up(self, data: dict) -> any:
        """ Abstract method that cleans up data. """
        raise NotImplementedError()

class KaiRealtimeDatabase(Database):
    """ This class handles read/write to KepingAI's
        real-time database. 
            
        Note 
        ----
        *This database's usage is purely meant to save small-medium
        states of data. This is not a usage for archiving database.*
    """
    def __init__(self,
                 database_url: str = 'https://keping-ai-continuum-default-rtdb.firebaseio.com/'):
        """
            A dedicated real-time database is created for both dev and prod usage.
            All CRUD services is conducted through this class.

            Parameters
            ----------
            database_url: `str`
                The url to the real-time database, default is link spe
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
    
    def listen(self, reference: str, callback: callable) -> db.ListenerRegistration:
        """ Will subscribe to specific path asynchronously. 

            Warning
            --------
            *Only use this for small size data. Also this
            event fires every hour and dumps the entire
            child data referenced to it.*

            Parameters
            ----------
            reference: `str`
                The path reference database.
            callback: `callable`
                The callback function to call if changes made.
            
            Returns
            -------
            `db.ListenerRegistration`
                The listener registration object, close when complete.
        """
        return db.reference(reference).listen(callback)
    
    def clean_up(self, data: dict) -> dict:
        """ Clean up the data formatting for real-time database. 

            Parameters
            ----------
            data: `dict`
                The raw data to be formatted correctly.
            
            Returns
            -------
            `dict`
                Casted data to specific real-time database format.
        """
        _data = {} 
        # create a separate dictionary 
        # that is within the format of real-time-database
        for key, value in data.items():
            if not isinstance(value, (str, bool, float, int, dict)):
                _data[key] = value.to_dict()
            else: _data[key] = value
        return _data
