import uuid
from datetime import datetime, timedelta
from enum import Enum

class SignalStatus(Enum):
    NEW = "NEW"
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    COMPLETED = "COMPLETED"
    EXPIRED = "EXPIRED"
    def __str__(self):
        return str(self.value)

class Signal():
    def __init__(self,
                 base: str,
                 quote: str,
                 spread: float,
                 purchase_price: float,
                 last_price: float,
                 direction: int,
                 callback: callable,
                 n_tick_forward: int,
                 buffer: int = 3,
                 realized_profit : float = 0.0,
                 id: str = uuid.uuid4(),
                 created_at: datetime = datetime.utcnow(),
                 expired_at: datetime = None,
                 status: SignalStatus = SignalStatus.NEW):
        self.id = id
        self.base = base
        self.quote = quote
        self.symbol = f"{base.upper()}{quote.upper()}"
        self.spread = spread
        self.purchase_price = purchase_price
        self.exit_price = purchase_price * (1 + spread)
        self.last_price = last_price
        self.direction = direction
        self.n_tick_forward = n_tick_forward
        self.callback = callback
        self._status = status
        self.buffer = buffer
        self.realized_profit = realized_profit
        self.created_at = created_at
        self.expired_at = (self.created_at + timedelta(
            minutes=(15 * (n_tick_forward + buffer))) 
            if not expired_at else expired_at)
    
    @property
    def status(self) -> SignalStatus:
        return self._status

    def open(self):
        """ Will change the signal status to OPEN """
        self._status = SignalStatus.OPEN
    
    def close(self):
        """ Will close the signal immediately. """
        self._status = SignalStatus.CLOSED
        self.callback(self)
    
    def update(self, last_price: float) -> SignalStatus:
        """ Will update the signal's status based upon the current
            last price of the ticker.

            Parameters
            ----------
            last_price: `float`
                The last price of the ticker.
            
            Return
            ------
            `SignalStatus`
                Will return the update status of the signal.
        """
        # if last price have gone above the exit price
        if last_price >= self.exit_price:
            self._status = SignalStatus.COMPLETED
            self.callback(self)
        # check if time has surpassed expected expired date
        if datetime.utcnow() >= self.expired_at:
            self._status = SignalStatus.EXPIRED
            self.callback(self) 
        return self._status
    
    def to_dict(self) -> dict:
        """ Returns
            -------
            `dict`
                A dictionary formatted signal.
        """
        return dict(
            id=self.id,
            base=self.base,
            quote=self.quote,
            symbol=self.symbol,
            spread=self.spread,
            purchase_price=self.purchase_price,
            exit_price=self.exit_price,
            last_price=self.last_price,
            direction=self.direction,
            n_tick_forward=self.n_tick_forward,
            callback=str(self.callback),
            status=str(self._status),
            buffer=self.buffer,
            realized_profit=self.realized_profit,
            created_at=str(self.created_at),
            expired_at=str(self.expired_at)
        )

def init_signal_from_rtd(data: dict) -> Signal:
    """ Will initialize a Signal class object
        from real-time database.

        Parameters
        ----------
        data: `dict`
            The data to be converted to Signal instance.
        
        Returns
        -------
        `Signal`
            A signal instance containing signal data.
    """
    return Signal(
        base=data['base'],
        quote=data['quote'],
        spread=data['spread'],
        purchase_price=data['purchase_price'],
        last_price=data['last_price'],
        direction=data['direction'],
        callback=data['callback'],
        n_tick_forward=data['n_tick'],
        created_at=data['created_at'],
        buffer=data['buffer'],
        realized_profit=data['realized_profit'],
        id=data['id'],
        expired_at=data['expired_at'],
        status=SignalStatus(data['status'].upper()))