import uuid, logging
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, Union

from numpy.core.fromnumeric import take

class SignalStatus(Enum):
    NEW = "NEW"
    OPEN = "OPEN"
    UPDATING = "UPDATING"
    CLOSED = "CLOSED"
    COMPLETED = "COMPLETED"
    EXPIRED = "EXPIRED"
    
    def __str__(self):
        return str(self.value)

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, str): 
            return str(self.value).upper() == __o.upper()
        return super().__eq__(__o)

class Signal():
    """ A signal class that embodies a trigger generated from
        layer 1 & layer 2 interaction.
    """
    def __init__(self,
                 base: str,
                 quote: str,
                 take_profit: float,
                 spread: float,
                 purchase_price: float,
                 last_price: float,
                 direction: int,
                 callback: callable,
                 n_tick_forward: int,
                 buffer: int = 24,
                 realized_profit : float = 0.0,
                 id: str = None,
                 created_at: int = None,
                 expired_at: int = None,
                 status: SignalStatus = SignalStatus.NEW,
                 expiration_minutes: Optional[Union[int, float]] = None):
        """ Initializing a signal class.

            Parameters
            ----------
            base: `str`
                The base symbol.
            quote: `str`
                The quote symbol.
            take_profit: `float`
                The take profit in percentage.
            spread: `float`
                The spread predicted from layer 2.
            purchase_price: `float`
                The price signal was entered.
            last_price: `float`
                The last price when entered.
            direction: `int`
                The direction of signal 1 = long, 0 = short.
            callback: `callable`
                A callback function to call if signal closed.
            n_tick_forward: `int`
                The n tick forward signal expected to close.
            buffer: `int`
                The buffer n tick forward.
            realized_profit: `float`
                The realized profit after signal closed.
            id: `str`
                The unique id.
            created_at: `int`
                The UTC timestamp of signal creation.
            expired_at: `int`
                The UTC timestamp signal to be expired.
            status: `SignalStatus`
                The current status.
            expiration_minutes: `Optional[Union[int, float]]`
                The expiration duration in minutes, if None: use the expiration
                calculation using the n_tick_forward and buffer.
        """
        self.id = id if id else str(uuid.uuid4())
        self.base = base
        self.quote = quote
        self.symbol = f"{base}{quote}".upper()
        self.take_profit = take_profit
        self.spread = spread
        self.purchase_price = purchase_price
        mult = (1 + (take_profit / 100)) if direction == 1 else (1 - (take_profit / 100))
        self.exit_price = purchase_price * mult
        self.last_price = last_price
        self.direction = direction
        self.n_tick_forward = n_tick_forward
        self.callback = callback
        self._status = status
        self.buffer = buffer
        self.realized_profit = realized_profit
        if expiration_minutes is None:
            expiration_minutes = (45 * (n_tick_forward + buffer))
        self.created_at = datetime.utcnow().timestamp() if not created_at else created_at
        self.expired_at = ((datetime.fromtimestamp(self.created_at) + timedelta(
            minutes=expiration_minutes)).timestamp()
            if not expired_at else expired_at)
        logging.info(f"[signal] created! symbol:{self.symbol}, "
            f"spread: {self.spread}, ttp: {self.take_profit}, direction: {self.direction}")
        self.open()
    
    @property
    def status(self) -> SignalStatus:
        """ The real-time status of the signal. """
        return self._status

    def open(self):
        """ Will change the signal status to OPEN """
        self._status = SignalStatus.OPEN
    
    def close(self):
        """ Will close the signal immediately. """
        self._status = SignalStatus.CLOSED
        self.callback(self)
    
    def is_open(self) -> bool:
        """ Check if signal is still open.
        
            Returns
            -------
            `bool` 
                Returns `True` if signal is still open.
        """
        return self._status == SignalStatus.OPEN
        
    def update(self, last_price: float) -> SignalStatus:
        """ Will update the signal's status based upon the current
            last price of the ticker.

            Parameters
            ----------
            last_price: `float`
                The last price of the ticker.
            
            Returns
            -------
            `SignalStatus`
                Will return the update status of the signal.
        """
        # update the status of the signal
        # this will prevent duplicate runs
        self._status = SignalStatus.UPDATING
        self.last_price = last_price
        # if last price have gone above the exit price
        if self.direction == 1 and last_price >= self.exit_price:
            self.realized_profit = round(abs(self.last_price - 
                self.purchase_price) / self.purchase_price * 100, 4)
            logging.info(f"[completed] signal - symbol: {self.symbol}, "
                f"direction: {self.direction}, realized-profit: {self.realized_profit}%")
            self._status = SignalStatus.COMPLETED
            self.callback(self)
        # if last price have gone above the exit price
        elif self.direction == 0 and last_price <= self.exit_price:
            self.realized_profit = round(abs(self.last_price - 
                self.purchase_price) / self.purchase_price * 100, 4)
            logging.info(f"[completed] signal - symbol: {self.symbol}, "
                f"direction: {self.direction}, realized-profit: {self.realized_profit}%")
            self._status = SignalStatus.COMPLETED
            self.callback(self)
        # check if time has surpassed expected expired date
        elif datetime.utcnow().timestamp() >= self.expired_at:
            self._status = SignalStatus.EXPIRED
            logging.info(f"[expired] signal - symbol: {self.symbol}, "
                f"direction: {self.direction}, expiration: {self.expired_at}")
            self.callback(self) 
        # signal is updated and back to open
        else: self.open()
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
            take_profit=self.take_profit,
            spread=self.spread,
            purchase_price=self.purchase_price,
            exit_price=self.exit_price,
            last_price=self.last_price,
            direction=self.direction,
            n_tick_forward=self.n_tick_forward,
            status=str(self._status),
            buffer=self.buffer,
            realized_profit=self.realized_profit,
            created_at=self.created_at,
            expired_at=self.expired_at
        )

def init_signal_from_rtd(data: dict, callback: callable) -> Signal:
    """ Will initialize a Signal class object
        from real-time database.

        Parameters
        ----------
        data: `dict`
            The data to be converted to Signal instance.
        callback: `callable`
            A callback for updating the signal.
        
        Returns
        -------
        `Signal`
            A signal instance containing signal data.
    """
    return Signal(
        base=data['base'],
        quote=data['quote'],
        take_profit=data['take_profit'],
        spread=data['spread'],
        purchase_price=data['purchase_price'],
        last_price=data['last_price'],
        direction=data['direction'],
        callback=callback,
        n_tick_forward=data['n_tick_forward'],
        created_at=data['created_at'],
        buffer=data['buffer'],
        realized_profit=data['realized_profit'],
        id=data['id'],
        expired_at=data['expired_at'],
        status=SignalStatus(data['status'].upper()))