from kaihft.engines import Signal
from kaihft.engines.signal import SignalStatus

def test_signal():
    def close_signal(signal):
        assert signal.status != SignalStatus.OPEN
    take_profit = 0.351642
    last_price = 294
    # initialize a long signal example
    signal = Signal(
        base="AAVE",
        quote="USDT",
        take_profit=take_profit,
        spread=0.98355701,
        purchase_price=295.39,
        last_price=last_price,
        direction=1,
        callback=close_signal,
        n_tick_forward=8)
    # test signal status
    assert signal.is_open()
    # test signal update price constant
    signal.update(last_price)
    assert signal.is_open()
    # test signal update price above take profit
    # this will trigger callback above and esure signal is closed
    profit_price = last_price * (1 + (take_profit/100))
    signal.update(profit_price)
    
    # initialize a long signal example
    last_price = 3753.62
    signal = Signal(
        base="ETH",
        quote="USDT",
        take_profit=take_profit,
        spread=1.29756629,
        purchase_price=3753.62,
        last_price=last_price,
        direction=0,
        callback=close_signal,
        n_tick_forward=8)
    # test signal status
    assert signal.is_open()
    # test signal update price constant
    signal.update(last_price)
    assert signal.is_open()
    # test signal update price above take profit
    # this will trigger callback above and esure signal is closed
    profit_price = last_price * (1 - (take_profit/100))
    signal.update(profit_price)
