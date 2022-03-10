import ccxt
from kaiborg.exchanges import get_exchange
from ccxt.base.exchange import Exchange
import logging
import numpy as np


class Arbitrage:
    def __init__(self,
                 exchange: Exchange,
                 ideal_base: str,
                 actual_base: str,
                 alt_bases: list,
                 min_profit: float):
        self.exchange = exchange
        self.ideal_base = ideal_base
        self.actual_base = actual_base
        self.alt_bases = alt_bases
        self.min_profit = min_profit

    def scout_prices(self):
        raise NotImplementedError


class BinanceArbitrage(Arbitrage):
    def __init__(self,
                 exchange: Exchange,
                 ideal_base: str,
                 actual_base: str,
                 alt_bases: list,
                 min_profit: float):
        super(BinanceArbitrage, self).__init__(exchange=exchange,
                                               ideal_base=ideal_base,
                                               actual_base=actual_base,
                                               alt_bases=alt_bases,
                                               min_profit=min_profit)

    def scout_prices(self):
        actual_pairs = [base + f"/{self.actual_base.upper()}" for base in self.alt_bases]
        ideal_pairs = [base + f"/{self.ideal_base.upper()}" for base in self.alt_bases]

        while True:
            # case 1, if the alt_btc / bnb_btc > alt_bnb
            alt_actual_prices = self.exchange.fetch_bids_asks(symbols=actual_pairs)
            actual_ideal_price = self.exchange.fetch_bids_asks(symbols=[f"{self.actual_base}/{self.ideal_base}"])
            alt_ideal_prices = self.exchange.fetch_bids_asks(symbols=ideal_pairs)

            alt_actual_ask = np.array([alt_actual_prices[pair]['ask'] for pair in actual_pairs])
            alt_ideal_bid = np.array([alt_ideal_prices[pair]['bid'] for pair in ideal_pairs])
            actual_ideal_ask = actual_ideal_price[f'{self.actual_base}/{self.ideal_base}']['ask']

            efficient_price_1 = alt_ideal_bid / actual_ideal_ask

            # log if there is a potential 0.3% profit
            mask_1 = (efficient_price_1 / alt_actual_ask) > 1 + (self.min_profit/100)
            percent_difference_1 = (efficient_price_1 - alt_actual_ask) / alt_actual_ask * 100
            potential_alts_1 = np.array(self.alt_bases)[mask_1]

            if np.sum(mask_1) > 0:
                logging.info(f"Case 1. Potential coins: {potential_alts_1}, "
                             f"percent_difference: {percent_difference_1}")

            # case 2, if the alt_btc / bnb_btc < alt_bnb
            alt_actual_bid = np.array([alt_actual_prices[pair]['bid'] for pair in actual_pairs])
            alt_ideal_ask = np.array([alt_ideal_prices[pair]['ask'] for pair in ideal_pairs])
            actual_ideal_bid = actual_ideal_price[f'{self.actual_base}/{self.ideal_base}']['bid']

            efficient_price_2 = alt_ideal_ask / actual_ideal_bid

            # log if there is a potential 0.3% profit
            mask_2 = (efficient_price_2 / alt_actual_bid) < 1 + (self.min_profit/100)
            percent_difference_2 = (efficient_price_2 - alt_actual_bid) / alt_actual_bid * 100
            potential_alts_2 = np.array(self.alt_bases)[mask_2]

            if np.sum(mask_2) > 0:
                logging.info(f"Case 2. Potential coins: {potential_alts_2}, "
                             f"percent_difference: {percent_difference_2}")
