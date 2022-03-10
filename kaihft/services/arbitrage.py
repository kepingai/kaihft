from kaihft.arbitrage import BinanceArbitrage
from kaiborg.exchanges import get_exchange


def main(ideal_base):
    credentials = {
        "api_key": "L7lWcgoJkP2OPoEB4Ju2ftfBEoZI1yzrejbEYZKx3zmp9nPiuWJNmYJMCW6nQpLk",
        "api_secret": "pvThTUa0ucYGilwO3qoti9o8lmWHSa7RolOwYODlKEGuIzT4RUTwymHaZcuIF63n"
    }

    exchange = get_exchange(exchange='binance', credentials=credentials)
    alt_bases = ["AVAX", "ADA", "LUNA", "SOL", "MATIC", "DOT", "ATOM", "NEAR", "LTC", "LINK",
                 "UNI", "ETC", "MANA", "FTM", "SAND", "WAVES", "EGLD"]
    arbitrage = BinanceArbitrage(exchange=exchange,
                                 ideal_base=ideal_base,
                                 actual_base="BNB",
                                 alt_bases=alt_bases,
                                 min_profit=0.3)
    arbitrage.scout_prices()
