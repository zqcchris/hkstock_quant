import os
import sys
sys.path.append(os.getcwd())
from utils.mysql.connect import SQLUtils
from examples.hkstock.util.stock_utils import read_stocks
from exchange_market.hkstock.hkstock_enum import Interval
from examples.hkstock.bars_generator import BarsGenerator
from examples.hkstock.strategy.open_rank_prod import Strategy
from utils.utils import string2mil
base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


if __name__ == '__main__':
    cache = {}
    sql_utils = SQLUtils(mode="debug")
    stocks = read_stocks()
    symbol = "00001"
    code, market = symbol.split("_")
    for stock in stocks:
        code = stock.get("code")
        market = stock.get("market")
        symbol = "_".join([code, market])
        step_interval = Interval.min5.value
        bars_generator = BarsGenerator(cache=cache, code=code, mode="debug")
        bars_5m = bars_generator.get_bars_from_api(market=market, code=code, period=Interval.min5.value)

        strategy = Strategy(symbol=symbol, bars_generator=bars_generator, cache=cache, desert_interval=Interval.min30.value,
                            mode="debug", sql_utils=sql_utils, critic_interval=Interval.day.value)

        for idx, bar_5m in enumerate(bars_5m):
            bars_generator.compose(bar_5m=bar_5m)
            # if string2mil(bar_5m.dt) < string2mil("2023-01-01 00:00:00"): continue
            print("cur time:{}, cur symbol:{}".format(bar_5m.dt, code))
            strategy.run(symbol=symbol, bar_5m=bar_5m, step_interval=step_interval)
