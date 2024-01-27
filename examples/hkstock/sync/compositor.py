"""
由于股票数量巨大，定时任务监控时，如果一次性同步最新的行情数据，会被封禁ip，所以必须分批进行。
对5000+股票，分10批，每一次大概同步500个股票的5min行情数据，同步后
"""
import os, sys
import traceback

sys.path.append(os.getcwd())
base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(base_dir)
import pandas as pd
from pytdx.hq import TdxHq_API
from sqlalchemy import create_engine, text
from exchange_market.hkstock.hkstock_enum import Interval
from examples.hkstock.bars_generator import BarsGenerator
from examples.hkstock.util.stock_utils import read_stocks
api = TdxHq_API()


class Compositor(object):
    def __init__(self):
        self.host = "13.214.135.34"
        self.user = 'root'
        self.password = "zqc*32#fudan"
        self.database = "astock_market"
        self.engine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset=utf8".format(self.user, self.password, self.host, self.database))

        self.bars_generators = {}
        for idx, stock in enumerate(stocks):
            print("初始化数据: {}/{}".format(idx, len(stocks)))
            symbol = stock.get("symbol")
            market = str.upper(symbol[0:2])
            code = symbol[2:]
            bars_generator = BarsGenerator(cache=cache, code=code, market=market, mode="production")
            bars_m5 = bars_generator.get_bars_from_api(market=market, code=code, period=Interval.min5.value)
            for bar_5m in bars_m5:
                bars_generator.compose(bar_5m)
            self.bars_generators[code] = bars_generator
            self.sync(code=code, market=market)
        print("数据初始化完成，开始同步新数据...")

    def sync(self, code, market):
        bars_generator = self.bars_generators.get(code)
        periods = [Interval.min15.value, Interval.min30.value, Interval.min60.value, Interval.day.value, Interval.week.value]
        for period in periods:
            bars = bars_generator.bars_compose.get_bars(period)
            if not bars: continue
            self.db_sync(bars=bars, period=period, market=market, code=code)
        bars_generator.bars_compose.clear()
        self.bars_generators[code] = bars_generator

    def db_sync(self, code, market, period, bars):
        if period == Interval.min15.value:
            table_name = "_".join([code, market, "15m"])
        elif period == Interval.min30.value:
            table_name = "_".join([code, market, "30m"])
        elif period == Interval.min60.value:
            table_name = "_".join([code, market, "60m"])
        elif period == Interval.day.value:
            table_name = "_".join([code, market, "1d"])
        elif period == Interval.week.value:
            table_name = "_".join([code, market, "1w"])
        else:
            print("k compose error！")
            return

        with self.engine.connect() as connection:
            result = connection.execute(text("select symbol, dt from {} order by dt desc limit 10;".format(table_name)))
            records = result.fetchall()
            if not records:
                last_date = "2020-06-20 10:00"
            else:
                last_date = records[0][1]

            result = []
            for b in bars:
                b_dict = b.to_dict()
                if "exchange" in b_dict: del b_dict["exchange"]
                if "interval" in b_dict: del b_dict["interval"]
                result.append(b.to_dict())
            df = pd.DataFrame(result)
            order = ["symbol", "dt", "open", "high", "low", "close", "vol"]
            df = df[order]
            df = df[df["dt"] > last_date]
            if df.empty: return
            print("{}: sync {} data in to {}, last_date is: {}".format(code, period, table_name, last_date))
            df.to_sql(name=table_name, con=connection, if_exists="append", index=False)

    def compose(self, code, market):
        bars_generator = self.bars_generators.get(code)
        bars_5m, prev_dt = bars_generator.get_latest_bar(code=code, market=market, period=Interval.min5.value)
        if not bars_5m:
            print("{}: 无新数据合成，等待更新, 上一次更新为：{}".format(code, prev_dt))
        else:
            for bar_5m in bars_5m:
                print("{}: 正在合成k线, 当前时间：{}".format(code, bar_5m.dt))
                bars_generator.compose(bar_5m=bar_5m)
            self.bars_generators[code] = bars_generator
            self.sync(code=code, market=market)


if __name__ == '__main__':
    cache = {}
    stocks = read_stocks()
    compositor = Compositor()

    while True:
        for stock in stocks:
            symbol = stock.get("symbol")
            name = stock.get("name")
            market = str.upper(symbol[0:2])
            code = symbol[2:]
            compositor.compose(code=code, market=market)
