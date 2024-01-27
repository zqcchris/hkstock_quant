#!/usr/bin/env python
import os
import sys
sys.path.append(os.getcwd())
import logging
import traceback
from datetime import datetime, timedelta
from examples.hkstock.ca_mgr import CAMgr
from utils.utils import string2mil, dt2ts_mil
from exchange_market.hkstock.hkstock_enum import Interval
from utils.cache.cache import myredis, cache_delete
from examples.hkstock.util.stock_utils import read_stocks
from utils.mysql.connect import SQLUtils
base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ca_mgr = CAMgr()
# cache = redis
cache = {}
symbols = read_stocks()
sql_utils = SQLUtils()
strategy_names = ["topx"]


def parent_process():
    interval = Interval.min5.value
    while True:
        try:
            crypto_chg = sql_utils.get_abnormal_change(interval=Interval.week.value)
            top10_symbols = [i[0] for i in crypto_chg][0:50]
            for idx, stock in enumerate(symbols):
                try:
                    name = stock.get("name")
                    symbol = stock.get("symbol")
                    print("current symbol:{},{}/{}".format(symbol, idx, len(symbols)))
                    bars_generator = ca_mgr.get_bars_generator(symbol)
                    position_cache = ca_mgr.get_position_cache(symbol=symbol, logger=logger)
                    bars_5m, prev_dt = bars_generator.get_latest_bar(code=symbol)
                    if not bars_5m:
                        print("{}: 无新数据，等待更新...，prev_dt：{}; {}/{}".format(symbol, prev_dt, idx, len(symbols)))
                    else:
                        for bar_5m in bars_5m:
                            bars_generator.compose(bar_5m=bar_5m, mode="production")
                            position_cache.cache_update(symbol=symbol, bar_5m=bar_5m, cache=cache)
                            if symbol not in top10_symbols: continue
                            # 48H前的数据只用于k线合成，不参与策略运行
                            if dt2ts_mil(datetime.utcnow() + timedelta(hours=8)) - string2mil(bar_5m.dt) > 172800000: continue
                            print("{}: prev time: {}, cur time: {}; progress: {}/{}".format(symbol, prev_dt, bar_5m.dt, idx, len(symbols)))
                            for strategy_name in strategy_names:
                                strategy = ca_mgr.get_ca(symbol=symbol, logger=logger, strategy_name=strategy_name, cache=cache)
                                strategy.run(symbol=symbol, bar_5m=bar_5m, step_interval=interval)
                                ca_mgr.set_ca(symbol=symbol, ca=strategy)
                        ca_mgr.set_bars_generator(symbol=symbol, bars_generator=bars_generator)
                except:
                    traceback.print_exc()
        except:
            traceback.print_exc()


if __name__ == "__main__":
    if os.path.exists(base_dir + "/data/hkstock.log"):
        os.remove(base_dir + "/data/hkstock.log")
    logger = logging.getLogger(base_dir + "/data/hkstock.log")
    logger_level = logging.DEBUG
    logger.setLevel(logging.DEBUG)
    rh = logging.FileHandler(base_dir + "/data/hkstock.log")
    fm = logging.Formatter("%(asctime)s  %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S")
    rh.setFormatter(fm)
    logger.addHandler(rh)

    pushed = []
    parent_process()
