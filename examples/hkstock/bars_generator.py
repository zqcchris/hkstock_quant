# Astock Bars Generator
import os
import sys
sys.path.append(os.getcwd())
base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(base_dir)
import copy
import json
import requests
from utils.cache.cache import myredis
from exchange_market.bar import RawBar
from exchange_market.hkstock.hkstock_enum import Interval
from examples.hkstock.sync.bar_compose import BarCompose


class BarsGenerator(object):
    def __init__(self, cache=myredis, code="000001", mode="debug"):
        """
        :param: symbol: 股票代码，like "000001", "603127", "300691"
        :param: mode: choice of [debug, production], debug和production模式对应不同的数据初始化的数据源，
                      debug使用本地行情文本文件进行数据初始化， production使用网络数据作为数据初始化的数据源
        :param: cache：全局缓存，通常为redis，用于虚拟仓的缓存
        """
        self.cache = cache
        self.mode = mode
        self.code = code
        self.bars_compose = BarCompose()

    @staticmethod
    def raw_bars_generator(datas, period=Interval.min5.value, separator=None):
        bars = []
        for data in datas:
            if separator: data = data.split(separator)
            rb = RawBar(exchange="hkstock", symbol=data[0].strip(), dt=data[1].strip(), open=float(data[2]),
                        high=float(data[3]), low=float(data[4]), close=float(data[5]), vol=float(data[6]), interval=period)
            bars.append(copy.deepcopy(rb))
        return bars

    def get_bars_from_api(self, code, period, **kwargs):
        """
        :param: market: choice of ["SZ", "SH"]
        :param: code: 股票代码，如：000063
        :param: limit: string, 最大返回10000，也就是一个月的5min的数据
        """
        post_data = {"code": code, "period": period, **kwargs}
        response = requests.post("http://127.0.0.1:8000/hkstock", data=post_data)
        rsp = json.loads(response.text)
        datas = rsp.get('data', [])
        datas.reverse()
        bs = self.raw_bars_generator(datas=datas, period=Interval.min5.value, separator=None)
        if len(bs) > 0 and period == Interval.min5.value:
            self.bars_compose.set_latest_dt(bs[-1].dt)
        return bs

    def get_latest_bar(self, symbol="000001", period=Interval.min5.value, **kwargs):
        """
        :param: code: 股票代码，如：000001、300200
        :param: market: 市场类型，choice of ["SZ", "SH"]
        :param: bar, 枚举类型
        """
        prev_dt = self.bars_compose.get_last_dt()
        post_data = {"code": symbol, "period": period, "start_time": prev_dt, "limit": "5", **kwargs}
        response = requests.post("http://127.0.0.1:8000/hkstock", data=post_data)
        rsp = json.loads(response.text)
        datas = rsp.get('data', [])
        datas.reverse()
        bars = self.raw_bars_generator(datas=datas, period=Interval.min5.value, separator=None)
        if len(bars) > 0:
            self.bars_compose.set_latest_dt(bars[-1].dt)
            return bars, prev_dt
        return None, prev_dt

    def get_bars(self, period, symbol, end_time, limit=30, **kwargs):
        """
        :param: market: choice of ["SZ", "SH"]
        :param: code: 股票代码，如：000063
        :param: limit: string, 最大返回1500，也就是一个月的5min的数据
        """
        if self.mode == "debug":
            return self.get_bars_from_cache(period=period, symbol=symbol, limit=limit, **kwargs)
        code, market = symbol.split("_")
        post_data = {"market": market, "code": code, "period": period, "end_time": end_time, "limit": limit, **kwargs}
        response = requests.post("http://127.0.0.1:8000/hkstock", data=post_data)
        rsp = json.loads(response.text)
        datas = rsp.get('data', [])
        datas.reverse()
        bs = self.raw_bars_generator(datas=datas, period=period, separator=None)
        return bs

    def get_bars_from_cache(self, period, symbol, limit, **kwargs):
        """
        :param: market: choice of ["SZ", "SH"]
        :param: code: 股票代码，如：000063
        :param: limit: string, 最大返回1500，也就是一个月的5min的数据
        """
        if period == Interval.min5.value:
            return self.bars_compose.bars_m5[-limit:]
        elif period == Interval.min15.value:
            return self.bars_compose.bars_m15[-limit:]
        elif period == Interval.min30.value:
            return self.bars_compose.bars_m30[-limit:]
        elif period == Interval.min60.value:
            return self.bars_compose.bars_m60[-limit:]
        elif period == Interval.day.value:
            return self.bars_compose.bars_day[-limit:]
        elif period == Interval.week.value:
            return self.bars_compose.bars_week[-limit:]

    def compose(self, bar_5m=None):
        """ 注意k线合成顺序必须是大级别优先，5分钟级别最后执行，顺序不得调整 """
        self.bars_compose.update_bar_week_window(bar_5m)
        self.bars_compose.update_bar_day_window(bar_5m)
        self.bars_compose.update_bar_60min_window(bar_5m)
        self.bars_compose.update_bar_30min_window(bar_5m)
        self.bars_compose.update_bar_15min_window(bar_5m)
        self.bars_compose.update_bar_5m_window(bar_5m)

    @staticmethod
    def get_price_precision():
        return 2


if __name__ == '__main__':
    bars_generator = BarsGenerator(code="00001", mode="production")
    bs = bars_generator.get_bars_from_api(code="00001", period=Interval.min5.value)
    # rsp = bars_generator.get_latest_bar(symbol="000001")
    print(bs)
    # for b in bars_generator.bars_m5:
    #     print(b)
