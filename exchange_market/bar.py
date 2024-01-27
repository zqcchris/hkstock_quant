# coding: utf-8
import pandas as pd


class TAXX(object):
    """ 将行情的指标分为两类：1、递归类；2、非递归类。递归类指标需要缓存在bar中，这样才能更精确 """
    def __init__(self):
        # MACD计算
        self.ema12 = 0
        self.ema26 = 0
        self.diff = 0
        self.dea = 0
        self.macd = 0
        self.delta = 0
        self.mavg21 = 0

        # 用于ema策略使用
        self.ema155 = 0
        self.ema300 = 0

        # 计算RSI6
        self.up_sum = 0
        self.down_sum = 0
        self.up_avg6 = 0
        self.down_avg6 = 0
        self.rsi6 = 0


class RawBar(object):
    """原始K线元素"""
    def __init__(self, symbol, dt, open, high, low, close, vol=None, interval=None, exchange=""):
        self.exchange = exchange
        self.symbol = symbol
        self.dt = dt
        self.open = open
        self.close = close
        self.high = high
        self.low = low
        self.vol = vol
        self.interval = interval
        self.taxx = TAXX()

    def __str__(self):
        return ", ".join([self.symbol, self.dt, str(self.open), str(self.high), str(self.low), str(self.close),
                          str(self.vol)])

    def to_dict(self):
        return self.__dict__


if __name__ == '__main__':
    rb = RawBar(exchange="hkstock", symbol="000001", dt="2023-05-25 14:35:00", open="23", high="25", low="20", close="24")
    p = rb.to_dict()
    del p["exchange"]
    print(p)
    x = pd.DataFrame([p])
    print(x)
