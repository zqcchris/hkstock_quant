from pandas import Series
from ta.trend import MACD


class Strategy(object):
    def __init__(self):
        pass

    def trend_recognize(self, bars):
        closes = Series([float(b.close) for b in bars])
        macd = MACD(close=closes)
        dea = macd.macd_signal().values
        diff = macd.macd().values
        if diff[-1] < dea[-1] and diff[-2] >= dea[-2]:
            return True, bars[-1].dt
        else:
            return False, bars[-1].dt
