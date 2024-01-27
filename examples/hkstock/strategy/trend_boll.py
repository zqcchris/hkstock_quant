from pandas import Series
from ta.trend import MACD

from ta.trend import ema_indicator, EMAIndicator
from ta.volatility import BollingerBands


class Strategy(object):
    def __init__(self):
        pass

    @staticmethod
    def recog_trend_exec(bh, bars, bl):
        """ 在历史k线中寻找2次突破布林上轨的日期，且两次突破布林上轨区间内，不允许有突破下轨的走势 """
        signal_dt = []
        for i in range(len(bars)):
            cur_index = len(bars) - 2 - i
            # 一旦寻找到2次涨破布林上轨的走势，那么说明趋势行情已来
            # if len(signal_dt) == 2:
            #     return True, signal_dt
            if bars[cur_index].high > bh[cur_index]:
                return False, signal_dt
            elif bars[cur_index].low < bl[cur_index]:
                return True, signal_dt
                # signal_dt.append(bars[cur_index-i].dt)
        return False, signal_dt

    def trend_recognize(self, bars):
        closes = Series([float(b.close) for b in bars])
        boll = BollingerBands(close=closes, window=21)
        bh = boll.bollinger_hband().values
        bl = boll.bollinger_lband().values
        if bars[-1].high < bh[-1]:
            return False, None
        else:
            flag, signal_dt = self.recog_trend_exec(bh=bh, bars=bars, bl=bl)
            if flag:
                return flag, signal_dt
        return False, None
