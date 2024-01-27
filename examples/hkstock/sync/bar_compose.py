from dateutil import parser
from exchange_market.bar import RawBar
from utils.utils import datetime2string
from exchange_market.hkstock.hkstock_enum import Interval


class BarCompose(object):
    """ 以5min为基础，合成15min、30min、4H、日线和周线级别的k线 """
    def __init__(self):
        """Constructor"""
        self.last_dt = ""
        self.bars_day = []
        self.bars_m15 = []
        self.bars_m30 = []
        self.bars_m5 = []
        self.bars_m60 = []
        self.bars_week = []

        self.bar_4h = None
        self.bar_15m = None
        self.bar_30m = None
        self.bar_60m = None
        self.bar_day = None
        self.week_bar = None

    def get_bars(self, period):
        if period == Interval.min5.value:
            return self.bars_m5
        elif period == Interval.min15.value:
            return self.bars_m15
        elif period == Interval.min30.value:
            return self.bars_m30
        elif period == Interval.min60.value:
            return self.bars_m60
        elif period == Interval.day.value:
            return self.bars_day
        elif period == Interval.week.value:
            return self.bars_week
        else:
            return []

    def clear(self):
        self.bars_day = self.bars_day[-30:]
        self.bars_m15 = self.bars_m15[-30:]
        self.bars_m30 = self.bars_m30[-30:]
        self.bars_m60 = self.bars_m60[-30:]
        self.bars_m5 = self.bars_m5[-500:]
        self.bars_week = self.bars_week[-30:]

    def set_latest_dt(self, dt):
        self.last_dt = dt

    def get_last_dt(self):
        if not self.last_dt:
            self.last_dt = "2023-01-01 10:00:00"
        return self.last_dt

    def update_bar_5m_window(self, bar) -> None:
        """ 用5min的k线合成大级别 """
        if bar not in self.bars_m5:
            self.bars_m5.append(bar)

    def update_bar_15min_window(self, bar) -> None:
        """ 用5min的k线合成大级别 """
        if bar in self.bars_m5: return
        if not self.bar_15m:
            dt = bar.dt
            self.bar_15m = RawBar(symbol=bar.symbol, dt=dt, open=bar.open, high=bar.high, low=bar.low, close=bar.close,
                                  vol=bar.vol, exchange=bar.exchange, interval=bar.interval)
            return

        finished_bar = None
        if parser.parse(bar.dt).minute in [0, 15, 30, 45]:  # okex是16:00切换为新日线
            self.bar_15m.high = max(self.bar_15m.high, bar.high)
            self.bar_15m.low = min(self.bar_15m.low, bar.low)

            self.bar_15m.close = bar.close
            self.bar_15m.vol += int(bar.vol)
            self.bar_15m.dt = datetime2string(parser.parse(bar.dt))

            finished_bar = self.bar_15m  # 保存日线bar
            self.bar_15m = None  # 因为日线bar已经保存给finished_bar, 将日线bar设为空等新数据来就会生成新的日线bar
        # 更新 现存的day_bar
        else:
            self.bar_15m.high = max(self.bar_15m.high, bar.high)
            self.bar_15m.low = min(self.bar_15m.low, bar.low)

            self.bar_15m.close = bar.close
            self.bar_15m.vol += int(bar.vol)

        # 推送日线给on_hour_bar处理
        if finished_bar:
            self.bars_m15.append(finished_bar)

    def update_bar_30min_window(self, bar) -> None:
        """ 用5min的k线合成大级别 , 参考同花顺，(]模式"""
        if bar in self.bars_m5: return
        if not self.bar_30m:
            dt = bar.dt
            self.bar_30m = RawBar(symbol=bar.symbol, dt=dt, open=bar.open, high=bar.high, low=bar.low, close=bar.close,
                                  vol=bar.vol, exchange=bar.exchange, interval=bar.interval)
            return

        finished_bar = None
        if parser.parse(bar.dt).minute in [0, 30]:  # okex是16:00切换为新日线
            self.bar_30m.high = max(self.bar_30m.high, bar.high)
            self.bar_30m.low = min(self.bar_30m.low, bar.low)

            self.bar_30m.close = bar.close
            self.bar_30m.vol += int(bar.vol)
            self.bar_30m.dt = bar.dt

            finished_bar = self.bar_30m  # 保存日线bar
            self.bar_30m = None  # 因为日线bar已经保存给finished_bar, 将日线bar设为空等新数据来就会生成新的日线bar
        # 更新 现存的day_bar
        else:
            self.bar_30m.high = max(self.bar_30m.high, bar.high)
            self.bar_30m.low = min(self.bar_30m.low, bar.low)

            self.bar_30m.close = bar.close
            self.bar_30m.vol += int(bar.vol)

        # 推送日线给on_hour_bar处理
        if finished_bar:
            self.bars_m30.append(finished_bar)

    def update_bar_60min_window(self, bar) -> None:
        """ 用5min的k线合成大级别 , 参考同花顺，(]模式"""
        if bar in self.bars_m5: return
        if not self.bar_60m:
            dt = bar.dt
            self.bar_60m = RawBar(symbol=bar.symbol, dt=dt, open=bar.open, high=bar.high, low=bar.low, close=bar.close,
                                  vol=bar.vol, exchange=bar.exchange, interval=bar.interval)
            return

        finished_bar = None
        if parser.parse(bar.dt).strftime("%H:%M") in ["10:30", "11:30", "14:00", "15:00"]:  # 60分钟线切换时间
            self.bar_60m.high = max(self.bar_60m.high, bar.high)
            self.bar_60m.low = min(self.bar_60m.low, bar.low)

            self.bar_60m.close = bar.close
            self.bar_60m.vol += int(bar.vol)
            self.bar_60m.dt = bar.dt

            finished_bar = self.bar_60m  # 保存日线bar
            self.bar_60m = None  # 因为日线bar已经保存给finished_bar, 将日线bar设为空等新数据来就会生成新的日线bar
        # 更新 现存的day_bar
        else:
            self.bar_60m.high = max(self.bar_60m.high, bar.high)
            self.bar_60m.low = min(self.bar_60m.low, bar.low)

            self.bar_60m.close = bar.close
            self.bar_60m.vol += int(bar.vol)

        # 推送日线给on_hour_bar处理
        if finished_bar:
            self.bars_m60.append(finished_bar)

    def update_bar_day_window(self, bar) -> None:
        """ 用5min的k线合成大级别 """
        if bar in self.bars_m5: return
        if not self.bar_day:
            dt = bar.dt
            self.bar_day = RawBar(symbol=bar.symbol, dt=dt, open=bar.open, high=bar.high, low=bar.low, close=bar.close,
                                  vol=bar.vol, exchange=bar.exchange, interval=bar.interval)
            return

        finished_bar = None
        # 23:59 更新bar，生成新的日线bar
        if parser.parse(bar.dt).strftime("%H:%M") in ["15:00"]:   # 日线切换时间
            self.bar_day.high = max(self.bar_day.high, bar.high)
            self.bar_day.low = min(self.bar_day.low, bar.low)

            self.bar_day.close = bar.close
            self.bar_day.vol += int(bar.vol)
            self.bar_day.dt = parser.parse(bar.dt).strftime("%Y-%m-%d")

            finished_bar = self.bar_day  # 保存日线bar
            self.bar_day = None  # 因为日线bar已经保存给finished_bar, 将日线bar设为空等新数据来就会生成新的日线bar
        # 更新 现存的day_bar
        else:
            self.bar_day.high = max(self.bar_day.high, bar.high)
            self.bar_day.low = min(self.bar_day.low, bar.low)

            self.bar_day.close = bar.close
            self.bar_day.vol += int(bar.vol)

        # 推送日线给on_hour_bar处理
        if finished_bar:
            self.bars_day.append(finished_bar)

    def update_bar_week_window(self, bar) -> None:
        """ 用5min的k线合成大级别 """
        if bar in self.bars_m5: return
        if not self.week_bar:
            dt = bar.dt
            self.week_bar = RawBar(symbol=bar.symbol, dt=dt, open=bar.open, high=bar.high, low=bar.low, close=bar.close,
                                   vol=bar.vol, exchange=bar.exchange, interval=bar.interval)
            return

        finished_bar = None
        # 每周五更新bar，生成新的周线bar
        if parser.parse(bar.dt).weekday() == 4 and parser.parse(bar.dt).hour == 15:  # okex是16:00切换为新日线
            self.week_bar.high = max(self.week_bar.high, bar.high)
            self.week_bar.low = min(self.week_bar.low, bar.low)

            self.week_bar.close = bar.close
            self.week_bar.vol += int(bar.vol)
            self.week_bar.dt = parser.parse(bar.dt).strftime("%Y-%m-%d")

            finished_bar = self.week_bar  # 保存日线bar
            self.week_bar = None  # 因为日线bar已经保存给finished_bar, 将日线bar设为空等新数据来就会生成新的日线bar
        # 更新 现存的day_bar
        else:
            self.week_bar.high = max(self.week_bar.high, bar.high)
            self.week_bar.low = min(self.week_bar.low, bar.low)

            self.week_bar.close = bar.close
            self.week_bar.vol += int(bar.vol)

        # 推送日线给on_hour_bar处理
        if finished_bar:
            self.bars_week.append(finished_bar)
