import sys, os
sys.path.append(os.getcwd())
from pandas import Series
from ta.volatility import BollingerBands
from examples.hkstock.strategy.dd_close import Close
from exchange_market.hkstock.hkstock_enum import PositionSide, Interval
from utils.utils import string2mil, dt2ts_mil, string2datetime
from utils.cache.cache import cache_set_object, cache_get_object, cache_delete


class PositionCache(object):
    """ 实现虚拟仓的sl、pending、tp状态更新，其中sl、pending状态更新要顺带更新数据库 """
    def __init__(self, bars_generator, logger=None, precision=4, sql_utils=None):
        self.bars_generator = bars_generator
        self.logger = logger
        self.precision = precision
        self.close_strategy = Close(bars_generator=bars_generator)
        self.sql_utils = sql_utils

    @staticmethod
    def position_minmax(position, bar_5m, cache):
        """
        这里要实时更新两套minmax，第一套是open_minmax，记录从max_tp，第二套是close_minmax，在移动止盈模式下，记录委托单的触发情况
        """
        if not position: return
        status = position.get_status()
        if status != "pending": return
        open_time = position.get_open_time()
        mode = position.get_mode()
        uuid = position.get_uuid()
        step_interval = position.get_step_interval()
        if mode in ["debug", "init", "production"]:
            if step_interval == Interval.min5.value:
                if string2mil(bar_5m.dt) - string2mil(open_time) < 5 * 60000: return position
            elif step_interval == Interval.min15.value:
                if string2mil(bar_5m.dt) - string2mil(open_time) < 15 * 60000: return position
            elif step_interval == Interval.min30.value:
                if string2mil(bar_5m.dt) - string2mil(open_time) < 30 * 60000: return position
            else:
                if string2mil(bar_5m.dt) - string2mil(open_time) < 5 * 60000: return position

        open_min, open_max = position.get_open_minmax()
        if bar_5m.low < open_min:
            open_min = bar_5m.low
        if bar_5m.high > open_max:
            open_max = bar_5m.high
        position.set_open_minmax(open_min, open_max)
        # print("开单时间: {}, 当前时间: {}, min-max: {}-{}".format(open_time, bar_5m.dt, open_min, open_max))

        # schedule_time = position.get_schedule_time()
        # if isinstance(schedule_time, str): schedule_time = string2mil(schedule_time)
        close_min, close_max = position.get_close_minmax()
        if bar_5m.low < close_min:
            close_min = bar_5m.low
        if bar_5m.high > close_max:
            close_max = bar_5m.high
        position.set_close_minmax(close_min, close_max)
        cache_set_object(cache=cache, key=uuid, value=position)

    def position_sl_update(self, bar_5m, cache, symbol, psd, close_price, mode, tp_mode):
        uuids = cache_get_object(cache=cache, key=symbol + "-" + psd)
        for uuid in uuids:
            cache_p = cache_get_object(cache=cache, key=uuid)
            if mode == "sl":
                cache_p.set_status("sl")
                open_price = cache_p.get_open_price()
                psd = cache_p.get_position_side()
                if psd == PositionSide.LONG.value:
                    sl_rate = round((close_price - open_price) / open_price, 4)
                else:
                    sl_rate = round((open_price - close_price) / open_price, 4)
                psd = cache_p.get_position_side()
                cache_set_object(cache=cache, key=symbol + "-" + psd, value=[])
                cache_delete(cache=cache, key=uuid)
                self.sql_utils.update_episode(close_time=bar_5m.dt, tp=sl_rate, uuid=uuid, max_tp="", tp_mode=tp_mode, tp_price=close_price)
            else:
                cache_p.set_status("finished")
                open_price = cache_p.get_open_price()
                psd = cache_p.get_position_side()
                if psd == PositionSide.LONG.value:
                    tp_rate = round((close_price - open_price) / open_price, 4)
                else:
                    tp_rate = round((open_price - close_price) / open_price, 4)
                cache_set_object(cache=cache, key=symbol + "-" + psd, value=[])
                cache_delete(cache=cache, key=uuid)
                self.sql_utils.update_episode(close_time=bar_5m.dt, tp=tp_rate, uuid=uuid, max_tp="", tp_mode=tp_mode, tp_price=close_price)

    def sl_monitor(self, position, bar_5m, cache):
        """ 在进行逼近试错开单之前，需要对当前正在持仓的的虚拟仓判断持仓状态，如果持仓状态为：sl，则允许开仓 """
        if not position: return
        status = position.get_status()
        if status != "pending": return
        self.position_minmax(position, bar_5m, cache)
        uuid = position.get_uuid()
        psd = position.get_position_side()
        sl_price = position.get_sl_price()
        symbol = position.get_symbol()
        close_min, close_max = position.get_close_minmax()

        if psd == PositionSide.LONG.value and close_min < sl_price and position.status == "pending":
            position.set_status("sl")  # 将模拟持仓状态设为止损并更新数据库
            cache_set_object(cache=cache, key=uuid, value=position)
            self.position_sl_update(psd=psd, close_price=sl_price, symbol=symbol, cache=cache, bar_5m=bar_5m, mode="sl", tp_mode="1")
            return position
        elif psd == PositionSide.SHORT.value and close_max > sl_price and position.status == "pending":
            position.set_status("sl")  # 将模拟持仓状态设为止损并更新数据库
            cache_set_object(cache=cache, key=uuid, value=position)
            self.position_sl_update(psd=psd, close_price=sl_price, symbol=symbol, cache=cache, bar_5m=bar_5m, mode="sl", tp_mode="1")
            return position
        else:
            cache_set_object(cache=cache, key=uuid, value=position)
            return position

    def cache_update(self, symbol, cache, bar_5m, mode="backtest"):
        """ 实时更新cache中的损盈状态 """
        print("cache")
        uuids = cache_get_object(cache=cache, key=symbol + "-" + PositionSide.LONG.value)
        init_uuid, cache_lp = "", None
        if uuids:  init_uuid = uuids[0]
        cache_lp = cache_get_object(cache=cache, key=init_uuid)
        print(cache_lp)
        if cache_lp and string2mil(bar_5m.dt) > string2mil(cache_lp.get_open_time()):
            cache_lp = self.allow_close(cache=cache, cache_p=cache_lp, bar_5m=bar_5m)
            cache_lp = self.tp_space(cache=cache, bar_5m=bar_5m, cache_p=cache_lp)
            # cache_lp = self.schedule_update(end_time=end_time, cache_p=cache_lp, bars_4h=bars_4h)
            cache_lp = self.sl_monitor(cache_lp, bar_5m=bar_5m, cache=cache)
            self.tp_update(bar_5m=bar_5m, cache=cache, position=cache_lp)
        uuids = cache_get_object(cache=cache, key=symbol + "-" + PositionSide.SHORT.value)
        init_uuid, cache_sp = "", None
        if uuids:
            init_uuid = uuids[0]
        cache_sp = cache_get_object(cache=cache, key=init_uuid)
        print(cache_sp)
        if cache_sp and string2mil(bar_5m.dt) > string2mil(cache_sp.get_open_time()):
            cache_sp = self.allow_close(cache=cache, cache_p=cache_sp, bar_5m=bar_5m)
            cache_sp = self.tp_space(cache=cache, bar_5m=bar_5m, cache_p=cache_sp)
            # cache_sp = self.schedule_update(end_time=end_time, cache_p=cache_sp, bars_4h=bars_4h)
            cache_sp = self.sl_monitor(cache_sp, bar_5m=bar_5m, cache=cache)
            self.tp_update(bar_5m=bar_5m, cache=cache, position=cache_sp)

    # def schedule_update(self, end_time, cache_p, bars_4h):
    #     """ 实时更新委托挂单 """
    #     if not cache_p: return cache_p
    #     symbol = cache_p.get_symbol()
    #
    #     if not bars_4h:
    #         bars_4h = self.get_bars(symbol=symbol, end_time=end_time, period=Interval.hour4.value)
    #     closes = Series([float(b.close) for b in bars_4h])
    #     boll = BollingerBands(close=closes, window=21)
    #     bl = boll.bollinger_lband().values
    #     bh = boll.bollinger_hband().values
    #     mavg = boll.bollinger_mavg().values
    #     status = cache_p.get_status()
    #     open_price = cache_p.get_open_price()
    #     if status != "pending": return cache_p
    #     tp_space = cache_p.get_tp_space()
    #     psd = cache_p.get_position_side()
    #     if psd == PositionSide.SHORT.value:
    #         if bars_4h[-1].low < bl[-1] and tp_space == "boll":
    #             schedule_price = round((bars_4h[-1].low+mavg[-1])/2, self.precision)
    #             if schedule_price < open_price:
    #                 cache_p.set_tp_price(schedule_price)
    #             return cache_p
    #     elif psd == PositionSide.LONG.value:
    #         if bars_4h[-1].high > bh[-1] and tp_space == "boll":
    #             schedule_price = round((bars_4h[-1].high+mavg[-1])/2, self.precision)
    #             # if schedule_price < open_price: return cache_p
    #             tp_price = cache_p.get_tp_price()
    #             if schedule_price > tp_price:
    #                 cache_p.set_tp_price(schedule_price)
    #             return cache_p
    #     return cache_p

    def tp_space(self, cache, bar_5m, cache_p):
        """
            根据虚拟仓的运行状态，实时调整虚拟仓的利润空间, 所依据的理论依据就是：随着走势运行，压力与支撑会随之发生变化，
            也要实时更新press对象和support对象
        """
        if not cache_p: return cache_p
        symbol = cache_p.get_symbol()
        bars_4h = self.bars_generator.get_bars(symbol=symbol, period=Interval.day.value, end_time=bar_5m.dt, limit=100)
        closes = Series([float(b.close) for b in bars_4h])
        boll = BollingerBands(close=closes, window=21)
        bl = boll.bollinger_lband().values
        bh = boll.bollinger_hband().values
        mavg = boll.bollinger_mavg().values
        status = cache_p.get_status()
        if status != "pending": return cache_p
        open_time = cache_p.get_open_time()
        if abs(string2mil(open_time) - string2mil(bar_5m.dt)) < 3600000 * 4: return cache_p
        tp_space = cache_p.get_tp_space()
        uuid = cache_p.get_uuid()
        psd = cache_p.get_position_side()
        if psd == PositionSide.SHORT.value:
            if bars_4h[-1].low < bl[-1] and tp_space != "boll":
                cache_p.set_tp_space("boll")
                cache_set_object(cache=cache, key=uuid, value=cache_p)
                return cache_p
        elif psd == PositionSide.LONG.value:
            if bars_4h[-1].high > bh[-1] and bars_4h[-1].low > mavg[-1] and tp_space != "boll":
                cache_p.set_tp_space("boll")
                cache_set_object(cache=cache, key=uuid, value=cache_p)
                return cache_p
        return cache_p

    def allow_close(self, cache, cache_p, bar_5m):
        """
            1、当cache_p是空单：
                            （1）4H第一次跌破boll下轨激活平仓，allow_close=1, 允许保本损
                            （2）以第一次跌破boll下轨作为支撑位，彻底跌破这个位置，allow_close=2，考虑趋势止盈
                            （3）在（2）的基础上，如果反弹到ma21，记录下轨作为第二支撑位，一旦彻底跌破第二支撑位，平完
            2、当cache_p是多单：
                            （1）4H第一次涨破boll上轨激活平仓，allow_close=1，允许保本损
                            （2）以第一次涨破boll上轨作为压力位，彻底涨破这个位置，allow_close=2，考虑趋势止盈
                            （3）在（2）的基础上，如果回落到ma21，记录上轨作为第二压力位，一旦彻底涨破第二压力位，平完
        """
        if not cache_p: return cache_p
        symbol = cache_p.get_symbol()
        bars_4h = self.bars_generator.get_bars(symbol=symbol, period=Interval.day.value, end_time=bar_5m.dt, limit=100)
        closes = Series([float(b.close) for b in bars_4h])
        boll = BollingerBands(close=closes, window=21)
        bl = boll.bollinger_lband().values
        bh = boll.bollinger_hband().values
        open_time = cache_p.get_open_time()
        uuid = cache_p.get_uuid()
        if abs(string2mil(open_time) - string2mil(bar_5m.dt)) < 3600000 * 4: return cache_p  # 持仓时间应该至少是4H，否则不止盈
        psd = cache_p.get_position_side()
        if psd == PositionSide.SHORT.value:
            if bars_4h[-1].low < bl[-1]:
                allow_close = "2"
                cache_p.set_allow_close(allow_close)
                cache_p.set_allow_close_date(bars_4h[-1].dt)
                cache_set_object(cache=cache, key=uuid, value=cache_p)
        elif psd == PositionSide.LONG.value:
            if bars_4h[-1].high > bh[-1]:
                allow_close = "2"
                cache_p.set_allow_close(allow_close)
                cache_p.set_allow_close_date(bars_4h[-1].dt)
                cache_set_object(cache=cache, key=uuid, value=cache_p)
        return cache_p

    def tp_update(self, bar_5m, cache=None, position=None):
        """ 对缓存的虚拟仓，执行平仓逻辑，进一步更新仓位状态 """
        if not position: return
        status = position.get_status()
        if status != "pending": return
        symbol = position.get_symbol()
        psd = position.get_position_side()
        open_time = position.get_open_time()
        if open_time == "2021-08-31 10:15":
            print("77")
        close_flag, tpsl = self.close_strategy.normal_close(position=position, cache=cache, debug=True, end_time=bar_5m.dt)
        if close_flag in ['3', '2', "4"]:
            close_price = bar_5m.open
            self.position_sl_update(psd=psd, tp_mode=close_flag, close_price=close_price, symbol=symbol, cache=cache, bar_5m=bar_5m, mode="tp")
