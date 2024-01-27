"""
signal-entrance strategy: 信号位-入场位策略，使用MACD和Boll配合的策略
设计思想：
        critic: 在突破上轨或下轨，生成critic，此时熵减最大。每回落一次中轨进行熵增后，critic的trigger重新按5m-15m-30m遍历一次
        Trigger: 在Critic确定了熵减效应后，用5m-15m-30m确定熵增

        todo:
        1、当5m或者15m止损太大，说明熵减效应依然强大，则取消当前级别的trigger
        2、一切围绕熵增定律来做，理解所有的历史走势，根据熵增来做
        3、熵增思想依然是震荡思路，获取的是alpha超额收益，哲学思想是回归均线，后续需要融入趋势思想，趋势思想是市场beta收益
"""
import os, sys
sys.path.append(os.getcwd())
import uuid
from pandas import Series
from ta.volatility import BollingerBands
# from examples.hkstock.strategy.trigger import Trigger
from exchange_market.mock_position import MockPosition
from exchange_market.hkstock.hkstock_enum import Interval, PositionSide
from examples.hkstock.cache.position_cache import PositionCache
from utils.cache.cache import cache_set_object, cache_get_object


class Location(object):
    """
    Location：识别趋势信号位，这种信号位通常是通过大幅度拉盘或砸盘显著突破boll的上轨或下轨来定位趋势。
    x1: 第一次跌破或者涨破布林上轨或者下轨，就会出现30m乖离率较大，需要经历一个5m的金叉死叉后才可以进场
    x2：第一次大幅度跌破布林轨道下方或上方多少比例后，触发5m抄底，这就是利用插针思路盈利
    x3: 在跌破下轨或上轨触发趋势后，经历2次5m金叉+死叉定位，后续采用5m见顶就进的思路，因为趋势已成
    x4：第一波在4h的boll处触发，然后第一次5m金叉+死叉进场，如果止损后，说明30m突破上轨，30m看涨趋势延续，需要再一次突破30m才可以作为进场信号
    x5：在趋势信号触发后，如果第一次5m精确进场模式触发了止损，那么说明30m已经是
    精确定位的逻辑：
                1、识别趋势信号位：这种信号位通常是通过大幅度拉盘或砸盘显著突破boll的上轨或下轨来定位趋势。由Location实现
                2、一旦确认了信号位，那么就需要用2个15m的macross来修复乖离率，
                3、在乖离率修复后，按5m逼近法进场，可以实现精确抄底或者抄顶
                4、额外引入定位模式：一旦突破或者跌破bh或bl一定比例，瞬间触发反弹操作机会的及时进场
    """
    def __init__(self, symbol, signal_dt, signal_type, interval, bars_generator=None, bar=None, cache=None, bv=None):
        self.symbol = symbol
        self.interval = interval
        self.signal_dt = signal_dt
        self.signal_type = signal_type  # 枚举类型："dead"、"gold"
        self.bars_generator = bars_generator
        self.cache = cache
        self.accurate_time = ""
        self.bar = bar
        self.bv = bv

    def __str__(self):
        return ", ".join([self.symbol, self.signal_type, self.signal_dt])


class Critic(object):
    def __init__(self, symbol, bars_generator, signal_type, start_date, interval=Interval.day.value, precision=4, cache=None):
        self.symbol = symbol
        self.interval = interval
        self.signal_type = signal_type
        self.start_date = start_date  # 在4H跌破上下轨才开启
        self.valid_flag = False
        self.trigger = None
        self.precision = precision
        self.bars_generator = bars_generator
        self.cache = cache

    def __str__(self):
        return "当前Critic： " + ", ".join([self.signal_type, self.start_date, str(self.valid_flag)])

    def trigger_generation(self, bar_5m, symbol):
        """ 在次级别熵增定律的实现 """
        self.validation(bar_5m)
        if not self.valid_flag: return
        trigger_interval = self.interval

        bars = self.bars_generator.get_bars(symbol=symbol, period=trigger_interval, end_time=bar_5m.dt, limit=50)
        closes = Series([float(b.close) for b in bars])
        boll = BollingerBands(close=closes, window=21)
        bl = boll.bollinger_lband().values
        bh = boll.bollinger_hband().values
        if self.signal_type == "m":
            if not self.trigger and bars[-1].high > bh[-1]:
                self.trigger = Trigger(symbol=self.symbol, trigger_time=bar_5m.dt, trigger_type="m",
                                       interval=self.interval, bars_generator=self.bars_generator)
        elif self.signal_type == "w":
            if not self.trigger and bars[-1].low < bl[-1]:
                self.trigger = Trigger(symbol=self.symbol, trigger_time=bar_5m.dt, trigger_type="w",
                                       interval=self.interval, bars_generator=self.bars_generator)

    def validation(self, bar_5m):
        if self.signal_type == "w":
            # 死叉时，需要计算15min的MACD金叉且再次回落15min的boll下轨才算调整结束
            bars = self.bars_generator.get_bars(symbol=self.symbol, period=self.interval, end_time=bar_5m.dt, limit=50)
            if not bars or len(bars) < 21: return bars
            closes = Series([float(b.close) for b in bars])
            boll = BollingerBands(close=closes, window=21)
            mavg = boll.bollinger_mavg().values
            if bar_5m.low < mavg[-1]:
                self.valid_flag = True
        elif self.signal_type == "m":
            # 金叉时，需要计算15min的MACD死叉且再次回落15min的boll下轨才算调整结束
            bars = self.bars_generator.get_bars(symbol=self.symbol, period=self.interval, end_time=bar_5m.dt, limit=50)
            if not bars or len(bars) < 21: return bars
            closes = Series([float(b.close) for b in bars])
            boll = BollingerBands(close=closes, window=21)
            mavg = boll.bollinger_mavg().values
            if bar_5m.high > mavg[-1]:
                self.valid_flag = True

    def open(self, bar_5m):
        if self.signal_type == "w":
            """ 日线滚仓空单 """
            # bars = self.bars_generator.get_bars(symbol=self.symbol, period=Interval.min15.value, end_time=bar_5m.dt, limit=50)
            # if not bars or len(bars) < 21: return bars
            # closes = Series([float(b.close) for b in bars])
            # boll = BollingerBands(close=closes, window=21)
            # bl = boll.bollinger_lband().values
            # bh = boll.bollinger_hband().values
            # if bars[-1].high > bh[-1]:
            return True, Interval.day.value
        else:
            """ 日线滚仓多单 """
            # bars = self.bars_generator.get_bars(symbol=self.symbol, period=Interval.min15.value, end_time=bar_5m.dt, limit=50)
            # if not bars or len(bars) < 21: return bars
            # closes = Series([float(b.close) for b in bars])
            # boll = BollingerBands(close=closes, window=21)
            # bl = boll.bollinger_lband().values
            # bh = boll.bollinger_hband().values
            # if bars[-1].low < bl[-1]:
            return True, Interval.day.value


class Strategy(object):
    def __init__(self, symbol, exchange="hkstock", bars_generator=None, logger=None, cache=None, mode="",
                 interval=Interval.min30.value, sql_utils=None):
        """ 4h-15m-5m """
        self.exchange = exchange
        self.symbol = symbol
        self.bars_generator = bars_generator  # 在模拟和实盘阶段的获取bars方法不同，所以为了兼容需要传入bars_generator
        self.order = None
        self.logger = logger
        self.cache = cache
        self.olds = []
        self.critic = None
        self.mode = mode
        self.sql_utils = sql_utils
        self.precision = 2
        self.interval = interval
        self.location = None
        self.position_cache = PositionCache(bars_generator=bars_generator, logger=logger, precision=2, sql_utils=self.sql_utils)

    def get_bars_generator(self):
        return self.bars_generator

    def update_db(self, position):
        """ 在数据库系统中更新开仓记录，这里可能需要滚仓的情形，这里需要注意虚拟仓的缓存要实时更新，这是必须要做的 """
        symbol = position.get_symbol()
        psd = position.get_position_side()
        open_time = position.get_open_time()
        o = round(position.get_open_price(), self.precision)
        sl_price = round(position.get_sl_price(), self.precision)
        sl_rate = position.get_sl_rate()
        trigger_time = position.get_trigger_time()
        uuid = position.get_uuid()
        comment = position.get_comment()
        category = position.get_category()
        uuids = cache_get_object(cache=self.cache, key=symbol + "-" + psd)
        if not uuids:
            """ 首次开单更新推送和虚拟仓缓存 """
            self.order = position
            cache_set_object(cache=self.cache, key=symbol + "-" + psd, value=[uuid])
            cache_set_object(cache=self.cache, key=uuid, value=position)
            self.sql_utils.to_episode(symbol=symbol, direction=psd, open_time=open_time, sl=sl_rate, uuid=uuid,
                                      exchange=self.exchange, trigger_time=trigger_time,  open_price=o,
                                      sl_price=sl_price, comment=self.interval)
        # else:
        #     """ 滚仓单只更新数据库，不推送, 但增加缓存虚拟仓记录 """
        #     uuids.append(uuid)
        #     cache_set_object(cache=self.cache, key=symbol + "-" + psd, value=uuids)
        #     cache_set_object(cache=self.cache, key=uuid, value=position)
        #     self.sql_utils.to_episode(symbol=symbol, direction=psd, open_time=open_time, sl=sl_rate, uuid=uuid,
        #                               exchange=self.exchange, trigger_time=trigger_time,
        #                               open_price=o, sl_price=sl_price, comment=comment)

    def sl_algo(self, psd, bar_5m, bars_4h, open_price, interval):
        """
            计算一个科学止损位，需要参考4H的boll轨道，也要参考30min的boll轨道。。注意识别boll的噪音问题
            止损的几种类型：
            1、30M的boll止损位
            2、4H的boll止损位
            3、爆仓损
            4、设置止损标记位，待回落后市价损（仅限量化自动策略）
        """
        bars_open = self.bars_generator.get_bars(symbol=self.symbol, period=Interval.day.value, end_time=bar_5m.dt, limit=50)
        closes = Series([float(b.close) for b in bars_open])
        boll = BollingerBands(close=closes, window=21)
        bl = boll.bollinger_lband().values
        bh = boll.bollinger_hband().values
        mavg = boll.bollinger_mavg().values

        closes4 = Series([float(b.close) for b in bars_4h])
        boll4 = BollingerBands(close=closes4, window=21)
        bl4 = boll4.bollinger_lband().values
        bh4 = boll4.bollinger_hband().values

        if psd == PositionSide.LONG.value:
            sl_price1 = open_price - (mavg[-1] - open_price)
            sl_price2 = bl[-1]
            sl_price3 = bl4[-1]
            sl_price = min(sl_price1, sl_price2, sl_price3)
            sl_rate = 0 - abs(round((sl_price - open_price) / open_price, 4))
            return round(sl_price, self.precision), sl_rate
        elif psd == PositionSide.SHORT.value:
            sl_price1 = open_price + open_price - mavg[-1]
            sl_price2 = bh[-1]
            sl_price3 = bh4[-1]
            sl_price = max(sl_price1, sl_price2, sl_price3)
            sl_rate = 0 - abs(round((sl_price - open_price) / open_price, 4))
            return round(sl_price, self.precision), sl_rate

    def run(self, symbol, step_interval, bar_5m=None, mode=""):
        """ 执行episode模式，计算open-close，但是平仓不应该依据4H上轨，这个思路导致回撤巨大，不是很合理 """
        bars_4h = self.critic_generation(symbol=symbol, bar_5m=bar_5m)
        if not self.critic: return
        print(self.critic)
        self.critic.validation(bar_5m=bar_5m)

        if mode == "init": return
        self.position_cache.cache_update(symbol=symbol, cache=self.cache, bar_5m=bar_5m)

        o, psd, trigger_time, cat = self.open(symbol=symbol, bar_5m=bar_5m)
        interval = self.interval
        if not psd: return
        if cat == "normal" and trigger_time not in self.olds:
            self.olds.append(trigger_time)
            sl_price, sl_rate = self.sl_algo(psd=psd, bar_5m=bar_5m, bars_4h=bars_4h, open_price=o, interval=interval)
            position = MockPosition(symbol=symbol, position_side=psd, open_price=o, precision=self.precision,
                                    mode=self.mode, open_time=bar_5m.dt, interval=interval, step_interval=step_interval,
                                    comment=interval, uuid=str(uuid.uuid1()), sl_price=sl_price, sl_rate=sl_rate,
                                    trigger_time=trigger_time, category=cat)
            self.update_db(position=position)

    def open(self, symbol, bar_5m):
        uuids_sp = cache_get_object(cache=self.cache, key=symbol + "-" + PositionSide.SHORT.value)
        uuids_lp = cache_get_object(cache=self.cache, key=symbol + "-" + PositionSide.LONG.value)
        o = bar_5m.open
        if self.critic.signal_type == "w":
            if self.critic.valid_flag:
                return o, PositionSide.LONG.value, self.critic.start_date, "normal"
            # if not uuids_lp:
            #     if self.critic.trigger and not self.critic.trigger.disable:
            #         flag, interval, trigger_time = self.critic.trigger.open(bar_5m)
            #         if flag:
            #             return o, PositionSide.LONG.value, self.critic.start_date, "normal", interval
            # else:
            #     uuid_init = uuids_lp[0]
            #     cache_lp = cache_get_object(cache=self.cache, key=uuid_init)
            #     tp_space = cache_lp.get_tp_space()
            #     if tp_space == "boll":
            #         flag, interval = self.critic.open(bar_5m)
            #         if flag:
            #             return o, PositionSide.LONG.value, self.critic.start_date, "roll", interval
        elif self.critic.signal_type == "m":
            """ 最好是3个trigger_interval，分别是5m、15m和30m，一旦连续止损，就冻结这个critic """
            if self.critic.valid_flag:
                return o, PositionSide.SHORT.value, self.critic.start_date, "normal"
            # if not uuids_sp:
            #     if self.critic.trigger and not self.critic.trigger.disable:
            #         flag, interval, trigger_time = self.critic.trigger.open(bar_5m)
            #         if flag:
            #             return o, PositionSide.SHORT.value, self.critic.start_date, "normal", interval
            # else:
            #     uuid_init = uuids_sp[0]
            #     cache_sp = cache_get_object(cache=self.cache, key=uuid_init)
            #     assert cache_sp.status == "pending", print("滚仓条件错误！")
            #     tp_space = cache_sp.get_tp_space()
            #     if tp_space == "boll":
            #         flag, interval = self.critic.open(bar_5m)
            #         if flag:
            #             return o, PositionSide.SHORT.value, self.critic.start_date, "roll", interval
        return o, None, None, None

    def critic_generation(self, symbol, bar_5m=None, cache=None):
        """ 基于定位器location生成的初始进场单：金叉死叉作为信号位 """
        bars = self.bars_generator.get_bars(symbol=symbol, period=self.interval, end_time=bar_5m.dt, limit=100)
        if not bars or len(bars) < 21: return bars
        closes = Series([float(b.close) for b in bars])
        boll = BollingerBands(close=closes, window=21)
        bh = boll.bollinger_hband().values
        bl = boll.bollinger_lband().values
        if bars[-1].low < bl[-1]:
            location = Location(symbol=symbol, signal_type="dead", signal_dt=bars[-1].dt, bar=bars[-1],
                                bars_generator=self.bars_generator, cache=cache, bv=round(bl[-1], self.precision),
                                interval=self.interval)
            if not self.location:
                self.location = location
            elif self.location.signal_type != location.signal_type:
                """ 上轨到下轨 """
                self.location = location
                print("********--------**********")
                print(location)
                if (not self.critic) or (self.critic and self.critic.signal_type != "m"):
                    self.critic = Critic(start_date=bars[-1].dt, symbol=symbol, signal_type="m", cache=self.cache,
                                         bars_generator=self.bars_generator, precision=self.precision, interval=self.interval)
            else:
                if location.signal_dt != self.location.signal_dt:
                    self.location = location
        elif bars[-1].high > bh[-1]:
            location = Location(symbol=symbol, signal_dt=bars[-1].dt, signal_type="gold", bar=bars[-1],
                                bars_generator=self.bars_generator, cache=cache, bv=round(bh[-1], self.precision),
                                interval=self.interval)
            if not self.location:
                self.location = location
            elif self.location.signal_type != location.signal_type:
                """ 下轨到上轨 """
                self.location = location
                print("********--------**********")
                print(location)
                if (not self.critic) or (self.critic and self.critic.signal_type != "w"):
                    self.critic = Critic(start_date=bars[-1].dt, symbol=symbol, signal_type="w", cache=self.cache,
                                         bars_generator=self.bars_generator, precision=self.precision, interval=self.interval)
            else:
                if location.signal_dt != self.location.signal_dt:
                    self.location = location
        return bars
