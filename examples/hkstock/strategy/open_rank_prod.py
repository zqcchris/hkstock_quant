"""
原理：识别某一级别中阳，一旦中阳出现，意味着可能是新的止损点到来，然后或许会出现滚仓机会
"""
import os, sys
sys.path.append(os.getcwd())
import json
import uuid
from pandas import Series
from ta.volatility import BollingerBands
from exchange_market.mock_position import MockPosition
from exchange_market.hkstock.hkstock_enum import Interval, PositionSide
from examples.hkstock.cache.position_cache import PositionCache
from utils.cache.cache import cache_set_object, cache_get_object
from utils.rt_push.telegram_bot import hk_stock_channel, send_text


class Strategy(object):
    def __init__(self, symbol, exchange="hkstock", bars_generator=None, logger=None, cache=None, mode="",
                 critic_interval=Interval.day.value, desert_interval=Interval.day.value, sql_utils=None):
        """ 4h-15m-5m """
        self.strategy_name = "_".join(["topx"])
        self.exchange = exchange
        self.symbol = symbol
        self.bars_generator = bars_generator  # 在模拟和实盘阶段的获取bars方法不同，所以为了兼容需要传入bars_generator
        self.order = None
        self.roll_order = None
        self.logger = logger
        self.cache = cache
        self.olds = []
        self.round = None
        self.critic = None
        self.mode = mode
        self.breakout = None
        self.sql_utils = sql_utils
        self.precision = bars_generator.get_price_precision()
        self.critic_interval = critic_interval
        self.desert_interval = desert_interval
        self.position_cache = PositionCache(bars_generator=bars_generator, logger=logger, precision=2, sql_utils=self.sql_utils)

    def get_strategy_name(self):
        return self.strategy_name

    @staticmethod
    def get_strategy_info():
        return "rank"

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
        interval = position.get_interval()
        tp_price = position.get_tp_price()
        comment = position.get_comment()
        category = position.get_category()
        strategy_name = position.get_strategy_name()
        vol = position.get_vol()
        tp_est = position.get_tp_est()
        cache_p = cache_get_object(cache=self.cache, key=symbol + "-" + psd)
        if not cache_p or (cache_p and cache_p.status != "pending"):
            self.order = position
            message_id = ""
            rsp = send_text(chat_id=hk_stock_channel, content=position.to_text())
            result = json.loads(rsp.text)
            if result["ok"] is True:
                message_id = result['result']['message_id']
                position.set_message_id(message_id)
            cache_set_object(cache=self.cache, key=symbol + "-" + psd, value=position)
            self.sql_utils.to_episode(symbol=symbol, direction=psd, open_time=open_time, sl=sl_rate, uuid=uuid, vol=vol,
                                      close_price=tp_price, exchange=self.exchange, trigger_time=trigger_time,
                                      message_id=message_id, open_price=o, sl_price=sl_price, tp_est=-1,
                                      comment=strategy_name+"_"+interval+"_init")
        else:
            """ 滚仓+保本损 """
            cache_open_price = cache_p.get_open_price()
            new_open_price = position.get_open_price()
            chg = (new_open_price - cache_open_price)/cache_open_price
            if chg > 0.02:
                position.set_category("roll")
                self.roll_order = position
                reply_mid = cache_p.get_message_id()
                message_id = ""
                congrates = "恭喜老铁, 本次推送涨幅{}%, 请参考下一条提示准备滚仓吧!!".format(round(chg * 100, 2))
                send_text(chat_id=hk_stock_channel, content=congrates, reply_to_message_id=reply_mid)
                rsp = send_text(chat_id=hk_stock_channel, content=position.to_text())
                result = json.loads(rsp.text)
                if result["ok"] is True:
                    message_id = result['result']['message_id']
                    position.set_message_id(message_id)
                cache_set_object(cache=self.cache, key=symbol + "-" + psd, value=position)
                self.sql_utils.to_episode(symbol=symbol, direction=psd, open_time=open_time, sl=sl_rate, uuid=uuid,
                                          message_id=message_id, vol=vol, close_price=tp_price, exchange=self.exchange,
                                          trigger_time=trigger_time, open_price=o, sl_price=sl_price, tp_est=-1,
                                          comment=strategy_name+"_"+interval+"_roll")

    def dynamic_algo(self, psd, bar_5m, open_price, open_interval=Interval.min5.value):
        """ 优化止损方案需要实现：1、兜底胜率；2、弥补仓动态估算与回撤控制 """
        bars_open = self.bars_generator.get_bars(symbol=self.symbol, interval=open_interval, end_time=bar_5m.dt)
        closes = Series([float(b.close) for b in bars_open])
        boll = BollingerBands(close=closes, window=21)
        bl = boll.bollinger_lband().values
        bh = boll.bollinger_hband().values
        mavg = boll.bollinger_mavg().values

        if psd == PositionSide.LONG.value:
            sl_band = bh[-1] - bl[-1]
            delta = sl_band/4
            sl_price = mavg[-1] - 3 * delta
            sl_price = bl[-1]
            sl_price = bars_open[-2].low - delta
            tp_est = round((bh[-1] - open_price)/open_price, self.precision)
            # accumulated_loss = self.round.get_accumulated_loss()
            # if tp_est != 0:
            #     compensate_vol = abs(accumulated_loss)/tp_est
            # else:
            compensate_vol = 0
            sl_rate = 0 - abs(round((sl_price - open_price) / open_price, 4))
            return round(sl_price, self.precision), sl_rate, tp_est, round(compensate_vol, 2)
        elif psd == PositionSide.SHORT.value:
            sl_band = bh[-1] - bl[-1]
            delta = sl_band/4
            sl_price = mavg[-1] + 3 * delta
            # sl_price = bh[-1]
            tp_est = 0 - round((bl[-1] - open_price)/open_price, self.precision)
            # accumulated_loss = self.round.get_accumulated_loss()
            # if tp_est != 0:
            #     compensate_vol = abs(accumulated_loss)/tp_est
            # else:
            compensate_vol = 0
            sl_rate = 0 - abs(round((sl_price - open_price) / open_price, 4))
            return round(sl_price, self.precision), sl_rate, tp_est, round(compensate_vol, 2)

    def run(self, symbol, step_interval=Interval.min5.value, bar_5m=None, mode=""):
        """ 执行episode模式，计算open-close，但是平仓不应该依据4H上轨，这个思路导致回撤巨大，不是很合理 """
        # self.position_cache.cache_update(bar_5m=bar_5m, cache=self.cache, symbol=symbol)
        psd, trigger_time, cat, interval, uid = self.open(bar_5m=bar_5m, symbol=symbol)
        if not psd: return
        sl_price, sl_rate, tp_est, compensate_vol = self.dynamic_algo(psd=psd, bar_5m=bar_5m, open_price=bar_5m.close, open_interval=interval)
        if trigger_time not in self.olds:
            self.olds.append(trigger_time)
            position = MockPosition(symbol=symbol, position_side=psd, open_price=bar_5m.close, precision=self.precision,
                                    mode=self.mode, open_time=bar_5m.dt, interval=interval, step_interval=step_interval,
                                    comment=self.strategy_name, uuid=str(uuid.uuid1()), sl_price=sl_price,
                                    sl_rate=sl_rate, exchange=self.exchange, trigger_time=trigger_time, category=cat,
                                    strategy_name=self.strategy_name)
            self.update_db(position=position)

    def interval_filter(self, symbol, bar_5m):
        bars_1h = self.bars_generator.get_bars(symbol=symbol, end_time=bar_5m.dt, interval=Interval.min60.value, limit=240)
        bars_30m = self.bars_generator.get_bars(symbol=symbol, end_time=bar_5m.dt, interval=Interval.min30.value, limit=240)
        bars_15m = self.bars_generator.get_bars(symbol=symbol, end_time=bar_5m.dt, interval=Interval.min15.value, limit=240)
        bars_5m = self.bars_generator.get_bars(symbol=symbol, end_time=bar_5m.dt, interval=Interval.min5.value, limit=240)
        if len(bars_1h) > 1 and bars_1h[-2].taxx.macd < 0:
            return Interval.min60.value
        if len(bars_30m) > 1 and bars_30m[-2].taxx.macd < 0:
            return Interval.min30.value
        elif len(bars_15m) > 1 and bars_15m[-2].taxx.macd < 0:
            return Interval.min15.value
        elif len(bars_5m) > 1 and bars_5m[-2].taxx.macd < 0:
            return Interval.min5.value
        else:
            return None

    def open(self, symbol, bar_5m):
        interval_x = self.interval_filter(symbol=symbol, bar_5m=bar_5m)
        if not interval_x: return None, None, None, None, None
        bars = self.bars_generator.get_bars(symbol=symbol, end_time=bar_5m.dt, interval=interval_x, limit=240)
        if bars[-1].taxx.macd > bars[-2].taxx.macd and bars[-3].taxx.macd > bars[-2].taxx.macd:
            psd = PositionSide.LONG.value
            trigger_time = bars[-1].dt
            return psd, trigger_time, None, interval_x, str(uuid.uuid1())
        return None, None, None, None, None
