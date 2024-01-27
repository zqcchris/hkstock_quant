import os, sys
sys.path.append(os.getcwd())
import uuid


class MockPosition(object):
    """
    mode:
          init:            初始数据中的虚拟仓
          production:      生产环境中生成虚拟仓
    category:
          normal/init:     正常仓位，一般代表初始仓位
          roll:            滚仓仓位
    tp_space: sl           代表持仓处于止损空间，按照正常的价格设置止损
              equal        代表持仓处于保本空间，在1倍杠杆下盈利超过5%进入保本空间
              relay        代表持仓处于中继空间，意味着不想平仓，但是止损调整到新的boll的4H轨道处
              boll         代表持仓已经扩展到boll的4H轨道
              ma           代表持仓处于均线空间，按照MA21均线的变化实时动态调整委托平仓价
              diverge      代表持仓进入背离空间，即4H的轨道和MA21相差太大，为了防止均线空间的回吐，额外设置的背离空间，背离空间按照插针处理
    """
    def __init__(self, symbol, position_side, open_time, open_price=None, precision=None, sl_rate=-0.01, sl_price=-1,
                 uuid=None, close_time=None, trigger_time=None, accurate_time=None, interval=None, tp_price=None,
                 mode="", status="", final_sl=-100, comment="", amt="", allow_close="", allow_close_date="", tp_est="",
                 tp_space="sl", step_interval="", category="", strategy_name="", allow_sl=False, vol=100, message_id="",
                 exchange=""):
        """ 虚拟仓基础数据 """
        self.exchange = exchange
        self.symbol = symbol
        self.interval = interval
        self.position_side = position_side
        self.open_time = open_time                   # 字符串类型的开仓时间
        self.close_time = close_time
        self.trigger_time = trigger_time             # 记录趋势确认的时间
        self.accurate_time = accurate_time           # 记录修复乖离率后的时间
        self.sl_rate = sl_rate                       # 止损率
        self.tp_rate = 0
        self.sl_price = round(sl_price, precision)
        self.open_price = open_price
        self.final_sl = round(final_sl, precision)
        self.tp_space = tp_space
        self.strategy_name = strategy_name
        self.message_id = message_id
        if tp_price:
            self.tp_price = round(tp_price, precision)
        else:
            self.tp_price = round(sl_price, precision)
        self.open_minmax = [1000000, 0]
        self.close_minmax = [1000000, 0]

        """ 辅助变量 """
        self.uuid = str(uuid)
        self.mode = mode
        self.comment = comment
        self.precision = precision
        self.allow_close_date = allow_close_date
        if not status:
            self.status = "pending"
        else:
            self.status = status

        """ 以下变量用于记录上线交易后的虚拟仓相关属性，用以计算相关虚拟仓平仓细节 """
        self.category = category        # 虚拟仓类型：init：首仓；roll：滚仓
        self.minmax_date = open_time    # 记录虚拟仓从何时开始进行min_max计算。因为随着
        self.schedule_time = open_time  # 记录虚拟仓从何时开始进行min_max计算。因为随着
        self.min_low = 1000000          # 记录虚拟仓开仓后的最低价格，初始值为1000000
        self.max_high = 0               # 记录虚拟仓开仓后最高价格，初始值为0
        self.amt = amt                  # 实际上线后，需要记录开单张数
        self.vol = vol                  # 记录每一轮交易的开单金额
        self.tp_est = tp_est            # 记录每一个虚拟仓的预估收益率
        self.allow_close = allow_close  # 记录虚拟仓位是否允许平仓标识位
        self.max_tp = 0                 # 记录虚拟仓持仓期间的最大收益
        self.step_interval = step_interval  # 记录开仓的最小步进级别，一旦开单后，可以从下一个step_interval开始，计算minmax
        self.allow_sl = allow_sl            # 日线macd未反转，不允许止损首仓

    def set_vol(self, vol):
        self.vol = vol

    def get_vol(self):
        return round(self.vol, 2)

    def set_tp_est(self, tpe):
        self.tp_est = tpe

    def get_tp_est(self):
        return self.tp_est

    def set_allow_sl(self, a: bool):
        self.allow_sl = a

    def get_allow_sl(self):
        return self.allow_sl

    def get_strategy_name(self):
        return self.strategy_name

    def get_category(self):
        return self.category

    def set_category(self, c):
        self.category = c

    def get_step_interval(self):
        return self.step_interval

    def set_tp_space(self, ts):
        self.tp_space = ts

    def get_tp_space(self):
        return self.tp_space

    def set_allow_close(self, ac):
        self.allow_close = ac

    def set_allow_close_date(self, d):
        self.allow_close_date = d

    def set_max_tp(self, tp):
        self.max_tp = tp

    def get_max_tp(self):
        return self.max_tp

    def get_tp_rate(self):
        return self.tp_rate

    def set_tp_rate(self, tpr):
        self.tp_rate = tpr

    def get_allow_close(self):
        return self.allow_close

    def set_amt(self, amt):
        self.amt = amt

    def get_amt(self):
        return self.amt

    def get_uuid(self):
        return self.uuid

    def set_uuid(self):
        self.uuid = str(uuid.uuid1())

    def get_mode(self):
        return self.mode

    def get_comment(self):
        return self.comment

    def set_status(self, status):
        self.status = status

    def get_status(self):
        return self.status

    def get_tp_price(self):
        return round(self.tp_price, self.precision)

    def set_tp_price(self, tp):
        self.tp_price = round(tp, self.precision)

    def get_interval(self):
        return self.interval

    def get_sl_rate(self):
        return self.sl_rate

    def get_trigger_time(self):
        return self.trigger_time

    def get_accurate_time(self):
        return self.accurate_time

    def set_minmax(self, min, max):
        self.min_low = min
        self.max_high = max

    def set_open_minmax(self, min, max):
        open_min = self.open_minmax[0]
        open_max = self.open_minmax[1]
        minx, maxx = open_min, open_max
        if min < open_min:
            minx = min
        if max > open_max:
            maxx = max
        self.open_minmax = [minx, maxx]

    def set_close_minmax(self, min, max):
        close_min = self.close_minmax[0]
        close_max = self.close_minmax[1]
        minx, maxx = close_min, close_max
        if min < close_min:
            minx = min
        if max > close_max:
            maxx = max
        self.close_minmax = [minx, maxx]

    def set_schedule_time(self, sd):
        """ 每次更新委托，都需要记录委托挂单时间 """
        self.schedule_time = sd

    def get_schedule_time(self):
        return self.schedule_time

    def get_open_minmax(self):
        return self.open_minmax[0], self.open_minmax[1]

    def get_close_minmax(self):
        return self.close_minmax[0], self.close_minmax[1]

    def get_minmax(self):
        return self.min_low, self.max_high

    def reset_minmax(self):
        self.min_low = 1000000
        self.max_high = 0

    def set_minmax_date(self, dt):
        self.minmax_date = dt

    def __str__(self):
        if self.tp_space == "boll":
            return ", ".join([self.symbol, self.position_side, self.open_time, self.status, self.tp_space])
        else:
            return ", ".join([self.symbol, self.position_side, self.open_time, self.status, self.tp_space])

    def get_symbol(self):
        return self.symbol

    def get_open_time(self):
        return self.open_time

    def get_position_side(self):
        return self.position_side

    def get_sl_price(self):
        if self.sl_price:
            return round(self.sl_price, self.precision)
        else:
            return 0

    def set_sl(self, sl):
        self.sl_price = sl

    def get_open_price(self):
        if not self.open_price:
            return ""
        return round(self.open_price, self.precision)

    def get_final_sl(self):
        return self.final_sl

    def get_precision(self):
        return self.precision

    def set_message_id(self, mid):
        self.message_id = mid

    def get_message_id(self):
        if hasattr(self, 'message_id'):
            return self.message_id
        else:
            return None

    def to_text(self):
        if self.exchange == "hkstock":
            content = "【AinanceLabs】\n股票代码: {}\n方向: 买入\n价格: {}\n止损: {}\n止损率: {}\n触发时间：{}\n-------------" \
                      "-------\n*** 仅供参考，请勿外发 ***".format(self.symbol[0:-3], self.open_price, self.sl_price,
                                                          self.sl_rate, self.open_time)
            return content
        elif self.exchange == "binance":
            content = "【AinanceLabs】\n币种: {}\n方向: {}\n价格: {}\n止损: {}\n止损率: {}\n触发时间：{}\n----------------" \
                      "----\n*** 仅供参考，请勿外发 ***".format(self.symbol[0:-4], self.position_side, self.open_price,
                                                       self.sl_price, self.sl_rate, self.open_time)
            return content
        elif self.exchange == "ustock":
            content = "【AinanceLabs】\n代码: {}\n方向: {}\n价格: {}\n止损: {}\n止损率: {}\n触发时间：{}\n----------------" \
                      "----\n*** 仅供参考，请勿外发 ***".format(self.symbol.split(".")[-1], self.position_side, self.open_price,
                                                       self.sl_price, self.sl_rate, self.open_time)
            return content
        else:
            content = "【AinanceLabs】\n币种: {}\n方向: {}\n价格: {}\n止损: {}\n止损率: {}\n触发时间：{}\n----------------" \
                      "----\n*** 仅供参考，请勿外发 ***".format(self.symbol[0:-4], self.position_side, self.open_price,
                                                       self.sl_price, self.sl_rate, self.open_time)
            return content
