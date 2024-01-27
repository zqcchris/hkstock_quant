from pandas import Series
from ta.volatility import BollingerBands
from exchange_market.hkstock.hkstock_enum import Interval
from exchange_market.hkstock.hkstock_enum import PositionSide


class Close(object):
    """
        一个好的平仓法是什么：
        1、大幅度盈利的单子不能全部回吐
        2、趋势单最好能平在顶部附近
        3、最好可以利用插针止盈
        4、波动太小，布林依赖出现问题，也可以平仓
        5、趋势后如何过滤
        6、定义插针的比例，然后增加一条插针止盈的模式
        7、超大止损问题，需要深入研究

        平仓重要的是需要在满足盈亏比之后才可以允许平仓，盈亏比都不满足的话，就不必要平仓了，肯定是亏损的
        1、拉盘超过50%，直接清仓，插针永远不可持续
        2、5%的保本损也必须要有
        3、一旦有盈利空间，要实时更新上轨止盈，防止极限插针扫损
        4、做多一旦涨破上轨，就保本损，而不是固定比例保本损

        根据mock position的tp_space决定平仓方式:
              sl      代表持仓处于止损空间，按照正常的价格设置止损
              equal   代表持仓处于保本空间，在1倍杠杆下盈利超过5%进入保本空间
              relay   代表持仓处于中继空间，意味着不想平仓，但是止损调整到新的boll的4H轨道处
              boll    代表持仓已经扩展到boll的4H轨道
              ma      代表持仓处于均线空间，按照MA21均线的变化实时动态调整委托平仓价
              diverge 代表持仓进入背离空间，即4H的轨道和MA21相差太大，为了防止均线空间的回吐，额外设置的背离空间，背离空间按照插针处理
    """
    def __init__(self, bars_generator=None):
        self.bars_generator = bars_generator

    def close_schedule(self, position, debug=True, end_time=None):
        """ 随着持仓时间变化，走势呈现压力支撑的动态变化，需要实时调整委托平仓以反应最新的走势模态 """
        if debug:
            tp_space = position.get_tp_space()
            symbol = position.get_symbol()
            if tp_space == "sl": return '0', None
            allow_close = position.get_allow_close()
            if allow_close != '2': return '0', None
            bars = self.bars_generator.get_bars(period=Interval.min5.value, symbol=symbol, end_time=end_time, limit=50)
            tp_price = position.get_tp_price()
            open_price = position.get_open_price()
            position_side = position.get_position_side()

            if bars[-1].high > tp_price and position_side == PositionSide.SHORT.value:
                tpsl = -round((tp_price - open_price) / open_price, 4)
                close_flag = "2"
                return close_flag, tpsl
            elif bars[-1].low < tp_price and position_side == PositionSide.LONG.value:
                tpsl = round((tp_price - open_price) / open_price, 4)
                close_flag = "2"
                return close_flag, tpsl
            else:
                return '0', None
        # 实际上线后，虚拟止损技术会通过止损挂单的方式实现
        else:
            return None, None

    def gd_close_ma(self, position, close_interval, end_time):
        """ 在布林开单循环模式下，进行终极平仓判断 """
        position_side = position.get_position_side()
        symbol = position.get_symbol()

        # 金叉死叉平仓
        bars = self.bars_generator.get_bars(period=close_interval, symbol=symbol, end_time=end_time, limit=50)
        closes = Series([float(b.close) for b in bars])
        boll = BollingerBands(close=closes, window=21)
        bh = boll.bollinger_hband().values
        bl = boll.bollinger_lband().values
        if position_side == PositionSide.LONG.value and bars[-1].low <= bl[-1]:
            return "buy_dead_cross", bars[-1].close
        elif position_side == PositionSide.SHORT.value and bars[-1].high >= bh[-1]:
            return "sell_gold_cross", bars[-1].close
        else:
            return None, bars[-1].close

    def close_final(self, position, end_time="", close_interval=None):
        """ 这是仓位再循环的终极平仓法：涨破或者跌破4H的上下轨道触发 """
        position_side = position.get_position_side()
        open_price = position.get_open_price()
        allow_close = position.get_allow_close()
        if allow_close != '2': return '', None

        close_flag, close_price = self.gd_close_ma(position=position, close_interval=close_interval, end_time=end_time)
        tpsl = round((close_price - open_price) / open_price, 4)
        if close_flag and position_side == PositionSide.LONG.value:
            return "4", tpsl
        elif close_flag and position_side == PositionSide.SHORT.value:
            tpsl = -tpsl
            return "4", tpsl
        return '', None

    def normal_close(self, position, end_time, cache, debug=False):
        """ 标准化平仓接口, 通过标准化接口调用目前的4中平仓策略，如果后续想单独测试某类平仓模式，也可以单独添加，具备了灵活性 """
        if not position: return None, None
        status = position.get_status()
        if status != "pending": return None, None
        open_interval = Interval.day.value

        # 平仓模式1: 挂单止损策略，如果触发模式1，那么后续模式就不再执行
        # close_flag, tpsl = self.close_schedule(position=position, debug=debug, end_time=end_time)
        # if close_flag == '2':
        #     return close_flag, tpsl

        close_flag, tpsl = self.close_final(position=position, end_time=end_time, close_interval=open_interval)
        if close_flag == '4':
            return close_flag, tpsl
        return '0', None

