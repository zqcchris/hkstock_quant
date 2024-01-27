import os, sys
sys.path.append(os.getcwd())
from utils.mysql.connect import SQLUtils
from exchange_market.hkstock.hkstock_enum import Interval
from examples.hkstock.bars_generator import BarsGenerator
# from examples.hkstock.util.stock_utils import dump_stock_info
from examples.hkstock.strategy.open_rank_prod import Strategy as RankStrategy
from examples.hkstock.cache.position_cache import PositionCache
# stock_info = dump_stock_info()


class CAMgr(object):
    def __init__(self):
        self.cas = {}
        self.symbol_bars_generator = {}
        self.symbol_position_cache = {}
        self.sql_utils = SQLUtils(mode="production")

    def ca_init_pop(self, symbol, logger, cache, mode="production"):
        bars_generator = self.get_bars_generator(symbol=symbol)
        rankpool_30 = RankpoolStrategy(symbol=symbol, bars_generator=bars_generator, cache=cache, mode="debug",
                                       sql_utils=self.sql_utils, critic_interval=Interval.min30.value,
                                       desert_interval=Interval.min15.value)
        rankpool_week = RankpoolStrategy(symbol=symbol, bars_generator=bars_generator, cache=cache, mode="debug",
                                         sql_utils=self.sql_utils, critic_interval=Interval.week.value,
                                         desert_interval=Interval.min15.value)
        rankpool_daily = RankpoolStrategy(symbol=symbol, bars_generator=bars_generator, cache=cache, mode="debug",
                                          sql_utils=self.sql_utils, critic_interval=Interval.day.value,
                                          desert_interval=Interval.min15.value)
        topx = RankStrategy(symbol=symbol, bars_generator=bars_generator, cache=cache, mode="debug", logger=logger,
                            desert_interval=Interval.day.value, critic_interval=Interval.week.value,
                            sql_utils=self.sql_utils)
        strategies = [rankpool_30, rankpool_week, rankpool_daily, topx]
        for strategy in strategies:
            self.set_ca(symbol=symbol, ca=strategy)

    def get_ca(self, symbol, strategy_name="trendroll", cache=None, logger=None, mode="production"):
        strategy_id = "_".join([symbol, strategy_name])
        if strategy_id not in self.cas:
            self.ca_init_pop(symbol=symbol, logger=logger, mode=mode, cache=cache)
        return self.cas.get(strategy_id)

    def set_ca(self, symbol, ca):
        strategy_name = ca.get_strategy_name()
        strategy_id = "_".join([symbol, strategy_name])
        self.cas[strategy_id] = ca

    def get_bars_generator(self, symbol):
        if symbol not in self.symbol_bars_generator:
            bars_generator = BarsGenerator()
            self.symbol_bars_generator[symbol] = bars_generator
        return self.symbol_bars_generator[symbol]

    def set_bars_generator(self, symbol, bars_generator):
        self.symbol_bars_generator[symbol] = bars_generator

    def get_position_cache(self, symbol, logger):
        if symbol not in self.symbol_position_cache:
            bars_generator = self.get_bars_generator(symbol)
            position_cache = PositionCache(bars_generator=bars_generator, logger=logger, precision=2, sql_utils=self.sql_utils)
            self.symbol_position_cache[symbol] = position_cache
        return self.symbol_position_cache[symbol]

    def ca_disable(self, symbol):
        self.cas[symbol].disable()
