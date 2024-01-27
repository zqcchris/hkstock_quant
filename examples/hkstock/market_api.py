import os, sys
sys.path.append(os.getcwd())
import pymysql
from config.server_config import hku_server
from exchange_market.hkstock.hkstock_enum import Interval
from datetime import datetime, timezone, timedelta


class MarketAPI(object):
    def __init__(self):
        self.host = hku_server
        self.user = 'root'
        self.password = "zqc32fudan"
        self.database = "hkstock_market"
        self.con = pymysql.connect(host=self.host, user=self.user, database=self.database, password=self.password, charset='utf8', autocommit=True)

    def get_kline(self, market, code, **kwargs):
        if not self.con.open:
            print("pymysql断线重连中...")
            self.con = pymysql.connect(host=self.host, user=self.user, password=self.password, charset='utf8',
                                       autocommit=True, database=self.database)

        result = self.query_min_with_param(code=code, **kwargs)
        return result

    def query_min_with_param(self, code, start_time=None, end_time=None, limit="10000", period=Interval.min5.value):
        """
        :param market: choice in ["SZ", "SH"]
        :param code: like 000001
        :param start_time: like 2023-05-12 10:05:00
        :param limit: string, 最大返回条数1500条
        :param period: choice in [Interval], 默认5min级别

        当start_time不为空，end_time不为空，返回自[start_time, now]的所有数据，按时间倒序受limit数限制
        当start_time不为空，end_time为空，返回自[start_time, now]的所有数据, 按时间倒序受limit数限制
        当end_time不为空，且limit不为空，返回自[*, end_time]区间的数据，但总数限制为limit条
        当start_time不为空，end_time不为空，但最大条数超过500，则返回自[*, end_time]区间的数据，但总数限制为limit条
        """

        """ 当只传入start_time未传入end_time，则默认end_time为当前时间，返回自开始时间到最新数据 """
        datas = []
        table_name = "_".join([code, "5m"])

        if start_time:
            if not end_time: end_time = datetime.now(timezone.utc) + timedelta(hours=8)
            if not limit or int(limit) > 10000: limit = 10000
            sql = "select distinct symbol, dt, open, high, low, close, vol from {} where unix_timestamp(dt) > " \
                  "unix_timestamp('{}') and unix_timestamp(dt) < unix_timestamp('{}') order by dt desc limit {};"\
                .format(table_name, start_time, end_time, limit)
        else:
            if not end_time: end_time = datetime.now(timezone.utc) + timedelta(hours=8)
            if not limit or int(limit) > 10000: limit = 10000
            sql = "select distinct symbol, dt, open, high, low, close, vol from {} where unix_timestamp(dt) < " \
                  "unix_timestamp('{}') order by dt desc limit {};" \
                .format(table_name, end_time, limit)

        cursor = self.con.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        for entry in result:
            datas.append(list(entry))
        return datas

    def connection_close(self):
        if self.con.open:
            self.con.close()
