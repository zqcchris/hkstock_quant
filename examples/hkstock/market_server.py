"""
以mysql的各级别行情数据为基础，构建行情服务器，提供5m\15m\30m\1d\5d的行情服务
"""
import os, sys
sys.path.append(os.getcwd())
import tornado.web
import tornado.ioloop
from exchange_market.hkstock.hkstock_enum import Interval as HKStockInterval
from examples.hkstock.market_api import MarketAPI as HKStockMarketAPI
hkstock_market_api = HKStockMarketAPI()


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write({"data": "zhaoqingchen"})


class HKStockHandler(tornado.web.RequestHandler):
    def post(self, *args, **kwargs):
        code = self.get_argument(name="code")
        period = self.get_argument(name="period", default=HKStockInterval.min5.value)
        limit = self.get_argument(name="limit", default="100000")
        start_time = self.get_argument(name="start_time", default="")
        end_time = self.get_argument(name="end_time", default="")
        result = hkstock_market_api.get_kline(code=code, start_time=start_time, end_time=end_time,
                                              limit=limit, period=period)
        self.write({"data": result})


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r'/', MainHandler),
            (r'/hkstock', HKStockHandler)
        ]
        tornado.web.Application.__init__(self, handlers)


if __name__ == "__main__":
    app = Application()
    app.listen(8000)
    print("Tornado Started in port 8000，http://127.0.0.1:8000")
    tornado.ioloop.IOLoop.current().start()
