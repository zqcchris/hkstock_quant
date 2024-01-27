import os, sys
sys.path.append(os.getcwd())
base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(base_dir)
import traceback
import efinance as ef
from dateutil import parser
from utils.utils import string2mil
from sqlalchemy import create_engine
from datetime import timedelta, datetime
from config.server_config import hku_server
from examples.hkstock.filter.batch3 import batch3
from examples.hkstock.sync.market_sync import sync
from examples.hkstock.filter.blacklist import blacklist
from examples.hkstock.util.stock_utils import read_stocks


if __name__ == '__main__':
    host = hku_server
    user = 'root'
    password = "zqc32fudan"
    database = "hkstock_market"
    engine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset=utf8".format(user, password, host, database))

    stocks = read_stocks()
    now = datetime.now().strftime("%Y%m%d")
    yesterday = parser.parse(now) + timedelta(days=-3)
    yesterday = yesterday.strftime("%Y%m%d")
    codes = []
    for idx, stock in enumerate(stocks):
        code = stock.get("symbol")
        name = stock.get("name")
        if name in blacklist: continue
        if code not in batch3: continue
        codes.append(code)
    x = ef.stock.get_quote_history(stock_codes=codes, beg=yesterday, end=now, klt=5, fqt=0)

    for stock in stocks:
        try:
            code = stock.get("symbol")
            if code not in codes: continue
            raw_df = x[code]
            now = datetime.utcnow().strftime("%H:%M:%S")
            """ 09:30-16:00 / utc01:30-utc08:00 """
            if string2mil(now) > string2mil("08:05:00") or string2mil(now) < string2mil("01:00:00"):
                sync(code=code, engine=engine, df=raw_df)
            else:
                raw_df.drop(raw_df.tail(1).index, inplace=True)
                sync(code=code, engine=engine, df=raw_df)
        except:
            print("同步{}的5min数据出现错误".format(stock.get("name")))
            traceback.print_exc()
