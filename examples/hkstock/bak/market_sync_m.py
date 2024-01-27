import os
import sys
sys.path.append(os.getcwd())
import datetime
import pandas as pd
import akshare as ak
from sqlalchemy import create_engine, text
from dateutil import parser
from examples.hkstock.filter.whitelist import whitelist
from utils.utils import get_public_ip
base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def read_stocks():
    df = pd.read_excel(base_dir+"/data/astock_basic88.xlsx", converters={"symbol": str})
    result = df.to_dict(orient="records")
    return result


def schedule_task_min(connection, symbol="000063", period="5"):
    if period not in ["5", "15", "30", "60"]:
        print("Not supported period for min task")
        sys.exit(0)
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    result = connection.execute(text("select * from {}_{}m;".format(symbol, period)))
    records = result.fetchall()
    if not records:
        last_date = "2023-01-01 10:00:00"
    else:
        last_date = parser.parse(records[-1][1])
        last_date = last_date + datetime.timedelta(minutes=int(period))
    df = ak.stock_zh_a_hist_min_em(symbol=symbol, start_date=last_date, end_date=now, adjust="qfq", period=period)
    if df.empty: return
    df.drop(labels=["振幅", "换手率", "成交量", "涨跌幅", "涨跌额"], axis=1, inplace=True)
    df.rename(columns={"时间": "dt", "开盘": "open", "最高": "high", "最低": "low", "收盘": "close", "成交额": "vol"}, inplace=True)
    df.to_sql(name="{}_{}m".format(symbol, period), con=connection, if_exists="append", index=False)


if __name__ == '__main__':
    """ 设计为定时任务，每隔5分钟同步一次分钟行情数据，每日同步一次日线周线行情。并设计另外的服务器，将mysql中的行情数据，以restful API的方式提供服务 """
    ip = get_public_ip()
    # ip = "13.250.10.80"
    if ip == "13.250.10.80":
        host = "13.250.10.80"
        user = 'root'
        password = "zqc*32#fudan"
    elif ip == '18.141.167.105':
        host = '18.141.167.105'
        user = 'root'
        password = "zqc*32#fudan"
        database = "astock_market"
    elif ip == "13.214.135.34":
        host = "13.214.135.34"
        user = 'root'
        password = "zqc*32#fudan"
        database = "astock_market"
    else:
        print("服务器ip地址变更，请核查！")
        sys.exit(0)

    engine = create_engine("mysql+pymysql://{}:{}@{}/astock_market?charset=utf8".format(user, password, ip))
    con = engine.connect()
    stocks = read_stocks()
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for idx, stock in enumerate(stocks):
        if stock.get("name") not in whitelist: continue
        symbol = stock.get("symbol")
        print("{}: sync 5 minute market data for {}, {}/{}".format(now, symbol, idx, len(stocks)))
        schedule_task_min(connection=con, symbol=symbol, period="5")
    con.close()
