import os
import sys
sys.path.append(os.getcwd())
import re
import pandas as pd
import tushare as ts
import akshare as ak
from examples.hkstock.filter.blacklist import blacklist
pro = ts.pro_api('6aaf8180796145d7e361f64a539676cb10b7952df02395d59850be21')
base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


def read_stocks():
    df = pd.read_excel(base_dir+"/data/hkstock_basics.xlsx", converters={"symbol": str})
    result = df.to_dict(orient="records")
    stocks = []
    for stock in result:
        name = stock.get("name")
        name = re.sub("\s+", "", name)
        symbol = stock.get("symbol")
        if name in blacklist: continue
        stocks.append(stock)
    return stocks


def get_stock_basic():
    # 查询当前所有正常上市交易的股票列表
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    data.to_excel(base_dir+"/data/hkstock_basic.xlsx")


def split(batch_size=9):
    stocks = read_stocks()
    batch_cnt = int(len(stocks)/batch_size)
    for i in range(batch_size):
        bs = stocks[i * batch_cnt: batch_cnt * (i + 1)]
        data = "batch{} = [\n".format(i + 1) + "\n".join(["\t'{}',\t\t# {}".format(b["symbol"], b["name"]) for b in bs]) + "\n]\n"
        with open(file="batch{}.py".format(i+1), mode="w", encoding="utf8") as f:
            f.write(data)


if __name__ == '__main__':
    # a = ak.stock_zh_a_spot_em()
    # a.to_excel(base_dir + "/data/astock_basic2.xlsx")
    # print(a)

    # a = ak.stock_zh_a_spot()
    # a.to_excel(base_dir + "/data/astock_basic88.xlsx")
    # print(a)

    # a = ak.stock_hk_spot()
    # a.to_excel(base_dir + "/data/hkstock_basicsyy.xlsx")
    # print(a)
    split(3)

