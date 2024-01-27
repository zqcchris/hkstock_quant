import os
import sys
sys.path.append(os.getcwd())
import pandas as pd
from examples.hkstock.util.stock_utils import read_stocks
base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


def create_hk_tables(con):
    cursor = con.cursor()
    cursor.execute("use hkstock_market;")
    stocks = read_stocks()
    intervals = ["5m"]
    for idx, stock in enumerate(stocks):
        print("create market table for hkstock {}: {}/{}".format(stock.get("symbol"), idx, len(stocks)))
        for interval in intervals:
            create_hkstock = """ create TABLE if not exists {}(
            id int(11) not null auto_increment primary key,
            symbol varchar(20) default null,
            dt varchar(20) default null,
            open varchar(20) default null,
            high varchar(20) default null,
            low varchar(20) default null,
            close varchar(20) default null,
            vol varchar(20) default null,
            index(dt)
            )ENGINE=InnoDB AUTO_INCREMENT=2, DEFAULT CHARSET=utf8;
            """.format(stock.get("symbol")+"_"+interval)
            cursor.execute(create_hkstock)
