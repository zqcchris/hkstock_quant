# import os, sys
# sys.path.append(os.getcwd())
# base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
# sys.path.append(base_dir)
# import time
# import traceback
# import pandas as pd
# from datetime import datetime
# from pytdx.hq import TdxHq_API
# from sqlalchemy import create_engine
# from examples.hkstock.util.stock_utils import read_stocks
# api = TdxHq_API()
#
#
# def dump(category, market, code, count=800, dump_limit=-1):
#     """
#     :param: category
#                         1minK线：category in [7, 8]
#                         5minK线： category in [0]
#                         15minK线：category in [1]
#                         30minK线：category in [2]
#                         1HK线：category in [3]
#                         日线：category in [4, 9]
#                         周线: category in [5]
#                         月线: category in [6]
#                         季线：category in [10]
#                         年线：category in [11]
#     :param: market      0: 深市；1: 沪市
#     :param: code        股票代码
#     :param: count       最大数800
#     :param: dump_limit  计划下载的数据量，如果为了上线后初始化，只要3000条即可，如果是存本地txt用于回测，则需要下载全部
#     """
#     data = None
#     if market == 0:
#         symbol = ".".join([code, "SZ"])
#     else:
#         symbol = ".".join([code, "SH"])
#
#     if dump_limit == -1: dump_limit = 50000
#     if api.connect('119.147.212.81', 7709):
#         print(f'connect successful ')
#         data = api.to_df(api.get_security_bars(category=category, market=market, code=code, start=0, count=count))
#         data.drop(labels=["year", "month", "day", "hour", "minute", "amount"], axis=1, inplace=True)
#         ex = None
#         while True:
#             try:
#                 time.sleep(1)
#                 start = int(data.size/6)
#                 print("{}: 获取条数：{}".format(code, start))
#                 data2 = api.to_df(api.get_security_bars(category=category, market=market, code=code, start=start, count=count))
#                 ex = data2
#                 if data2.empty: break
#                 data2.drop(labels=["year", "month", "day", "hour", "minute", "amount"], axis=1, inplace=True)
#                 data = pd.concat([data2, data], ignore_index=True)
#                 if int(data2.size/6) < count or (data.size/6) > dump_limit: break
#             except:
#                 traceback.print_exc()
#                 print("*****")
#                 print(ex)
#                 print("*****")
#                 break
#
#         print(f'disconnect ... ')
#         api.disconnect()
#     data["symbol"] = symbol
#     return data
#
#
# if __name__ == '__main__':
#     ip = "18.177.119.24"
#     host = "18.177.119.24"
#     user = 'root'
#     password = "zqc32fudan"
#     database = "hkstock_market"
#
#     engine = create_engine("mysql+pymysql://{}:{}@{}/hkstock_market?charset=utf8".format(user, password, host))
#     con = engine.connect()
#
#     stocks = read_stocks()
#     f = os.listdir(base_dir + "/data/market/hkstock")
#     for idx, stock in enumerate(stocks):
#         try:
#             symbol = stock.get("symbol")
#             name = stock.get("name")
#             market = str.upper(symbol[0:2])
#             code = symbol[2:]
#             # if code != "600280": continue
#             table_name = "_".join([code, market, "5m"])
#             print("dump data: {}, {}/{}".format(table_name, idx, len(stocks)))
#             if market == "SH":
#                 market = 1
#             elif market == "SZ":
#                 market = 0
#             else:
#                 continue
#             category = 0
#
#             df = dump(category=category, market=market, code=code, dump_limit=12000)
#             df.rename(columns={"datetime": "dt"}, inplace=True)
#             order = ["symbol", "dt", "open", "high", "low", "close", "vol"]
#             df = df[order]
#             now = datetime.utcnow()
#             if (now.hour >= 7 and now.minute > 10) or (now.hour >= 3 and now.minute > 40):
#                 df.to_sql(name=table_name, con=con, if_exists="append", index=False)
#                 con.commit()
#             else:
#                 df.drop(df.tail(1).index, inplace=True)
#                 df.to_sql(name=table_name, con=con, if_exists="append", index=False)
#                 con.commit()
#             # df.to_csv('{}/data/market/hkstock/{}.txt'.format(base_dir, table_name), sep='\t', header=False,
#             #           index=False, encoding='ascii', float_format='%.2f', mode='a+')
#
#             # print(df)
#             time.sleep(1)
#         except:
#             traceback.print_exc()
#     con.close()
#
