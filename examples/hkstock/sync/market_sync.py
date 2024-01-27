import os, sys
sys.path.append(os.getcwd())
base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(base_dir)
import traceback
from sqlalchemy import text


def sync(engine, code, df):
    try:
        table_name = "_".join([code, "5m"])

        with engine.connect() as connection:
            result = connection.execute(text("select symbol, dt from {} order by dt desc limit 10;".format(table_name)))
            records = result.fetchall()
            if not records:
                last_date = "2023-06-20 10:00"
            else:
                last_date = records[0][1]

            df.drop(labels=["振幅", "换手率", "成交额", "涨跌幅", "涨跌额", "股票名称"], axis=1, inplace=True)
            df.rename(columns={"日期": "dt", "股票代码": "symbol", "开盘": "open", "最高": "high", "最低": "low", "收盘": "close",
                               "成交量": "vol"}, inplace=True)
            order = ["symbol", "dt", "open", "high", "low", "close", "vol"]
            df["symbol"] = code
            df = df[order]
            df["vol"] = df["vol"].apply(lambda x: x * 100)
            df = df[df["dt"] > last_date]
            if df.empty: return
            print("sync 5min data for: {}, last_date is: {}, cur date is: {}".format(code, last_date, df.tail(1).iloc[0, 1]))
            df.to_sql(name=table_name, con=connection, if_exists="append", index=False)
            connection.commit()
    except:
        traceback.print_exc()
        print("Exception: ", code)