import requests
import multitasking
import pandas as pd
from tqdm import tqdm
from retry import retry
from typing import Dict, List, Union
from exchange_market.hkstock.hkstock_enum import Interval


class HkstockAPI(object):
    def __init__(self):
        self.market = "116"  # 在东方财富网中，港股的market代码默认为116, market.code构成唯一证券代码
        self.url = "http://41.push2his.eastmoney.com/api/qt/stock/kline/get"

    def get_kline(self, symbol: str = "105.MSFT", interval: str = "min5",
                  start_date: str = "19700101", end_date: str = "20230807", adjust: str = "") -> pd.DataFrame:
        """
        东方财富网-行情-美股-每日行情
        http://quote.eastmoney.com/us/ENTX.html#fullScreenChart
        :param symbol: 股票代码; 此股票代码需要通过调用 ak.stock_us_spot_em() 的 `代码` 字段获取
        :param interval: choice of {'daily', 'weekly', 'monthly'}
        :param start_date: str 开始日期
        :param end_date: str 结束日期
        :param adjust: choice of {"qfq": "1", "hfq": "2", "": "不复权"}
        :return: DataFrame
        """
        interval_dict = {"daily": "101", "weekly": "102", "monthly": "103", "min5": "5"}
        adjust_dict = {"qfq": "1", "hfq": "2", "": "0"}
        params = {
            "secid": ".".join([self.market, symbol]),
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": interval_dict[interval],
            "fqt": adjust_dict[adjust],
            "end": "20500000",
            "lmt": "1000000",
            "_": "1623766962675",
        }
        r = requests.get(self.url, params=params)
        data_json = r.json()
        if not data_json:
            return pd.DataFrame()
        if not data_json["data"]:
            print("***"*10)
            print(symbol)
        if not data_json["data"]["klines"]:
            return pd.DataFrame()
        temp_df = pd.DataFrame([item.split(",") for item in data_json["data"]["klines"]])
        temp_df.columns = [
            "日期",
            "开盘",
            "收盘",
            "最高",
            "最低",
            "成交量",
            "成交额",
            "振幅",
            "涨跌幅",
            "涨跌额",
            "换手率",
        ]
        temp_df.index = pd.to_datetime(temp_df["日期"])
        temp_df = temp_df[start_date:end_date]
        temp_df.reset_index(inplace=True, drop=True)
        temp_df["开盘"] = pd.to_numeric(temp_df["开盘"])
        temp_df["收盘"] = pd.to_numeric(temp_df["收盘"])
        temp_df["最高"] = pd.to_numeric(temp_df["最高"])
        temp_df["最低"] = pd.to_numeric(temp_df["最低"])
        temp_df["成交量"] = pd.to_numeric(temp_df["成交量"])
        temp_df["成交额"] = pd.to_numeric(temp_df["成交额"])
        temp_df["振幅"] = pd.to_numeric(temp_df["振幅"])
        temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"])
        temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"])
        temp_df["换手率"] = pd.to_numeric(temp_df["换手率"])
        temp_df.sort_values(["日期"], inplace=True)
        return temp_df

    def get_quote_history(self, symbols: Union[str, List[str]], interval: str = Interval.min5.value,
                          start_time: str = '20230701', end_time: str = '20230809'
                          ) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        df = self.get_quote_history_for_stock(symbols=symbols, start_time=start_time, end_time=end_time, interval=interval)
        return df

    def get_quote_history_for_stock(self, symbols: Union[str, List[str]], interval: str = Interval.min5.value,
                                    start_time: str = '20230701', end_time: str = '20230803'
                                    ) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        if isinstance(symbols, str):
            return self.get_kline(symbol=symbols, start_date=start_time, end_date=end_time, interval=interval)

        elif hasattr(symbols, '__iter__'):
            symbols = list(symbols)
            return self.get_quote_history_multi(symbols=symbols, start_time=start_time, end_time=end_time, interval=interval)
        raise TypeError(
            '代码数据类型输入不正确！'
        )

    def get_quote_history_multi(self, symbols: List[str], interval: str = Interval.min5.value,
                                start_time: str = '20230701', end_time: str = '20230803', tries: int = 3
                                ) -> Dict[str, pd.DataFrame]:
        """ 获取多只股票、债券历史行情信息 """
        dfs: Dict[str, pd.DataFrame] = {}
        total = len(symbols)

        @multitasking.task
        @retry(tries=tries, delay=1)
        def start(symbol: str):
            _df = self.get_kline(symbol, start_date=start_time, end_date=end_time, interval=interval)
            dfs[symbol] = _df
            pbar.update(1)
            pbar.set_description_str(f'Processing => {symbol}')

        pbar = tqdm(total=total)
        for symbol in symbols:
            start(symbol)
        multitasking.wait_for_tasks()
        pbar.close()
        return dfs


if __name__ == '__main__':
    api = HkstockAPI()
    df = api.get_quote_history(symbols=["01801", "06838", "03928"])
    print(df)
