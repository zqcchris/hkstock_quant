import sys, os
sys.path.append(os.getcwd())
import re
import time
import datetime
import iso8601
from dateutil import parser


# 将秒的时间戳转为字符串形式的datetime
def sec2datetime(timestamps):
    t = datetime.datetime.fromtimestamp(timestamps).strftime("%Y-%m-%d %H:%M:%S")
    return t


def string2mil(st):
    return dt2ts_mil(string2datetime(st))


# 将毫秒的时间戳转为字符串形式的datetime
def mil2datetime(mil_timestamp):
    d = datetime.datetime.fromtimestamp(mil_timestamp / 1000).strftime("%Y-%m-%d %H:%M:%S")
    return d


# # 将datetime类型的时间转为秒时间戳
def dt2ts_sec(dt):
    r = int(time.mktime(dt.timetuple()))
    return r


# 将datetime类型的时间转为毫秒时间戳
def dt2ts_mil(dt: object) -> object:
    r = int(time.mktime(dt.timetuple())) * 1000
    return r


def ts2truct(ts):
    time_struct = time.localtime(ts)
    return time_struct


def string2datetime(st="2021-06-13 06:58:00"):
    return parser.parse(st)
    # result = datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S")
    # return result


def datetime2string(dt: datetime):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def string2date(st="2021-06-13"):
    result = datetime.datetime.strptime(st, "%Y-%m-%d")
    return result


def timezone_transfer(isotime="2021-05-24T16:00:00.000Z"):
    a = iso8601.parse_date(isotime).strftime("%Y-%m-%d %H:%M:%S")
    s = string2datetime(a)+datetime.timedelta(hours=8)
    return datetime2string(s)


def get_xiaoshu(x):
    """ 针对小数，获取小数点的位数 """
    if x.is_integer():
        return 0
    else:
        return len(str(float(x)).split(".")[1])


def period_cal(t):
    periods = []
    if t.tm_hour == 23 and t.tm_min == 59:
        periods.append("1day")
        periods.append("4hour")
        periods.append("60min")
        periods.append("30min")
        periods.append("15min")
        periods.append("5min")
        periods.append("1min")
    elif (t.tm_hour + 1) % 4 == 0 and t.tm_min == 59:
        periods.append("4hour")
        periods.append("60min")
        periods.append("30min")
        periods.append("15min")
        periods.append("5min")
        periods.append("1min")
    elif (t.tm_min + 1) % 60 == 0:
        periods.append("60min")
        periods.append("30min")
        periods.append("15min")
        periods.append("5min")
        periods.append("1min")
    elif (t.tm_min + 1) % 30 == 0 and (t.tm_min + 1) % 60 != 0:
        periods.append("30min")
        periods.append("15min")
        periods.append("5min")
        periods.append("1min")
    elif (t.tm_min + 1) % 15 == 0 and (t.tm_min + 1) % 30 != 0:
        periods.append("15min")
        periods.append("5min")
        periods.append("1min")
    elif (t.tm_min + 1) % 5 == 0 and (t.tm_min + 1) % 15 != 0:
        periods.append("5min")
        periods.append("1min")
    else:
        periods.append("1min")
    return periods


def get_timestamp():
    return int(time.time() * 1000)


def get_public_ip():
    # return "46.250.253.191"
    # result = os.popen("wget -qO - icanhazip.com")
    result = os.popen("curl cip.cc")
    oo = result.read().strip()
    pattern = r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}"
    ip_addresses = re.findall(pattern, oo)
    return ip_addresses[0]


if __name__ == '__main__':
    ip = get_public_ip()
    print(ip)
    # f = string2date("2023-11-24 10:30")
    # print(f)

