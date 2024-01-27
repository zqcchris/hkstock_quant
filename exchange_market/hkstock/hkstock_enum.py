from enum import Enum


# 时间间隔
class Interval(Enum):
    min5 = "min5"
    min15 = "15"
    min30 = "30"
    min60 = "60"
    day = 'daily'
    week = 'week'


class PositionSide(Enum):
    LONG = "buy"
    SHORT = "sell"

