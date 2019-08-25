"""
day:6
weed:3
month:8
year:10
"""
import datetime
import unittest

TIME_UNIT_FORMAT = {
    "day": lambda x: x.strftime('%Y%m%d'),
    "week": lambda x: (x - datetime.timedelta(days=x.weekday())).strftime('%Y%m%d'),
    "month": lambda x: x.strftime('%Y%m'),
    "year": lambda x: x.strftime('%Y')
}
TIME_UNIT_SEQ = ['day', 'week', 'month', 'year']
def my_timedelta(t,years=0,months=0, days=0, seconds=0, microseconds=0,
                milliseconds=0, minutes=0, hours=0, weeks=0):
    if years!=0:
      return datetime.datetime(
        t.year+years,t.month, t.day,t.hour,t.minute, t.second,t.microsecond,t.tzinfo)
    elif months!= 0:
      month = t.month+months
      year = t.year
      if month >12:
        month = month % 12
        year +=1
      if month < 1:
        year -=1
        month += 12
      return datetime.datetime(
       year,month, t.day,t.hour,t.minute, t.second,t.microsecond,t.tzinfo)    
    else:
      return t+ datetime.timedelta(days, seconds, microseconds,
                  milliseconds, minutes, hours, weeks)  

class TimsSparse(object):

    @staticmethod
    def time_sparse(datetime_list, options={"days": 6,
                                            "weeks": 3,
                                            "months": 8,
                                            "years": 10},
                    now=datetime.datetime.now()):
        """
        datetime_list: 时间列表, 
        """
        ret = {}  # (str_date,datetime),(str,list)
        datetime_list = sorted(datetime_list)  # 按逆序排序
        for i, unit in enumerate(TIME_UNIT_SEQ):
            _this_end = TIME_UNIT_FORMAT[unit](now)
            p = {}
            p[unit+'s'] = 0 - options[unit+'s']
            _this_begin = TIME_UNIT_FORMAT[unit](my_timedelta(now,**p))
            while len(datetime_list) > 0:
                x = datetime_list.pop() # 从大到小
                print(x)
                str_by_unit = TIME_UNIT_FORMAT[unit](x)
                if str_by_unit < _this_begin:
                    break
                if str_by_unit >= _this_end and i != 0:
                    continue

                if str_by_unit not in ret:
                    ret[str_by_unit] = []
                ret[str_by_unit].append(x)
        
        print(ret)

import json
class TestTimsSparse(unittest.TestCase):
    def setUp(self):
        print("测试开始")

    def test_TIME_UNIT_FORMAT(self):
        a = datetime.datetime(2019, 8, 24)
        self.assertEqual(TIME_UNIT_FORMAT['day'](a), '20190824')
        self.assertEqual(TIME_UNIT_FORMAT['week'](a), '20190819')
        self.assertEqual(TIME_UNIT_FORMAT['month'](a), '201908')
        self.assertEqual(TIME_UNIT_FORMAT['year'](a), '2019')

    def test_time_sparse(self):
        datetime_list = [
            datetime.datetime(2019, 8, 24, 5, 17),
            datetime.datetime(2019, 8, 24, 5),
            datetime.datetime(2019, 8, 23),
            datetime.datetime(2019, 8, 22),
            datetime.datetime(2019, 8, 21),
            datetime.datetime(2019, 8, 14),
            datetime.datetime(2019, 8, 15),
            datetime.datetime(2019, 8, 5),
            datetime.datetime(2019, 8, 6),
            datetime.datetime(2019, 8, 1),
            datetime.datetime(2019, 8, 2),
            datetime.datetime(2019, 7, 22),
            datetime.datetime(2019, 7, 21),
            datetime.datetime(2019, 7, 14),
            datetime.datetime(2019, 6, 21),
            datetime.datetime(2019, 6, 14),
            datetime.datetime(2019, 5, 21),
            datetime.datetime(2019, 4, 14),
            datetime.datetime(2019, 5, 21),
            datetime.datetime(2019, 3, 14),
            datetime.datetime(2019, 2, 21),
            datetime.datetime(2019, 1, 14),
            datetime.datetime(2018, 2, 21),
            datetime.datetime(2018, 1, 14),
            datetime.datetime(2017, 2, 21),
            datetime.datetime(2017, 1, 14),
            datetime.datetime(2016, 2, 21),
            datetime.datetime(2016, 1, 14),
        ]
        ret = TimsSparse.time_sparse(datetime_list, now=datetime_list[0])
        print(json.dumps(ret,ensure_ascii=False,indent=2))

    def tearDown(self):
        print("测试结束")


if __name__ == "__main__":
    print(datetime.datetime.now().timetuple())
    # 构造测试集
    unittest.main(verbosity=2)
