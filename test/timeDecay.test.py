import datetime
import unittest
import json
import os,sys,inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 

from  util.timeDecay import TIME_UNIT_FORMAT,TimeDecay


class TestTimsSparse(unittest.TestCase):
    def setUp(self):
        print("测试开始")

    def test_TIME_UNIT_FORMAT(self):
        a = datetime.datetime(2019, 8, 24)
        self.assertEqual(TIME_UNIT_FORMAT['day'](a), 'day-20190824')
        self.assertEqual(TIME_UNIT_FORMAT['week'](a), 'week-20190819')
        self.assertEqual(TIME_UNIT_FORMAT['month'](a), 'month-201908')
        self.assertEqual(TIME_UNIT_FORMAT['year'](a), 'year-2019')

    def test_time_sparse(self):
        expected_ret1 = {
            "2016-01-14 00:00:00": "year-2016",
            "2016-02-21 00:00:00": None,
            "2017-01-14 00:00:00": "year-2017",
            "2017-02-21 00:00:00": None,
            "2018-01-14 00:00:00": "year-2018",
            "2018-02-21 00:00:00": None,
            "2019-01-14 00:00:00": None,
            "2019-02-21 00:00:00": "month-201902",
            "2019-03-14 00:00:00": "month-201903",
            "2019-04-14 00:00:00": "month-201904",
            "2019-05-21 00:00:00": "month-201905",
            "2019-06-14 00:00:00": "month-201906",
            "2019-06-21 00:00:00": None,
            "2019-07-14 00:00:00": "month-201907",
            "2019-07-21 00:00:00": None,
            "2019-07-22 00:00:00": None,
            "2019-08-01 00:00:00": "week-20190729",
            "2019-08-02 00:00:00": None,
            "2019-08-05 00:00:00": "week-20190805",
            "2019-08-06 00:00:00": None,
            "2019-08-14 00:00:00": "week-20190812",
            "2019-08-15 00:00:00": None,
            "2019-08-21 00:00:00": "day-20190821",
            "2019-08-22 00:00:00": "day-20190822",
            "2019-08-23 00:00:00": "day-20190823",
            "2019-08-24 05:00:00": "day-20190824",
            "2019-08-24 05:17:00": "day-20190824"
            }
        expected_ret2 = {
            "2016-01-14 00:00:00": "year-2016",
            "2016-02-21 00:00:00": None,
            "2017-01-14 00:00:00": "year-2017",
            "2017-02-21 00:00:00": None,
            "2018-01-14 00:00:00": "year-2018",
            "2018-02-21 00:00:00": None,
            "2019-01-14 00:00:00": None,
            "2019-02-21 00:00:00": "month-201902",
            "2019-03-14 00:00:00": "month-201903",
            "2019-04-14 00:00:00": "month-201904",
            "2019-05-21 00:00:00": "month-201905",
            "2019-06-14 00:00:00": "month-201906",
            "2019-06-21 00:00:00": None,
            "2019-07-14 00:00:00": "month-201907",
            "2019-07-21 00:00:00": None,
            "2019-07-22 00:00:00": None,
            "2019-08-01 00:00:00": None,
            "2019-08-02 00:00:00": None,
            "2019-08-05 00:00:00": "week-20190805",
            "2019-08-06 00:00:00": None,
            "2019-08-14 00:00:00": "week-20190812",
            "2019-08-15 00:00:00": None,
            "2019-08-21 00:00:00": "week-20190819",
            "2019-08-22 00:00:00": None,
            "2019-08-23 00:00:00": None,
            "2019-08-24 05:00:00": None,
            "2019-08-24 05:17:00": None,
            "2019-08-31 05:17:00": "day-20190831",
        }
        options = {"days": 6,
                "weeks": 3,
                "months": 6,
                "years": 10}
        datetime_list = sorted(expected_ret1.keys())
        ret = TimeDecay.time_decay(datetime_list,options = options)
        # print(json.dumps(ret, ensure_ascii=False, indent=2))
        self.assertDictEqual(ret, expected_ret1)
        datetime_list = sorted(expected_ret2.keys())
        ret = TimeDecay.time_decay(datetime_list, options = options)
        self.assertDictEqual(ret, expected_ret2)


        expected_ret3 = {
            "2016-01-14 00:00:00": "year-2016",
            "2016-02-21 00:00:00": None,
            "2017-01-14 00:00:00": "year-2017",
            "2017-02-21 00:00:00": None,
            "2018-01-14 00:00:00": "year-2018",
            "2018-02-21 00:00:00": None,
            "2019-01-14 00:00:00": None,
            "2019-02-21 00:00:00": None,
            "2019-03-14 00:00:00": "month-201903",
            "2019-04-14 00:00:00": "month-201904",
            "2019-05-21 00:00:00": "month-201905",
            "2019-06-14 00:00:00": "month-201906",
            "2019-06-21 00:00:00": None,
            "2019-07-14 00:00:00": "month-201907",
            "2019-07-21 00:00:00": None,
            "2019-07-22 00:00:00": None,
            "2019-08-01 00:00:00": "month-201908",
            "2019-08-02 00:00:00": None,
            "2019-08-05 00:00:00": None,
            "2019-08-06 00:00:00": None,
            "2019-08-14 00:00:00": "week-20190812",
            "2019-08-15 00:00:00": None,
            "2019-08-21 00:00:00": "week-20190819",
            "2019-08-22 00:00:00": None,
            "2019-08-23 00:00:00": None,
            "2019-08-24 05:00:00": None,
            "2019-08-24 05:17:00": None,
            "2019-08-31 05:17:00": "week-20190826"
        }
        datetime_list = sorted(expected_ret3.keys())
        end_time = datetime.datetime.strptime(
            datetime_list[-1], "%Y-%m-%d %H:%M:%S")
        end_time = end_time + datetime.timedelta(days=7)
        ret = TimeDecay.time_decay(datetime_list, end_time=end_time, options = options)
        # print(end_time, json.dumps(ret, ensure_ascii=False, indent=2))
        self.assertDictEqual(ret, expected_ret3)

    def tearDown(self):
        print("测试结束")


if __name__ == "__main__":
    # 构造测试集
    unittest.main(verbosity=2)
