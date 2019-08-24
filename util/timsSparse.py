"""
day:6
weed:3
month:8
year:10
"""
import datetime
import unittest

TIME_UNIT_FORMAT={
  "day": lambda x : x.strftime('%Y%m%d'),
  "week": lambda x : (x - datetime.timedelta(days=x.weekday())).strftime('%Y%m%d'),
  "month": lambda x : x.strftime('%Y%m'),
  "year": lambda x : x.strftime('%Y')
}
class TimsSparse(object):

    @staticmethod
    def time_sparse(datetime_list, options={"day": 6,
                                         "week": 3,
                                         "month": 8,
                                         "year": 10}):
        """
        datetime_list: 时间列表, 
        """
        now = datetime.datetime.now()
        ret = []# (str,list),(str,list)
        datetime_list = sorted(datetime_list,reverse=True) #按逆序排序
        for x in datetime_list:
          pass
          





class TestTimsSparse(unittest.TestCase):
  def setUp(self):
    print("测试开始")

  def test_TIME_UNIT_FORMAT(self):
    a = datetime.datetime(2019,8,24)
    self.assertEqual(TIME_UNIT_FORMAT['day'](a),'20190824')
    self.assertEqual(TIME_UNIT_FORMAT['week'](a),'20190819')
    self.assertEqual(TIME_UNIT_FORMAT['month'](a),'201908')
    self.assertEqual(TIME_UNIT_FORMAT['year'](a),'2019')

  def tearDown(self):
    print("测试结束")

if __name__ == "__main__":
  # 构造测试集
  unittest.main(verbosity=2)
