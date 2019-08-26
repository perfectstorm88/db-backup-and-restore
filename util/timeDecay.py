"""
备份存储的时间衰减策略： 越近的数据越重要，保存的时间间隔越小，份数越多；越老的数据重要性越小，保存的时间间隔越大，份数越少
可以通过策略参数控制，例如：
- `"days": 6`,   最近6天，每天保存一份
- `"weeks": 3`,  最近3周，每周保存一份
- `"months": 6`, 最近6个月，每月保存一份
- `"years": 5`,  最近5年，每年保存一份，超过5年以上就不保留备份了
"""
import datetime

TIME_UNIT_FORMAT = {
    "day": lambda x: 'day-'+x.strftime('%Y%m%d'),
    "week": lambda x: 'week-'+(x - datetime.timedelta(days=x.weekday())).strftime('%Y%m%d'),
    "month": lambda x: 'month-'+x.strftime('%Y%m'),
    "year": lambda x: 'year-'+x.strftime('%Y')
}
TIME_UNIT_SEQ = ['day', 'week', 'month', 'year']


def my_timedelta(t, years=0, months=0, days=0, seconds=0, microseconds=0,
                 milliseconds=0, minutes=0, hours=0, weeks=0):
    if years != 0:
        return datetime.datetime(
            t.year+years, t.month, t.day, t.hour, t.minute, t.second, t.microsecond, t.tzinfo)
    elif months != 0:
        month = t.month+months
        year = t.year
        if month > 12:
            month = month % 12
            year += 1
        if month < 1:
            year -= 1
            month += 12
        return datetime.datetime(
            year, month, 1)
    else:
        return t + datetime.timedelta(days, seconds, microseconds,
                                      milliseconds, minutes, hours, weeks)


class TimeDecay(object):

    @staticmethod
    def time_decay(datetime_list,
                   options={"days": 6,
                            "weeks": 3,
                            "months": 6,
                            "years": 10},
                   end_time=None,
                   time_format="%Y-%m-%d %H:%M:%S"):
        """
        datetime_list: 时间列表, 是string类型，时间格式通过time_format这个参数廷议
        options  为保存策略：
        end_time  表示最新的时间，默认是datetime_list中最大时间
        time_format 
        """
        _ret_temp = {}  # (str_date,datetime),(str,list)
        datetime_list = sorted(datetime_list)  # 按逆序排序
        ret = dict(zip(datetime_list, [None]*len(datetime_list)))
        if end_time is None:  # 如果end_time没有在函数外部定义，则
            end_time = datetime.datetime.strptime(datetime_list[-1], time_format)
        for i, unit in enumerate(TIME_UNIT_SEQ):
            _this_end = TIME_UNIT_FORMAT[unit](end_time)
            p = {}
            p[unit+'s'] = 0 - options[unit+'s']
            _this_begin = TIME_UNIT_FORMAT[unit](my_timedelta(end_time, **p))
            while len(datetime_list) > 0:
                x = datetime_list.pop()  # 从大到小
                _d = datetime.datetime.strptime(x, time_format)
                str_by_unit = TIME_UNIT_FORMAT[unit](_d)
                if str_by_unit not in _ret_temp:
                    _ret_temp[str_by_unit] = []
                if str_by_unit < _this_begin:
                    datetime_list.append(x)
                    break
                if str_by_unit >= _this_end:
                    if i == 0:
                        _ret_temp[str_by_unit].append(x)
                else:
                    _ret_temp[str_by_unit] = [x]  # 保留这个单位区间内最老的一个

        for x in _ret_temp:
            for y in _ret_temp[x]:
                ret[y] = x

        return ret
