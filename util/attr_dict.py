from copy import deepcopy


class AttrDict(dict):
    """对字段进行封装，方便属性获取，支持a["x"]和a.x，并且key不存在时不会抛出异常"""

    def __init__(self, dictionary={}):
        tmp_dict = {}
        for key, value in dictionary.items():
            tmp_dict[key] = value
            if isinstance(value, dict):
                tmp_dict[key] = AttrDict(value)
            elif isinstance(value, list):
                for index, list_item in enumerate(value):
                    if isinstance(list_item, dict):
                        value[index] = AttrDict(list_item)
        super().__init__(tmp_dict)

    #[Python － __getattr__() 和 __getattribute__() 方法的区别](http://www.cnblogs.com/bettermanlu/archive/2011/06/22/2087642.html)
    def __getattr__(self, name):
        return self[name]

    # def __getattribute__(self, key):
    #     return self[key]

    __setattr__ = dict.__setitem__

    __delattr__ = dict.__delitem__

    def __getitem__(self, item):
        if item in self:
            return super().__getitem__(item)
        else:
            return None

    def __deepcopy__(self):
        return AttrDict({deepcopy(k): deepcopy(v) for k, v in self.items()})


if __name__ == "__main__":
    original_dict = {
        "a": 1,
        "b": "1",
        "c": ["a", "b"],
        "d": {
            "a": 1,
            "b": "1"
        },
        "e": [{}, {}]
    }
    test_attr_dict = AttrDict(original_dict)
    assert isinstance(test_attr_dict, AttrDict)
    assert isinstance(test_attr_dict.d, AttrDict)
    assert isinstance(test_attr_dict.e[0], AttrDict)
    assert test_attr_dict.a == 1
    assert test_attr_dict.b == "1"
    assert len(test_attr_dict.c) == 2
