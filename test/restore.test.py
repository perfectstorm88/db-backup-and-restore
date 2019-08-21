import unittest
import os,sys,inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 
from restore import RestoreHelper
from util.attr_dict import AttrDict
r = RestoreHelper()


class TestRestore(unittest.TestCase):
  def setUp(self):
    print("测试开始")

  def test_download_unzip(self):
    r.__dict__['file_obj'] = AttrDict({
          'name': '20190820170140.zip',
          'size': '2.2KB',
          'type': 'local',
          'path': './archive/mongo_lcz_test1/20190820170140.zip'})
    r.__dict__['config'] = AttrDict({
          'tmpPath': './temp',
          'archivePath': './archive'
        })
    r.download_unzip()

  def tearDown(self):
    r.__dict__['uri']='mongodb://test:test@47.97.22.225:13722/lcz_test1'
    r.__dict__['db_file_dir']='./temp/x1yER7mB/lcz_test1'
    r.exec_mongorestore()
    print("测试结束")

if __name__ == "__main__":
  # 构造测试集
  unittest.main()
