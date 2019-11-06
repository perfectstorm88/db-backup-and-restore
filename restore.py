#-*- coding: UTF-8 -*-
import os
import random
import string
import shutil
import subprocess
import pymongo
from backup import read_config
from util.filehelper import FileHelper
from util.attr_dict import AttrDict
import pydash
import os
import sys
from backup import read_config
from util.osshelper import OssHelper
from util.filehelper import FileHelper
from util.mongodbHelper import MongodbHelper
from util.mysqlHelper import MysqlHelper
from urllib.parse import urlparse
import logging
stream_handler = logging.StreamHandler(sys.stdout)
logging.basicConfig(level=logging.DEBUG,
                    format='%(message)s',
                    handlers=[stream_handler])
class RestoreHelper(object):
    def __init__(self):
        self.config = read_config()
        self.task = None
        self.file_obj= None
        self.file_obj_list = []  # {name:文件名,size:文件大小,type:'local' or 'oss' or 'qinu'}
        self.oss_conf = self.config.oss
        if self.oss_conf:
            self.oss = OssHelper(self.oss_conf.accessKey, self.oss_conf.secretKey, self.oss_conf.url, self.oss_conf.bucket)
        else:
            self.oss = None
        self.db_helper = None

    def start(self):
        print('******************* welcome to use database restore program ****************')
        return 'choice_task'

    def choice_task(self):
        print('please choice the task to restore')
        for i, task in enumerate(self.config.tasks):
            print(f'{i}) {task.name}')
        print('-1) return last step')

        task_idx = input('(choice task)->').strip()
        task_idx = int(task_idx)
        if task_idx < 0:
            return 'wait_uri'
        elif task_idx >= len(self.config.tasks):
            print('the index no exist,please input the number again!')
            return 'choice_task'
        else:
            self.task = self.config.tasks[task_idx]
            db_type = self.task['type']
            if db_type == 'mongodb':
                self.db_helper = MongodbHelper()
            elif db_type == 'mysql':
                self.db_helper = MysqlHelper()
            else:
                raise Exception(f"unsupported db_type [{db_type}]")

            return 'get_file_list'


    def get_file_list(self):
        # 先获取本地文件列表
        archivePath = pydash.get(self.config, 'archivePath')
        if not archivePath:
            raise Exception("配置缺少archivePath")
        local_dir = archivePath + "/" + self.task.name
        if os.path.exists(local_dir):
            for _dir in os.listdir(local_dir):
                self.file_obj_list.append(AttrDict({
                    "name":_dir,
                    "size":os.path.getsize(os.path.join(local_dir, _dir)),
                    "type":"local",
                    "path":os.path.join(local_dir, _dir)
                }))
        if self.oss:
            fileList = self.oss.get_file_list(f"{os.path.basename(archivePath)}/{self.task.name}/")
            self.file_obj_list.extend(fileList)
        print('please choice the following file to restore')
        if not len(self.file_obj_list):
            print(f'task:{self.task.name} has no data to restore, please re-select the task again')
            return 'choice_task'

        self.file_obj_list = sorted(self.file_obj_list,key=lambda x:x['name'])
        for i, file_obj in enumerate(self.file_obj_list):
            # print(
            #     f' {i}) {file_obj["name"]} {int(file_obj["size"]/1024/1024)}MB ({_local_or_remote})')
            print(
                f' {i}) {file_obj["name"]} {FileHelper.get_size(file_obj["size"])}  ({file_obj["type"]})')
        print('-1) return last step')
        return 'choice_file'

    def choice_file(self):
        file_idx = input('(choice task)->').strip()
        file_idx = int(file_idx)
        if file_idx < 0:
            return 'choice_task'
        elif file_idx >= len(self.file_obj_list):
            print('the index no exist,print input  again!')
            return 'choice_file'
        else:
            self.file_obj= self.file_obj_list[file_idx]
            return 'wait_uri'

    def wait_uri(self):
        # uri format  [Uniform Resource Identifier (URI): Generic Syntax](https://tools.ietf.org/html/rfc3986)
        print('please input the destination db uri,format is [scheme://][user[:[password]]@]host[:port][/schema][?attribute1=value1&attribute2=value2]')
        print(f'(such as {self.db_helper.sample()})')
        self.uri = input('(uri)->').strip()
        if len(self.uri) == 0:
            return 'wait_uri'
        return 'check_uri'

    def check_uri(self):
        # 检查数据库的联通性和权限问题
        print("now is check uri ....")
        u = urlparse(self.uri)
        if u.scheme and u.hostname and u.path:
            return 'download_unzip'
        else:
            print('the uri format invalid ,please input again!')
            return 'wait_uri'



    def download_unzip(self):
        # 创建临时目录，执行完毕后再删除
        _temp_dir = ''.join(random.sample(
            string.ascii_letters + string.digits, 8))
        db_filepath = os.path.join(self.config.tmpPath.replace('./', ''), _temp_dir)
        if self.file_obj['type'] == "local":
            zip_file =  self.file_obj.path
        else:
            # 从oss下载
            oss_path = f"{self.oss_conf.prefix}{self.task.name}/{self.file_obj['name']}"
            zip_file = os.path.join(db_filepath.replace('./', ''), self.file_obj['name'])
            print(f'download file from oss:{oss_path}')
            self.oss.download(oss_path, zip_file)

        print(f'unzip file:{zip_file}')
        self.db_file = self.db_helper.extract(zip_file,db_filepath)
        return 'exec_restore'

    def exec_restore(self):

        self.db_helper.restore(self.db_file,self.uri)
        # 删除原始目录
        shutil.rmtree(os.path.dirname(self.db_file))
        return 'exit'

    def exit(self):
        print('exited success!')
        exit()


    def _get_local_file(self,_path):
        print(_path)
        _local_files = [f for f in os.listdir(_path) if f.endswith('.zip')]
        ret = []
        for f in _local_files:
            file_path = os.path.join(_path,f)
            ret.append(AttrDict({"name":f,
                    "size":FileHelper.sizeof_fmt(os.path.getsize(file_path)),
                    "type":'local',
                    "path":file_path
                    }))
        return ret


if __name__ == '__main__':
    r = RestoreHelper()
    status = 'start'
    while status != "exit":
        status = getattr(r, status)()
    print("process exit")
    # db_filepath = "/root/mongodb-backup-and-restore/./"
    # print(db_filepath.replace('./', ''))
