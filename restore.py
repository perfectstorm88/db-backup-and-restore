
import os
import random
import string
import shutil
import subprocess
import pymongo
from backup import read_config
from util.filehelper import FileHelper
from util.attr_dict import AttrDict
class RestoreHelper(object):
    def __init__(self):
        self.config = read_config()
        self.task = None
        self.file_obj= None
        self.file_obj_list = []  # {name:文件名,size:文件大小,type:'local' or 'oss' or 'qinu'}

    def start(self):
        print('welcome to use mongodb backup')
        return 'wait_uri'

    def wait_uri(self):
        print('please input the dest db uri(such as mongodb://usename:password@127.0.0.1:27017/test)')
        self.uri = input('(uri)->').strip()
        if len(self.uri) == 0:
            return 'wait_uri'
        return 'check_uri'

    def check_uri(self):
        # 检查数据库的联通性和权限问题
        ret = True
        if ret:
            return 'choice_task'
        else:
            print('the uri connnect failed ,please input again!')
            return 'wait_uri'

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
            return 'get_file_list'

    def get_file_list(self):
        '''先获取本地文件列表 + 在获取OSS上的文件列表'''
        # TODO
        self.file_obj_list = []
        local_path = os.path.join(self.config.archivePath,self.task.name)
        self.file_obj_list.extend(self._get_local_file(local_path))
        if len(self.file_obj_list)==0:
            print(f'could not find any file for the backup task[{self.task.name}],please choice new task again')
            return 'choice_task'

        print('please choice the following file to restore')
        for i, file_obj in enumerate(self.file_obj_list):
            print(f" {i}) {file_obj['name']} {file_obj['size']} ({file_obj['type']})")
        print('-1) return last step')
        return 'choice_file'

    def choice_file(self):
        file_idx = input('(choice task)->').strip()
        file_idx = int(file_idx)
        if file_idx < 0:
            return 'choice_task'
        elif file_idx >= len(self.config.tasks):
            print('the index no exist,print input  again!')
            return 'choice_file'
        else:
            self.file_obj= self.file_obj_list[file_idx]
            return 'download_unzip'

    def download_unzip(self):
        print(self.file_obj)
        # 创建临时目录，执行完毕后再删除
        _temp_dir = ''.join(random.sample(
            string.ascii_letters + string.digits, 8))
        db_filepath = os.path.join(self.config.tmpPath, _temp_dir)
        print("*"*90,db_filepath)
        if self.file_obj['type'] == 'local':
            zip_file =  self.file_obj.path
        else:
            # 从oss下载
            pass
        print(zip_file,db_filepath)
        # 解压到临时目录
        shutil.unpack_archive(zip_file,db_filepath)
        # 找到数据库名称
        sub_dir = [f for f in os.listdir(db_filepath) if os.path.isdir(os.path.join(db_filepath,f))]
        if len(db_filepath)>1:
            return 'choice_database'
        # 执行mongodb
        self.db_file_dir = os.path.join(db_filepath,sub_dir)
        return 'exec_mongorestore'

    def exec_mongorestore(self):
        uri_ret = pymongo.uri_parser.parse_uri(self.uri)
        print(uri_ret)
        cmd = 'mongorestore '
        cmd += f' --dir={self.db_file_dir}'

        cmd += f' --host={uri_ret["nodelist"][0][0]}'
        cmd += f' --port={uri_ret["nodelist"][0][1]}'
        cmd += f' --username={uri_ret["username"]}'
        cmd += f' --password={uri_ret["password"]}'
        # cmd += f' --host={self.uri}'
        cmd += f' --db={uri_ret["database"]}'
        cmd += f' --drop'
        print(cmd)
        proc = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        try:
            while True:
                buff = proc.stdout.readline()
                print(buff)
                if proc.poll() is not None:
                    break
        except Exception as e:
            status = -1
        status = proc.returncode
        if status != 0:
            print(f'恢复数据库{0}出错,结果为,执行的命令为{1}'.format(task['name'],cmd))
            return 'exit'
        else:
            # 删除原始目录
            shutil.rmtree(os.path.dirname(self.db_file_dir))
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
    while True:
        status = getattr(r, status)()