
import pydash
import os
from backup import read_config
from util.osshelper import OssHelper


class RestoreHelper(object):
    def __init__(self):
        self.config = read_config()
        self.task = None
        self.zip_file = None
        self.zip_files = []  # {name:文件名,size:文件大小,isLocal:是否本地}

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
        ret = True
        if ret:
            return 'get_task_list'
        else:
            print('the uri connnect failed ,please input again!')
            return 'wait_uri'

    def get_task_list(self):
        print('please choice the task to restore')
        for i, task in enumerate(self.config.tasks):
            print(f'{i}) {task.name}')

        print('-1) return last step')
        return 'choice_task'

    def choice_task(self):
        task_idx = input('(choice task)->').strip()
        task_idx = int(task_idx)
        if task_idx < 0:
            return 'wait_uri'
        elif task_idx >= len(self.config.tasks):
            print('the index no exist,print input  again!')
            return 'choice_task'
        else:
            self.task = self.config.tasks[task_idx]
            return 'get_file_list'

    def get_file_list(self):
        # 先获取本地文件列表
        archivePath = pydash.get(self.config, 'archivePath')
        if not archivePath:
            raise Exception("配置缺少archivePath")
        local_dir = archivePath + "/" + self.task.name
        for dir in os.listdir(local_dir):
            self.zip_files.append({
                "name":dir,
                "size":os.path.getsize(os.path.join(local_dir, dir)),
                "isLocal":True
            })
        #在获取OSS上的文件列表
        ossConf = self.config.oss
        oss = OssHelper(ossConf.accessKey, ossConf.secretKey, ossConf.url, ossConf.bucket)
        fileList = oss.get_file_list(f"{archivePath}/{self.task.name}/")
        self.zip_files.extend(fileList)
        print('please choice the following file to restore')
        if len(self.zip_files):
            return
        for i, file_obj in enumerate(self.zip_files):
            _local_or_remote = "local" if file_obj.is_local else 'remote'
            print(
                f' {i}) {file_obj.name} {int(file_obj/1024/1024)}MB ({_local_or_remote})')
        print('-1) return last step')
        return 'choice_file'

    def choice_file(self):
        file_idx = input('(choice task)->').strip()
        file_idx = int(file_idx)
        if file_idx < 0:
            return 'get_task_list'
        elif file_idx >= len(self.config.tasks):
            print('the index no exist,print input  again!')
            return 'choice_file'
        else:
            self.zip_file = self.zip_files[file_idx]
            return 'get_file_list'

    def download_unzip(self):
        pass

    def exec_mongorestore(self):
        pass

    def exit(self):
        pass


if __name__ == '__main__':
    # r = RestoreHelper()
    # status = 'start'
    # while True:
    #     status = getattr(r, status)()
    for dir in os.listdir("./util/"):
        print(dir, os.path.getsize(os.path.abspath(dir)))
        # print(dir)
