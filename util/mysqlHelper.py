
import random
import string
import logging
import os
import subprocess
import shutil
import sys
import inspect
from urllib.parse import urlparse

currentdir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

from util.stringhelper import StringHelper
logger = logging.getLogger(__name__)


class MysqlHelper():
    """
    /Users/lcz>mysqldump -uroot -h47.99.73.225 -p123456 db1 > db1.sql
    mysqldump: [Warning] Using a password on the command line interface can be insecure.
    /Users/lcz>mysql -uroot -h47.99.73.225 -p123456 db2 < db1.sql
    mysql: [Warning] Using a password on the command line interface can be insecure.
    """
    @staticmethod
    def sample():
        return 'mysql://root:123456@47.99.73.225/db1'   

    @staticmethod
    def backup(params, taskName, tmp_dir_base='./temp', archivePath='./archive'):
        """备份方法
        params:{u:root,p:123456,databases:db1}
        """
        _temp_dir = ''.join(random.sample(
            string.ascii_letters + string.digits, 8))
        tmp_dir = os.path.join(tmp_dir_base, _temp_dir)
        os.makedirs(tmp_dir)
        cmd = 'mysqldump '
        for (k, v) in params.items():
            cmd += (' --' if len(k) > 1 else ' -')
            cmd += k
            if k in ['B', 'databases']:
                cmd += " "
            else:
                cmd += ('=' if len(k) > 1 else '')
            cmd += f'{v}'

        cmd += f' > {tmp_dir}/back.sql'
        logger.info(f'start exec backup cmd: {cmd}')
        archive_type = 'zip'
        status = 0
        proc = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        while True:
            buff = proc.stdout.readline()
            logger.debug(buff)
            if proc.poll() is not None:
                break

        status = proc.returncode
        if status != 0:
            logger.info(f'backup failed,cmd: {cmd}')
            return None
        # 获得db_filepath下的子目录，作为数据库名称
        zip_file = os.path.join(
            archivePath, taskName, StringHelper.get_datestr())
        zip_file = shutil.make_archive(zip_file, archive_type, tmp_dir)

        # 删除原始目录
        shutil.rmtree(tmp_dir)
        return zip_file

    @staticmethod
    def extract(zip_file, tmp_dir='./temp'):
        """从zip文件中抽取文件,返回下一步执行结果
        """
        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)

        # 解压到临时目录
        shutil.unpack_archive(zip_file, tmp_dir)
        # 找到数据库名称
        sub_dir_list = [f for f in os.listdir(tmp_dir) if not f.startswith('.')]
        return os.path.join(tmp_dir, sub_dir_list[0])

    @staticmethod
    def restore(db_file, uri):
        """恢复方法
        uri='mysql://root:123456@47.99.73.225/db2'
        https://docs.python.org/3/library/urllib.parse.html
        """

        u = urlparse(uri)
        if 'mysql' not in u.scheme:
            raise Exception('this not mysql connection uri:'+uri)
        
        cmd = 'mysql '
        if u.username:
            cmd += f' -u{u.username}'
        if u.password:
            cmd += f' -p{u.password}'
        if u.hostname:
            cmd += f' -h{u.hostname}'
        if u.port:
            cmd += f' -P{u.port}'

        cmd += " "+u.path.replace('/', '')
        cmd += f' < {db_file}'
        logger.info(f'start exec restore cmd: {cmd}')
        proc = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        try:
            while True:
                buff = proc.stdout.readline()
                logger.debug(buff)
                if proc.poll() is not None:
                    break
        except Exception as e:
            status = -1

        status = proc.returncode
        if status != 0:
            print(f'restore mysql failed,cmd: {cmd}')

if __name__ == "__main__":
    params = {
        "u": "root",
        "p": "123456",
        "databases": "db1",
        "host": "47.99.73.225"}
    taskName = 'a'
    stream_handler = logging.StreamHandler(sys.stdout)
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(message)s',
                        handlers=[stream_handler])
    zip_file = MysqlHelper().backup(params, taskName)
    print('zip_file=', zip_file)
    uri = 'mysql://root:123456@47.99.73.225/db2'
    db_file = MysqlHelper().extract(zip_file)
    print('db_file=', db_file)
    MysqlHelper().restore(db_file, uri)

    # a = 'mongodb://test:test@localhost:13722/test'
    # u = urlparse('//www.cwi.nl:80/%7Eguido/Python.html')
    # print(u)
    # print(urlparse(a))
    # r = urlparse('mysql://alex:pwd@localhost/test')
    # print(r)
    # print(r.username)
    # print(r.hostname)
    # print(r.port)
    # print(r.path)
    # # 'alex'
    # print(r.password)
    # # 'pwd'
