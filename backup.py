import os
import sys
import datetime
import subprocess
from  util.ftphelper import FtpHelper
from  util.emailhelper import EmailHelper
from  util.loghelper import LogHelper
from  util.filehelper import FileHelper
from  util.coshelper import CosHelper
from  util.osshelper import OssHelper
from  util.onedrivehelper import OneDriveHelper
import yaml
import time
import random
import string
import shutil
def log(msg):
    print(msg)
    LogHelper.info(msg)

def get_datestr():
    return datetime.datetime.now().strftime('%Y%m%d%H%M%S')


def read_config():
    from util.attr_dict import AttrDict
    config_file = os.path.realpath(os.path.join(os.path.dirname(__file__), 'config.yml'))
    if not os.path.exists(config_file):
        raise Exception('config.yml not exists,you need first `cp config.sample.yml config.yml`, and modify it by you environment')
        
    with open(config_file, 'rt',encoding='utf-8') as f:
        config_obj = yaml.safe_load(f.read())
        config_obj =  AttrDict(config_obj)
    return config_obj

def remote_save(localFilePath, config):
    if not localFilePath:
        return
    ossConf = config.oss
    oss = OssHelper(ossConf.accessKey, ossConf.secretKey, ossConf.url, ossConf.bucket)
    ossPath = ossConf.prefix + time.strftime('%Y%m%d%H%M%S') + ".gz"
    oss.upload(ossPath, localFilePath)

def backup(task,config):
    # if not os.path.exists(LOCAL_SAVE_PATH['sites']):
    #     log('本地站点备份文件保存位置不存在，尝试创建')
    #     try:
    #         os.makedirs(LOCAL_SAVE_PATH['sites'])
    #         log('创建成功:' + LOCAL_SAVE_PATH['sites'])
    #     except Exception as e:
    #         msg = '创建失败，失败原因:' + str(e)
    #         print(msg)
    #         log(msg)
    #         return
    db_file = backup_db(task,config)
    print('db_file=',db_file)
    remote_save(db_file, config)
    clear_old_backup(config,db_file)


#清除旧备份文件
def clear_old_backup(config,db_file):
    if 'oss' in config:
        #清除oss旧文件
        pass
        # for option in OSS_OPTIONS:
        #     oss = OssHelper(option['accesskeyid'],option['accesskeysecret'],option['url'],option['bucket'])
        #     for file in oss.get_file_list(option['sitedir'].rstrip('/') + '/') + oss.get_file_list(option['databasedir'].rstrip('/') + '/'):
        #         if is_oldfile(os.path.basename(file)):
        #             oss.delete(file)
    if 'local' in config:
        pass
        #清除本地文件
        # for option in COS_OPTIONS:
        #     cos = CosHelper(option['accesskeyid'],option['accesskeysecret'],option['region'],option['bucket'])
        #     for file in cos.get_file_list(option['sitedir'].rstrip('/') + '/') + cos.get_file_list(option['databasedir'].rstrip('/') + '/'):
        #         if is_oldfile(os.path.basename(file)):
        #             cos.delete(file)
    else: # 没有配置local，表示不进行本地存档
        os.remove(db_file)

# 备份数据库
def backup_db(task,config):
    db_type = task['type']
    db_file = None
    if db_type not in 'mssql,mysql,mongodb':
        log('type数据库类型错误,应该为mssql,mysql')
    log('备份数据库{0}:{1} 开始'.format(db_type,task['name']))
    if db_type == 'mongodb':
        db_file =  backup_db_mongodb(task,config)
    else:
        raise Exception(f"unsupported db_type {db_type}")
    log('备份数据库{0}:{1} 结束'.format(db_type,task['name']))
    return db_file

#备份 mongodb 数据库
def backup_db_mongodb(task,config):
    if not task.id:
        task.id = ''.join(random.sample(string.ascii_letters + string.digits, 8))
    db_filepath = os.path.join(config.tmpPath,task.id)
    os.makedirs(db_filepath)
    print(task.params)
    cmd = 'mongodump '
    for (k,v) in  task.params.items():
        cmd += ('--' if len(k)>1 else '-')
        # cmd += f'{k}={v} '
        cmd = cmd + k + "=" + v
    
    # cmd += f' --out={db_filepath}'
    cmd = cmd + " --out=" + db_filepath
    print(cmd)
    archive_type = 'zip'
    status = 0
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    try:
        while True:
            buff = proc.stdout.readline()
            print(buff)
            if proc.poll() != None:
                break
    except Exception as e:
        status = -1
    status = proc.returncode
    if status != 0:
        log('备份数据库{0}出错,执行的命令为{1}'.format(task['name'],cmd))
        return None
    else:
        # 获得db_filepath下的子目录，作为数据库名称
        archive_type = 'zip'
        zipFile = os.path.join(config.archivePath,task['name'],get_datestr())
        zipFile = shutil.make_archive(zipFile,'zip',db_filepath)
        # 删除原始目录
        shutil.rmtree(db_filepath)
        return zipFile

def start(task,config):
    starttime = datetime.datetime.now()
    log('开始备份')
    try:
        backup(task,config)
    except Exception as e:
        import traceback, sys
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("*** print_tb:")
        traceback.print_tb(exc_traceback, limit=10, file=sys.stdout)
        log('哎呀 出错了:' + str(e))
    endtime = datetime.datetime.now()
    log('本次备份完成，耗时{0}秒'.format((endtime - starttime).seconds))



if __name__ == '__main__':
    config = read_config()
    task = config.tasks[0]
    start(task,config)
    # shutil.unpack_archive('./temp/x.zip','./temp/y')
    # print(time.strftime('%Y%m%d%H%M%S'))
    