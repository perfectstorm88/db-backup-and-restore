"""
db backup
"""
import os
import sys
import datetime
import subprocess
import time
import random
import string
import shutil
import logging
import yaml
import pydash
import schedule
from util.osshelper import OssHelper
from util.attr_dict import AttrDict
from util.timeDecay import TimeDecay
from util.mongodbHelper import MongodbHelper
from util.mysqlHelper import MysqlHelper

# 简单实用的小例子，同时输出为console和文件
log_file = os.path.join(os.path.dirname(__file__), 'log', 'output.log')
if not os.path.exists(os.path.dirname(log_file)):
    os.makedirs(os.path.dirname(log_file))

file_handler = logging.FileHandler(log_file)
stream_handler = logging.StreamHandler(sys.stdout)
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    handlers=[stream_handler, file_handler])
logger = logging.getLogger(__name__)


def get_datestr():
    return datetime.datetime.now().strftime('%Y%m%d%H%M%S')


def read_config():
    """
    读取配置文件
    """
    config_file = os.path.realpath(os.path.join(
        os.path.dirname(__file__), 'config.yml'))
    if not os.path.exists(config_file):
        raise Exception('config.yml not exists,you need first `cp config.sample.yml config.yml`,'
                        + ' and modify it by you environment')

    with open(config_file, 'rt', encoding='utf-8') as f:
        config_obj = yaml.safe_load(f.read())
        config_obj = AttrDict(config_obj)

    if not config_obj.tmpPath:
        raise Exception('config.yml not define tmpPath!')
    config_obj.tmpPath = os.path.join(
        os.path.dirname(__file__), config_obj.tmpPath)
    if not config_obj.archivePath:
        raise Exception('config.yml not define archivePath!')
    config_obj.archivePath = os.path.join(
        os.path.dirname(__file__), config_obj.archivePath)

    if not config_obj.tasks:
        raise Exception('config.yml tasks not defined!')
    return config_obj


def remote_save(localFilePath, config, taskName):
    if not localFilePath:
        raise Exception(
            'when remote file to remote, find the local not exists!')
    ossConf = config.oss
    if ossConf:
        oss = OssHelper(ossConf.accessKey, ossConf.secretKey,
                        ossConf.url, ossConf.bucket)
        ossPath = ossConf.prefix + taskName + \
            "/" + os.path.basename(localFilePath)
        oss.upload(ossPath, localFilePath)


def backup(task, config):
    db_file = backup_db(task, config)
    remote_save(db_file, config, task.name)
    clear_old_backup(config, db_file, task.name)


# 清除旧备份文件
def clear_old_backup(config, db_file, task_name):
    if 'oss' in config:
        # 清除oss旧文件
        ossConf = config.oss
        oss_spaese = {}
        for i in ossConf.timeDecay: oss_spaese.update(i.items())
        if ossConf and oss_spaese:
            oss = OssHelper(ossConf.accessKey, ossConf.secretKey, ossConf.url, ossConf.bucket)
            file_list = oss.get_file_list(os.path.join(ossConf.prefix, task_name))
            file_name_list = map(lambda x: os.path.basename(x['name']).split(".")[0], file_list)
            file_dict = TimeDecay.time_decay(file_name_list, oss_spaese, None, '%Y%m%d%H%M%S')
            for key, value in file_dict.items():
                if not value: oss.delete(os.path.join(ossConf.prefix, task_name, key + '.zip'))
    if 'local' in config:
        local_path = os.path.dirname(db_file)
        zip_files = os.listdir(local_path)
        local_time_decay = config.local.timeDecay
        if local_time_decay:
            local_decay = {}
            for i in local_time_decay: local_decay.update(i.items()) # 合并时间衰减保存策略 list => dict
            local_file_name_list = map(lambda x: x.split(".")[0], zip_files)
            local_file = TimeDecay.time_decay(local_file_name_list, local_decay, None, '%Y%m%d%H%M%S')
            for key, value in local_file.items():
                if not value: os.remove(os.path.join(local_path, key + ".zip"))  # 删除value为空的文件
        # else:
        #     #清除本地文件, 默认保留时间是365天
        #     expireDays = config.expireDays if config.expireDays else 365
        #     for zip_file in zip_files:
        #         # 文件创建时间
        #         create_time = datetime.datetime.fromtimestamp(os.path.getctime(zip_file))
        #         # 计算文件现在的差值
        #         diff_days = (datetime.datetime.now() - create_time).days
        #         if diff_days > expireDays:
        #             os.remove(zip_file)
    else:  # 没有配置local，表示不进行本地存档
        os.remove(db_file)

# 备份数据库


def backup_db(task, config):
    db_type = task['type']
    if db_type == 'mongodb':
        db_file = MongodbHelper.backup(task.params, task['name'],config.tmpPath,config.archivePath)
    elif db_type == 'mysql':
        db_file = MysqlHelper.backup(task.params, task['name'],config.tmpPath,config.archivePath)
    else:
        raise Exception(f"unsupported db_type [{db_type}]")

    return db_file


def start(task, config):
    starttime = datetime.datetime.now()
    logger.info(f'start exec backup task [{task["name"]}]')
    try:
        backup(task, config)
    except Exception:
        logger.info('encounter an exception:', exc_info=True)
    endtime = datetime.datetime.now()
    logger.info(f'start exec backup task [{task["name"]}], takes {endtime - starttime}seconds')

def loop():

    config = read_config()
    for task in config.tasks:
        _schedule = task.schedule
        [schedule_type, at] = _schedule.split(' ')
        if schedule_type == 'day':
            schedule.every().day.at(at).do(start, task=task, config=config)
        elif schedule_type == 'hour':
            schedule.every().hour.at(at).do(start, task=task, config=config)
        elif schedule_type in [
                'monday', 'tuesday', 'wednesday',
                'thursday', 'friday', 'saturday', 'sunday']:
            getattr(schedule.every(), schedule_type).at(
                at).do(start, task=task, config=config)
        else:
            raise Exception('不支持的'+task.schedule)

    while True:
        schedule.run_pending()
        time.sleep(2)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', "--loop", action='store_true',
                        help='run as deamon,loop until exit!')
    parser.add_argument('-t', "--task", type=str,
                        help="backup one task immediately by input name, if ? then print all task name")
    args = parser.parse_args()
    if args.loop:
        logger.info("start loop all task!")
        loop()
    if args.task == '?':
        _config = read_config()
        for task in _config.tasks:
            print(task.name)
    elif args.task:
        _config = read_config()
        _task = pydash.find(_config.tasks, lambda x: x.name == args.task)
        if _task is None:
            logger.info(f'could not find task by name:{args.task}')
            exit()
        logger.info('start backup task immediately')
        start(_task, _config)
    else:
        parser.print_help()

