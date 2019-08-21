"""
mongodb backup and restore
"""
import os
import sys
import datetime
import subprocess
import yaml
import random
import string
import shutil
import pydash
from util.loghelper import LogHelper
from util.attr_dict import AttrDict

def log(msg):
    print(msg)
    LogHelper.info(msg)


def get_datestr():
    return datetime.datetime.now().strftime('%Y%m%d%H%M%S')


def read_config():
    config_file = os.path.realpath(os.path.join(
        os.path.dirname(__file__), 'config.yml'))
    if not os.path.exists(config_file):
        raise Exception('config.yml not exists,you need first `cp config.sample.yml config.yml`,'
             + ' and modify it by you environment')

    with open(config_file, 'rt', encoding='utf-8') as f:
        config_obj = yaml.safe_load(f.read())
        config_obj = AttrDict(config_obj)
    return config_obj


def backup(task, config):
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
    db_file = backup_db(task, config)
    print('db_file=', db_file)
    # remote_save(db_files)
    clear_old_backup(config, db_file)


# 清除旧备份文件
def clear_old_backup(config, db_file):
    if 'oss' in config:
        # 清除oss旧文件
        pass
    if 'local' in config:
        pass
        # 清除本地文件
    else:  # 没有配置local，表示不进行本地存档
        os.remove(db_file)

# 备份数据库


def backup_db(task, config):
    db_type = task['type']
    db_file = None
    if db_type not in 'mssql,mysql,mongodb':
        log('type数据库类型错误,应该为mssql,mysql')
    log('备份数据库{0}:{1} 开始'.format(db_type, task['name']))
    if db_type == 'mongodb':
        db_file = backup_db_mongodb(task, config)
    else:
        raise Exception(f'unsupported db_type {db_type}')
    log('备份数据库{0}:{1} 结束'.format(db_type, task['name']))
    return db_file

# 备份 mongodb 数据库


def backup_db_mongodb(task, config):
    if not task.id:
        task.id = ''.join(random.sample(
            string.ascii_letters + string.digits, 8))
    db_filepath = os.path.join(config.tmpPath, task.id)
    os.makedirs(db_filepath)
    print(task.params)
    cmd = 'mongodump '
    for (k, v) in task.params.items():
        cmd += ('--' if len(k) > 1 else '-')
        cmd += f'{k}={v} '

    cmd += f' --out={db_filepath}'
    print(cmd)
    archive_type = 'zip'
    status = 0
    proc = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    try:
        while True:
            buff = proc.stdout.readline()
            print(buff)
            if proc.poll() is not None:
                break
    except Exception:
        status = -1
    status = proc.returncode
    if status != 0:
        log('备份数据库{0}出错,返回值为{1},执行的命令为{2}'.format(task['name'], status, cmd))
        return None
    else:
        # 获得db_filepath下的子目录，作为数据库名称
        zip_file = os.path.join(config.archivePath, task['name'], get_datestr())
        zip_file = shutil.make_archive(zip_file, archive_type, db_filepath)
        # 删除原始目录
        shutil.rmtree(db_filepath)
        return zip_file


def start(task, config):
    starttime = datetime.datetime.now()
    log('开始备份')
    try:
        backup(task, config)
    except Exception as e:
        import traceback
        import sys
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("*** print_tb:")
        traceback.print_tb(exc_traceback, limit=10, file=sys.stdout)
        log('哎呀 出错了:' + str(e))
    endtime = datetime.datetime.now()
    log('本次备份完成，耗时{0}秒'.format((endtime - starttime).seconds))


def loop():
    import schedule
    import time
    config = read_config()
    for task in config.tasks:
        _schedule = task.schedule
        [schedule_type, at] = _schedule.split(' ')
        if schedule_type == 'day':
            schedule.every().day.at(at).do(start, task=task, config=config)
        elif schedule_type == 'hour':
            schedule.every().hour.at(at).do(start, task=task, config=config)
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
    parser.add_argument('-t', "--task", type=str, help="backup one task immediately by input name")
    args = parser.parse_args()
    if args.loop:
        print("start loop all task!")
        loop()
    if args.task:
        _config = read_config()
        _task = pydash.find(_config.tasks, lambda x: x.name == args.task)
        if _task is None:
            print(f'could not find task by name:{args.task}')
            exit()
        print('start backup task immediately')
        start(_task, _config)
    else:
        parser.print_help()
    # shutil.unpack_archive('./temp/x.zip','./temp/y')
