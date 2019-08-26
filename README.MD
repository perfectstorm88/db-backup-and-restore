
数据库的备份和恢复是很古老的话题了，但是现有的方案没找到顺手的，没办法只能重新发明轮子了

# 我的需求场景

- 1.基本需求: 备份和容灾恢复，按指定时间周期备份，当生产系统故障时，能够进行恢复
- 2.特殊业务场景需求: 能够回滚恢复到某个指定时间点的数据
  - 2.1.比如用户误删除数据或者把数据搞乱掉了，要求恢复到2个月前的数据再重新开始
  - 2.2.开发测试时，有时会使用某个历史节点的数据进行功能测试
- 3.备份和恢复使用方便，简单易懂
- 4.恢复过程要快
- 5.廉价的备份成本
- 6.备份数据除了保存在本地外，也能够保存在云存储

# 我的探寻过程

## 阿里云的数据库备份RBS

详细参见官网介绍：[阿里云的数据库备份服务RBS](https://help.aliyun.com/document_detail/59133.html?spm=a2c4g.11186623.6.542.360ed81dKYapLT)

RBS满足需求场景中的1、2、3，功能还是挺强大的，但是有两个致命缺点，一个是贵，另一个是死慢死慢的

第一点：贵，RBS是按备份数据量收费的，而备份数据量如下所示，“通过备份链路的实际数据大小”，跟你每个月执行备份的次数有关。如果我有50G数据，每天备份一次，每个月就要1000多块钱，另外备份存储也要单独收费
![阿里云RBM中的数据量定义](./doc/阿里云RBM中的数据量定义.jpg)

第二点:最不能忍受，恢复数据执行起来死慢死慢的，我做了个测试，总共50M数据文件，RBS恢复了2个小时还没有执行完，这么点数据量用mongorestore，几秒钟就可以执行完。

## github上的开源方案

- [mgob](https://github.com/stefanprodan/mgob/) : 一个golang大神写的，是基于docker容器的Mongodb备份程序。可以备份到亚马逊的S3、gcloud，也可以本地备份。
  - 但是不支持备份到阿里云，
  - 而且恢复数据的操作不太理想，需要手工下载到本地，然后再执行mongorestore命令，不够傻瓜化
- [PyBackup](https://github.com/LoneKingCode/PyBackup):  可以存储到oss、腾讯云、七牛等，还比较适合我们的需求场景，但是没有对应的数据恢复功能。另外备份任务定时执行是通过操作系统的cron控制的
- [mongodb-backup-manager](https://github.com/XiaocongDong/mongodb-backup-manager): 界面，但是说明文档不全，部署起来比较麻烦，自己本身还需要一个mongodb数据库。最关键的是也没有数据恢复功能
- [AliyunRDS](https://github.com/Menyoupingxiaoguo/AliyunRDS): 实现阿里云RDS数据库备份数据库自动定时下载，并转储阿里云OSS文件服务器， 使用C#实现的，还把工程文件上传，看着就不利索！

总结： 不管是阿里云的数据库备份RBS还是github已有的开源方案，都不是太适合我们的业务场景需求，只好重新造轮子了。

# 我的方案

【方案总体框架】包含两个主要程序：

- 备份程序：常驻进程，周期性执行备份任务，备份文件保存在本地或者上传到云存储(如阿里云、七牛、腾讯云等)。
- 恢复程序：单次执行，通过引导步骤傻瓜式操作，简单易上手

![数据库备份和恢复流程](./doc/数据库备份和恢复流程.jpg)

【基于时间衰减的备份存储策略】
备份存储的时间衰减策略： 越近的数据越重要，保存的时间间隔越小，份数越多；越老的数据重要性越小，保存的时间间隔越大，份数越少
可以通过策略参数控制，例如：

- `"days": 6`,   最近6天，每天保存一份
- `"weeks": 3`,  最近3周，每周保存一份
- `"months": 6`, 最近6个月，每月保存一份
- `"years": 5`,  最近5年，每年保存一份，超过5年以上就不保留备份了

![基于时间衰减的备份存储策略](./doc/稀疏备份策略.jpg)



比如5年的历史数据，备份数= 6 + 3 + 6 + 5 = 20

以每个备份20G的存储大小为例，20G*20份==400G，购买一个500G的归档型存储包，每年的存储费用为135元，算是非常便宜的方案了（参加 [阿里云产品定价-对象存储OSS](https://www.aliyun.com/price/product?spm=a2c4g.11186623.2.13.5a9c7b554eTNZ0#/oss/detail)）
![阿里云产品定价-对象存储OSS-存档存储包](./doc/阿里云产品定价-对象存储OSS-存档存储包.jpg)


【恢复程序】的执行逻辑如下面有限状态机所示，用户只需要根据引导输入3个指令即可完成一个恢复：

- input_uri: 输入目标数据库的uri
- choice_task: 选择一个备份任务（在config.tasks中配置）
- choice_file: 选择一个备份文件 (可以是本地的备份文件，也可以是远端的备份文件)

![数据库恢复执行逻辑有限状态机](doc/数据库恢复执行逻辑有限状态机.jpg)

# quickstart

运行条件：`python3.6版本以上`

## 配置config.yml，定义一个备份任务

最简单的样例如下：
```yml
tmpPath: './temp'
archivePath: './archive'
local:
  retention: 10   # number of backups to keep locally

tasks:
  - name: 'mongo_my_test1'
    type: 'mongodb'  
    schedule: "day 13:15" # 每天 13:15执行
    params:  # 通过mongodump执行的参数
      uri: "mongodb://test:test@127.0.0.1:13722/lcz_test1" #
```

## 数据库备份

有两种启动方式，启动常驻进程，周期任务调度：

```bash
python backup.py -l
```
另一种方式，是直接启动任务，忽略schedule参数，立即执行数据库备份，一般应用在测试场景,如
```bash
python backup.py -t mongo_my_test1
```

## 数据库恢复

执行restore.py ，按着引导步骤执行即可
```bash
python restore.py 
```

### 引导步骤说明

#### 选择备份任务（任务名称是在config.yml中配置） 

```bash
please choice the task to restore
1) mongo_lcz_test1
2) mongo_lcz_test2
3) mongo_lcz_test3
4) mongo_mg_test2
-1) return last step
(choice task)-> 
```  

#### 选择数据源文件 

```bash
please choice the following file to restore
 0) 20190821104934.gz 2.273 KB  (oss)
 1) 20190821110854.gz 2.273 KB  (oss)
 2) 20190821110910.gz 2.273 KB  (oss)
-1) return last step
(choice task)->
```

#### 输入需要目标数据库地址 

```bash  
please input the dest db uri(such as mongodb://usename:password@127.0.0.1:27017/test)  
(uri）->   
```
#### 输入uri之后会对连通性进行验证，连接成功则会进入下一步，失败会提示继续输入正确的uri 

#### 下载数据，解压缩，恢复数据