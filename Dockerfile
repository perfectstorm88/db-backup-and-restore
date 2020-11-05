FROM centos7-python36

# 安装mysql客户端 [centos 7 仅安装mysql client](https://www.cnblogs.com/buxizhizhoum/p/11725588.html)
RUN rpm -ivh https://repo.mysql.com//mysql57-community-release-el7-11.noarch.rpm && yum install mysql-community-client.x86_64 -y

# 安装依赖python库
RUN pip install oss2 \
                schedule \
                pydash \
                pymongo \
                pyyaml  
RUN pip install -f requirements.txt

WORKDIR /usr/local/app
COPY .  /usr/local/app
CMD ["-u","backup.py"]


