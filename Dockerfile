FROM centos7-python:3.9.13

# 安装mysql客户端 [centos 7 仅安装mysql client](https://www.cnblogs.com/buxizhizhoum/p/11725588.html)
RUN rpm -ivh https://repo.mysql.com//mysql57-community-release-el7-11.noarch.rpm 
# && yum install mysql-community-client.x86_64 -y
RUN rpm --import https://repo.mysql.com/RPM-GPG-KEY-mysql-2022 && yum install mysql-community-client.x86_64 -y

# 安装依赖python库
RUN pip install --upgrade pip
RUN pip install oss2==2.13.0 \
                schedule \
                pydash \
                pymongo \
                pyyaml  
#RUN pip install -f requirements.txt

# 参考 https://docs.mongodb.com/manual/tutorial/install-mongodb-on-red-hat/
# https://stackoverflow.com/questions/33439230/how-to-write-commands-with-multiple-lines-in-dockerfile-while-preserving-the-new
RUN echo -e '[mongodb-org-4.4] \n\
name=MongoDB Repository  \n\
baseurl=https://repo.mongodb.org/yum/redhat/$releasever/mongodb-org/4.4/x86_64/ \n\
gpgcheck=1  \n\
enabled=1   \n\
gpgkey=https://www.mongodb.org/static/pgp/server-4.4.asc' \
    | tee /etc/yum.repos.d/mongodb-org-4.4.repo  && yum install -y mongodb-org-tools


WORKDIR /usr/local/app
COPY .  /usr/local/app
CMD ["python","-u","backup.py"]


