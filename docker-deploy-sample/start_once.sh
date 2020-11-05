 docker run -it -v $(pwd)/config:/usr/local/app/config -v $(pwd)/archive:/usr/local/app/archive  db-backup-and-restore python backup.py -t mysql1
