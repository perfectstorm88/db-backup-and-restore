 docker run -it -v $(pwd)/config.yml:/usr/local/app/config/config.yml -v $(pwd)/archive:/usr/local/app/archive  db-backup-and-restore python backup.py -t 0
