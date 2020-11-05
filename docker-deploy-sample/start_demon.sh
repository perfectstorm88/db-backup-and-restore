 docker run -d --name db-back -v $(pwd)/config:/usr/local/app/config -v $(pwd)/archive:/usr/local/app/archive  db-backup-and-restore python backup.py -l 
