#!/bin/sh
docker build --rm -t=db-backup-and-restore .
# start backup.py
docker run -d db-backup-and-restore backup.py
