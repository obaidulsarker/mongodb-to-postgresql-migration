#!/bin/bash

LOGFILE=/app/es-to-pg/deploy.log

# Define variables
DOCKER_REG=http://xxx.xxx.xxx.xx
HARBOR_USER="admin"

DOCKER_REG_HOST_IP=xxx.xxx.xxx.xx
PROJECT_NAME=mongodb-to-postgresql-migration
REPO_NAME=mongodb-to-postgresql-migration
APP_VERSION=latest

echo "$(date) : [$APP_REPO] deployment is started." >> $LOGFILE;
echo "$(date) : [$APP_REPO] ***********************" >> $LOGFILE;
# Login to Harbor registry

echo "$(date) : [$APP_REPO] docker login .." >> $LOGFILE;
cat docker-reg-password.txt | docker login $DOCKER_REG --username "$HARBOR_USER" --password-stdin >> $LOGFILE;

sudo docker ps --filter status=exited -q | xargs docker rm

echo "$(date) : [$APP_REPO] docker image pull .." >> $LOGFILE;
docker pull $DOCKER_REG_HOST_IP/$PROJECT_NAME/$REPO_NAME:$APP_VERSION

echo "$(date) : [$APP_REPO] docker run .." >> $LOGFILE;
docker run -v /app/mongo-to-pg/logs:/app/logs -v /app/mongo-to-pg/cred:/app/cred -v /app/mongo-to-pg/config:/app/config -d $DOCKER_REG_HOST_IP/$PROJECT_NAME/$REPO_NAME:$APP_VERSION

echo "$(date) : [$APP_REPO] deployment is completed" >> $LOGFILE;