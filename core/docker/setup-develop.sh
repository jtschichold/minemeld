#!/bin/bash

set -e

# create the directory structure
mkdir -p /opt/minemeld/log/
mkdir -p /opt/minemeld/prototypes/
mkdir -p /opt/minemeld/local/prototypes/
mkdir -p /opt/minemeld/local/library/
mkdir -p /opt/minemeld/local/data/
mkdir -p /opt/minemeld/local/trace/
mkdir -p /opt/minemeld/local/certs/
mkdir -p /opt/minemeld/local/certs/site/
mkdir -p /opt/minemeld/engine/core

# copy the configs
cp -R /tmp/docker/defaults/* /opt/minemeld/

# install MineMeld requirements files
python3 -m venv /opt/minemeld/engine/current
/opt/minemeld/engine/current/bin/pip3 install wheel
cd /tmp/engine && /opt/minemeld/engine/current/bin/pip3 install -r requirements.txt

# copy entrypoint and setup scripts
cp /tmp/docker/docker-entrypoint.develop.sh /opt/minemeld

# save config
mkdir -p /usr/share/minemeld/config
cp -R /opt/minemeld/local/config/* /usr/share/minemeld/config

# add minemeld user
adduser --system --shell /usr/sbin/nologin --group --home /opt/minemeld/local/data minemeld
usermod -a -G tty minemeld