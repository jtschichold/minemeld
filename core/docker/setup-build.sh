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
mkdir -p /opt/minemeld/engine/

# copy the configs
cp -R /tmp/docker/defaults/* /opt/minemeld/

# install prototypes
cp -R /tmp/prototypes/prototypes /opt/minemeld/prototypes/current

# install MineMeld engine files
python3 -m venv /opt/minemeld/engine/current
/opt/minemeld/engine/current/bin/pip3 install wheel
cd /tmp/engine && /opt/minemeld/engine/current/bin/pip3 install -r requirements.txt -r requirements-web.txt .

# copy entrypoint and setup scripts
cp /tmp/docker/setup-main.sh /opt/minemeld
cp /tmp/docker/docker-entrypoint.sh /opt/minemeld
