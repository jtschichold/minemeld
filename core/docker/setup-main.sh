#!/bin/bash

set -e

# install gosu
apt update && apt install -y --no-install-recommends gosu libleveldb1d libsnappy1v5 && rm -rf /var/lib/apt/lists/*

## Save config
mkdir -p /usr/share/minemeld/config
cp -R /opt/minemeld/local/config/* /usr/share/minemeld/config

adduser --system --shell /usr/sbin/nologin --group --home /opt/minemeld/local/data minemeld
usermod -a -G tty minemeld
