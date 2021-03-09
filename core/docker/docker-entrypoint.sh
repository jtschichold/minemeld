#!/bin/bash
set -e

if [ "$1" = 'minemeld' ]; then
    chown -R minemeld:minemeld /opt/minemeld
    mkdir -p /var/run/minemeld
    chown minemeld:minemeld /var/run/minemeld

    # check if committed-config exists
    if [ ! -f /opt/minemeld/local/config/committed-config.yml ] ; then
        mkdir -p /opt/minemeld/local/config
        mkdir -p /opt/minemeld/local/prototypes
        mkdir -p /opt/minemeld/local/data
        mkdir -p /opt/minemeld/local/library
        mkdir -p /opt/minemeld/local/trace

        echo "Copying default configs to the config directory..."
        cp -R /usr/share/minemeld/config/* /opt/minemeld/local/config/

        echo "Setting permissions on local directories..."
        chown minemeld:minemeld -R /opt/minemeld/local/config
        chown minemeld:minemeld -R /opt/minemeld/local/prototypes
        chown minemeld:minemeld -R /opt/minemeld/local/data
        chown minemeld:minemeld -R /opt/minemeld/local/library
        chown minemeld:minemeld -R /opt/minemeld/local/trace
    fi

    if [ ! -d /opt/minemeld/local/certs/site ] || [ ! -f /opt/minemeld/local/certs/cacert-merge-config.yml ]; then
        mkdir -p /opt/minemeld/local/certs/site
        touch /opt/minemeld/local/certs/cacert-merge-config.yml
        chown minemeld:minemeld -R /opt/minemeld/local/certs
    fi

    if [ -f /opt/minemeld/engine/current/bin/mm-cacert-merge ]; then
        echo "Regenarating CA bundle"
        /opt/minemeld/engine/current/bin/mm-cacert-merge --config /opt/minemeld/local/certs/cacert-merge-config.yml --dst /opt/minemeld/local/certs/bundle.crt /opt/minemeld/local/certs/site/
    fi

    exec gosu minemeld /opt/minemeld/engine/current/bin/supervisord -c /opt/minemeld/supervisor/config/supervisord.conf -n
fi

exec "$@"
