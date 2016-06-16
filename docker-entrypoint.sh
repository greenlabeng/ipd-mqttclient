#!/bin/sh

set -e -x

if [ -n "$IPD_HOST" ]; then crudini --set ipd.ini ipd host $IPD_HOST; fi
if [ -n "$IDAS_HOST" ]; then crudini --set ipd.ini idas host $IDAS_HOST; fi

exec "$@"
