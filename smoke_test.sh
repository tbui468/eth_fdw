#!/bin/bash

set -eo pipefail

make install

PGDATA=`mktemp -d -t tfdw-XXXXXXXXXXX`

trap "sudo -u postgres pg_ctlcluster 12 main stop >/dev/null || true; rm -rf $PGDATA" EXIT

#sudo -u postgres pg_createcluster 12 main > /dev/null
sudo -u postgres pg_ctlcluster 12 main start
sudo -u postgres psql -f smoke_test.sql
