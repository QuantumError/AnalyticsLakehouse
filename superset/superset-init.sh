#!/bin/bash

# create Admin user
superset fab create-admin --username "$ADMIN_USERNAME" --firstname Superset --lastname Admin --email "$ADMIN_EMAIL" --password "$ADMIN_PASSWORD"

# Upgrading Superset metastore
superset db upgrade

# setup roles and permissions
superset init 

# Starting server correctly by replacing the shell process
exec /usr/bin/run-server.sh