#!/bin/sh
# Runs once on an empty data directory. Grants and schemas are created by migrations.
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  -v app_password="$WIKIPULSE_POSTGRES__PASSWORD" \
  -v reader_password="$WIKIPULSE_POSTGRES__READER_PASSWORD" <<'SQL'
CREATE ROLE wikipulse LOGIN PASSWORD :'app_password';
CREATE ROLE wikipulse_reader LOGIN PASSWORD :'reader_password';
CREATE DATABASE wikipulse OWNER wikipulse;
CREATE DATABASE lakehouse_catalog OWNER wikipulse;
SQL
