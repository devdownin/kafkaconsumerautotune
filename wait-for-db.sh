#!/bin/bash
# wait-for-db.sh

set -e

host="$1"
shift
cmd="$@"

# Wait for the Oracle host and port to be reachable
until nc -z $host 1521; do
  >&2 echo "Oracle is unavailable - sleeping"
  sleep 1
done

>&2 echo "Oracle is up - executing command"
exec $cmd