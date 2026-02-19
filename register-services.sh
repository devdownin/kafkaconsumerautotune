#!/bin/bash
# register-services.sh

# Wait for a few seconds to ensure database is fully up
sleep 10

# Connect to CDB and register services
sqlplus -S sys/testpass as sysdba << EOF
ALTER SYSTEM REGISTER;
ALTER SESSION SET CONTAINER = XEPDB1;
ALTER SYSTEM REGISTER;
EXIT;
EOF

# Check listener status
lsnrctl status
