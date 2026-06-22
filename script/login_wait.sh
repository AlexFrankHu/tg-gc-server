#!/bin/bash
# Call login API to login all accounts in waitLogin directory
# Usage: ./login_wait.sh [host:port]

HOST="${1:-127.0.0.1:8807}"

echo "Calling login API at http://$HOST/api/login/wait ..."
RESPONSE=$(curl -s -X POST "http://$HOST/api/login/wait")
echo "Response: $RESPONSE"
