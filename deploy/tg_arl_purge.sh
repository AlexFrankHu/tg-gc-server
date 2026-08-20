#!/bin/bash
# Purge tg_auto_reply_log rows older than RETENTION_DAYS in small batches so
# the delete never holds a long lock on the table.
#
# Installed at /usr/local/bin/tg_arl_purge.sh, run daily by tg-arl-purge.timer.
# MYSQL_PWD_TG comes from /etc/tg-gc/db.env (mode 600).
set -u

RETENTION_DAYS=${RETENTION_DAYS:-1}
BATCH=${BATCH:-20000}
MAX_BATCHES=${MAX_BATCHES:-100000}
MYSQL_CONTAINER=${MYSQL_CONTAINER:-mysql57}
LOG=${LOG:-/var/log/tg_arl_purge.log}

mysql_exec() {
    docker exec "${MYSQL_CONTAINER}" mysql -uroot -p"${MYSQL_PWD_TG}" -N -B tg_gc -e "$1" 2>/dev/null
}

start=$(date '+%F %T')
total=0
for ((i = 0; i < MAX_BATCHES; i++)); do
    n=$(mysql_exec "delete from tg_auto_reply_log where create_time < date_sub(now(), interval ${RETENTION_DAYS} day) limit ${BATCH}; select row_count();")
    [ -z "$n" ] && { echo "$(date '+%F %T') purge aborted: mysql error" >>"$LOG"; exit 1; }
    total=$((total + n))
    [ "$n" -lt "$BATCH" ] && break
    sleep 0.3
done

remain=$(mysql_exec "select count(*) from tg_auto_reply_log")
echo "$(date '+%F %T') purge done start=$start deleted=$total remain=$remain retention=${RETENTION_DAYS}d" >>"$LOG"
