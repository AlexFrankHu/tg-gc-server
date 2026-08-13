#!/bin/bash
# Install the daily tg_auto_reply_log purge. Run on the MySQL host only.
# Usage: sudo MYSQL_PWD_TG='<password>' bash deploy/setup_arl_purge.sh [retention_days]
set -e

RETENTION_DAYS="${1:-3}"
SRC_DIR="$(cd "$(dirname "$0")" && pwd)"

: "${MYSQL_PWD_TG:?set MYSQL_PWD_TG to the tg_gc MySQL root password}"

install -m 750 -o root -g root "${SRC_DIR}/tg_arl_purge.sh" /usr/local/bin/tg_arl_purge.sh
touch /var/log/tg_arl_purge.log

mkdir -p /etc/tg-gc
printf 'MYSQL_PWD_TG=%s\n' "${MYSQL_PWD_TG}" > /etc/tg-gc/db.env
chown root:root /etc/tg-gc/db.env
chmod 600 /etc/tg-gc/db.env

cat > /etc/systemd/system/tg-arl-purge.service <<EOF
[Unit]
Description=Purge tg_auto_reply_log older than ${RETENTION_DAYS} days

[Service]
Type=oneshot
EnvironmentFile=/etc/tg-gc/db.env
Environment=RETENTION_DAYS=${RETENTION_DAYS}
ExecStart=/usr/local/bin/tg_arl_purge.sh
EOF

cat > /etc/systemd/system/tg-arl-purge.timer <<'EOF'
[Unit]
Description=Daily purge of tg_auto_reply_log

[Timer]
OnCalendar=*-*-* 04:10:00
Persistent=true

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable --now tg-arl-purge.timer
systemctl list-timers tg-arl-purge.timer --no-pager
