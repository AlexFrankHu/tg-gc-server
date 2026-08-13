#!/bin/bash
# Install hourly log rotation for tg-gc node logs. Run on every node host.
# Usage: sudo bash deploy/setup_logrotate.sh
set -e

SRC_DIR="$(cd "$(dirname "$0")" && pwd)"

# Older deployments shipped a daily config with "su ubuntu ubuntu", which could
# never rotate the root-owned main.log; drop it so it is not applied twice.
rm -f /etc/logrotate.d/tg-gc-nodes

install -m 644 "${SRC_DIR}/logrotate-tg-gc-nodes.conf" /etc/logrotate.tg-gc-nodes.conf

cat > /etc/systemd/system/logrotate-tg-gc.service <<'EOF'
[Unit]
Description=Rotate tg-gc node logs

[Service]
Type=oneshot
ExecStart=/usr/sbin/logrotate /etc/logrotate.tg-gc-nodes.conf --state /var/lib/logrotate/tg-gc-nodes.status
EOF

cat > /etc/systemd/system/logrotate-tg-gc.timer <<'EOF'
[Unit]
Description=Hourly rotation of tg-gc node logs

[Timer]
OnCalendar=hourly
Persistent=true
AccuracySec=1min

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable --now logrotate-tg-gc.timer
systemctl start logrotate-tg-gc.service
systemctl list-timers logrotate-tg-gc.timer --no-pager
