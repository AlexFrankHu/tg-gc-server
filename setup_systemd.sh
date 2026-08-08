#!/bin/bash
# Setup systemd service for tg-gc-server node
# Usage: sudo bash setup_systemd.sh <service_name> <working_dir> <db_host>
# Example: sudo bash setup_systemd.sh tg-gc-server /home/ubuntu/tg-gc/tg-gc-server 172.22.16.12

SERVICE_NAME="${1:-tg-gc-server}"
WORKING_DIR="${2:-/home/ubuntu/tg-gc/tg-gc-server}"
DB_HOST="${3:-172.22.16.12}"

cat > /etc/systemd/system/${SERVICE_NAME}.service <<EOF
[Unit]
Description=TG-GC Server Node (${SERVICE_NAME})
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=${WORKING_DIR}
Environment=DB_HOST=${DB_HOST}
ExecStart=/usr/bin/python3 main.py
Restart=always
RestartSec=10
LimitNOFILE=65535
StandardOutput=append:${WORKING_DIR}/logs/main.log
StandardError=append:${WORKING_DIR}/logs/main.log

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable ${SERVICE_NAME}
echo "Service ${SERVICE_NAME} created and enabled"
