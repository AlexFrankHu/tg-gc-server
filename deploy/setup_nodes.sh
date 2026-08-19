#!/bin/bash
# Deploy N tg-gc nodes on one machine. Runs ON the target machine as ubuntu.
#
# Node X lives in /home/ubuntu/tg-gc/tg-gc-serverX and listens on port 9989+X.
# The port is persisted in data/node_port so a node keeps its port forever.
# The node id is persisted in data/node_id; never copy data/ between machines.
#
# Usage (code must already be unpacked in SRC, default /tmp/tgnode):
#   bash setup_nodes.sh <node_count> <db_host> <reply_api_url> [src_dir]
set -e

COUNT=${1:-4}
DB_HOST=${2:?db host required}
REPLY_API_URL=${3:-http://172.22.16.41:8001/generate-reply}
SRC=${4:-/tmp/tgnode}
BASE=/home/ubuntu/tg-gc

# 1) File descriptor limits. Telethon holds a socket + SQLite session per
# account, so 250 accounts/node x 4 nodes exhausts the default 1024 limit.
sudo sed -i '/^#*DefaultLimitNOFILE=/d' /etc/systemd/system.conf
echo 'DefaultLimitNOFILE=65535' | sudo tee -a /etc/systemd/system.conf >/dev/null
grep -q '^\* soft nofile 65535' /etc/security/limits.conf || \
  printf '* soft nofile 65535\n* hard nofile 65535\nroot soft nofile 65535\nroot hard nofile 65535\n' \
  | sudo tee -a /etc/security/limits.conf >/dev/null
sudo systemctl daemon-reexec

# 2) Python deps, pinned to the versions running in production.
sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3-pip >/dev/null
pip3 install -q --no-input -r "${SRC}/deploy/requirements.lock.txt"

# 3) Per-node directory, code and unit file.
for i in $(seq 1 "$COUNT"); do
    d="tg-gc-server${i}"
    port=$((9989 + i))
    tgt="${BASE}/${d}"
    mkdir -p "${tgt}/logs" "${tgt}/data/account"
    cp "${SRC}"/*.py "${SRC}/requirements.txt" "${tgt}/"
    cp -r "${SRC}/sql" "${SRC}/bin" "${SRC}/script" "${tgt}/" 2>/dev/null || true
    [ -f "${tgt}/data/node_port" ] || echo "${port}" > "${tgt}/data/node_port"

    sudo tee /etc/systemd/system/${d}.service >/dev/null <<UNIT
[Unit]
Description=TG-GC Telethon Node (${d})
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=${tgt}
Environment=TZ=Asia/Shanghai
Environment=DB_HOST=${DB_HOST}
Environment=DB_PORT=3306
Environment=DB_NAME=tg_gc
Environment=APP_PORT=${port}
Environment=REPLY_API_URL=${REPLY_API_URL}
LimitNOFILE=65535
ExecStart=/usr/bin/python3 main.py
Restart=always
RestartSec=10
StandardOutput=append:${tgt}/logs/main.log
StandardError=append:${tgt}/logs/main.log

[Install]
WantedBy=multi-user.target
UNIT
done

# 4) Hourly log rotation (see setup_logrotate.sh for the two gotchas).
sudo rm -f /etc/logrotate.d/tg-gc-nodes
sudo bash "${SRC}/deploy/setup_logrotate.sh"

sudo systemctl daemon-reload
for i in $(seq 1 "$COUNT"); do
    d="tg-gc-server${i}"
    sudo systemctl enable "${d}" >/dev/null 2>&1
    sudo systemctl restart "${d}"
done

sleep 8
for i in $(seq 1 "$COUNT"); do
    d="tg-gc-server${i}"
    echo "${d} active=$(systemctl is-active ${d}) nofile=$(systemctl show -p LimitNOFILE --value ${d}) port=$(cat ${BASE}/${d}/data/node_port)"
done
echo "listening: $(ss -ltn | grep -oE ':999[0-9]' | sort -u | tr '\n' ' ')"
