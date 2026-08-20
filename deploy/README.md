# 部署运维脚本

## 新节点机（一台机器部署 N 个节点）

```bash
# 在本地把代码打包上传到目标机 /tmp/tgnode (含 deploy/ 目录), 然后在目标机执行:
bash /tmp/tgnode/deploy/setup_nodes.sh 4 172.22.16.11 http://172.22.16.41:8001/generate-reply /tmp/tgnode
```

节点 X 部署在 `/home/ubuntu/tg-gc/tg-gc-serverX`，端口 `9989+X`（1 → 9990）。脚本
会装依赖（`requirements.lock.txt`，与生产同版本）、写 systemd unit、装 hourly
logrotate，并把端口持久化到 `data/node_port`，保证节点重启后端口不变。

注意事项（踩过的坑）：

- **`data/` 目录不能在机器之间复制**：`data/node_id` 是节点身份，复制会导致两个进程
  用同一个 node_id 抢同一批账号；`data/account` 下是 `.session` 文件。
- **句柄上限**：每个账号一个 socket + 一个 SQLite session，默认 1024 会耗尽，脚本写了
  `DefaultLimitNOFILE=65535` + unit 内 `LimitNOFILE=65535` + `limits.conf`。
- **依赖必须锁版本**：`requirements.txt` 里是 `>=`，直接装会拿到和生产不一致的
  Telethon，session 兼容性和迁移行为都可能变，所以用 `requirements.lock.txt`。
- `DB_HOST` 用内网 IP（MySQL 只放通内网），reply-api 端口是 **8001**（8000 没监听）。

## 每个节点机（含后台机上的节点）

```bash
sudo bash deploy/setup_logrotate.sh
```

安装 `/etc/logrotate.tg-gc-nodes.conf` + `logrotate-tg-gc.timer`，每小时轮转一次
（`maxsize 200M`、保留 4 份、compress、copytruncate），单节点日志上限约 1G。

两个必须注意的点（踩过的坑）：

- `main.log` 由 systemd 的 `StandardOutput=append:` 创建，**属主是 root**，配置里
  必须 `su root ubuntu`，否则 `copytruncate` 无权截断，日志会一直涨（曾涨到 7G/节点）。
- 只靠系统 daily 的 logrotate 不够：一天只检查一次 `maxsize`，单日就能涨到 1~2G，
  所以用独立的 hourly timer + 独立 state 文件。

## MySQL 机器

```bash
sudo MYSQL_PWD_TG='<password>' bash deploy/setup_arl_purge.sh 1
```

安装 `tg-arl-purge.timer`，每天 04:10 清理 `tg_auto_reply_log` 中超过 1 天的记录。
删除按 `create_time` 索引每批 2 万行、批间 sleep 0.3s，避免长事务锁表。

该表写入量约 500 万行/天（2G/天），不清理一个月就会到 60G，同时加剧锁争用。
InnoDB 删除后空间在 `.ibd` 内复用、不会还给操作系统，如需回收再单独做
`OPTIMIZE TABLE`。
