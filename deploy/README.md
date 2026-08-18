# 部署运维脚本

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
