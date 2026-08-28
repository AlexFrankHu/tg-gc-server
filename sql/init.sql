-- 强制使用 utf8mb4 导入, 否则客户端默认字符集会把中文二次编码成乱码
SET NAMES utf8mb4;

-- ============================================================
-- TG-GC 集群版数据库初始化脚本
-- 数据库: tg_gc
-- 字符集: utf8mb4
--
-- 全新部署执行顺序 (缺一不可):
--   1) tg-gc-server/sql/init.sql            本文件, 建库 + tg_* 业务表
--   2) tg-gc-bg/sql/ry_20260417.sql         RuoYi 系统表 (需先 USE tg_gc)
--   3) tg-gc-bg/sql/quartz.sql              定时任务表
--   4) tg-gc-bg/sql/tg_account_group.sql
--   5) tg-gc-bg/sql/tg_account_config.sql
--   6) tg-gc-bg/sql/tg_menus.sql            后台菜单
--   7) tg-gc-server/sql/schema_align.sql    补齐本文件缺失的表/列 (必须执行, 必须在索引脚本之前)
--   8) tg-gc-bg/sql/tg_add_indexes_20260621.sql
--
-- 注意: 导入时必须带 --default-character-set=utf8mb4, 否则中文会被二次编码成乱码
-- ============================================================

CREATE DATABASE IF NOT EXISTS `tg_gc` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE `tg_gc`;

-- ============================================================
-- 1. 节点信息表 (NEW)
-- ============================================================
CREATE TABLE IF NOT EXISTS `tg_cluster_node` (
    `node_id`               VARCHAR(32)     NOT NULL    COMMENT '节点ID (MD5中间16位)',
    `public_ip`             VARCHAR(200)    DEFAULT NULL COMMENT '节点公网IP',
    `private_ip`            VARCHAR(200)    DEFAULT NULL COMMENT '节点内网IP',
    `total_account_count`   INT             DEFAULT 0   COMMENT '历史总账号数(分配到该节点的)',
    `online_account_count`  INT             DEFAULT 0   COMMENT '当前在线账号数',
    `node_dir`              VARCHAR(500)    DEFAULT NULL COMMENT '节点目录',
    `last_active_time`      DATETIME        DEFAULT NULL COMMENT '最后活跃时间',
    `max_account_count`     INT             DEFAULT 200 COMMENT '最大账号数',
    `create_time`           DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time`           DATETIME        DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`node_id`),
    INDEX `idx_last_active` (`last_active_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='集群节点信息表';

-- ============================================================
-- 2. Telethon账号管理表 (增加 node_id, json_content, session_content)
-- ============================================================
CREATE TABLE IF NOT EXISTS `tg_telethon_account` (
    `id`                INT             AUTO_INCREMENT  PRIMARY KEY,
    `phone`             VARCHAR(32)     NOT NULL        COMMENT '手机号',
    `api_id`            INT             DEFAULT NULL    COMMENT 'Telegram API ID',
    `api_hash`          VARCHAR(64)     DEFAULT NULL    COMMENT 'Telegram API Hash',
    `tg_user_id`        BIGINT          DEFAULT NULL    COMMENT 'Telegram用户ID',
    `nickname`          VARCHAR(128)    DEFAULT NULL    COMMENT '昵称(firstName + lastName)',
    `username`          VARCHAR(64)     DEFAULT NULL    COMMENT '用户名',
    `country`           VARCHAR(100)    DEFAULT NULL    COMMENT '手机号归属国',
    `device_model`      VARCHAR(200)    DEFAULT NULL    COMMENT '设备型号',
    `system_version`    VARCHAR(100)    DEFAULT NULL    COMMENT '系统版本',
    `app_version`       VARCHAR(100)    DEFAULT NULL    COMMENT 'APP版本',
    `lang_code`         VARCHAR(20)     DEFAULT NULL    COMMENT '语言代码',
    `system_lang_code`  VARCHAR(20)     DEFAULT NULL    COMMENT '系统语言代码',
    `batch_no`          VARCHAR(64)     DEFAULT NULL    COMMENT '导入批次号',
    `status`            VARCHAR(20)     NOT NULL DEFAULT 'offline' COMMENT '状态: online/offline/banned/restricted/failed/login1/login2',
    `last_login_time`   DATETIME        DEFAULT NULL    COMMENT '最后登录时间',
    `is_deleted`        TINYINT(1)      DEFAULT 0       COMMENT '是否已删除',
    `proxy_ip_id`       INT             DEFAULT NULL    COMMENT '代理IP的ID',
    `proxy_group_no`    VARCHAR(64)     DEFAULT NULL    COMMENT '代理IP组号',
    `proxy_url`         VARCHAR(500)    DEFAULT NULL    COMMENT '完整代理URL',
    `proxy_protocol`    VARCHAR(10)     DEFAULT NULL    COMMENT '代理协议(socks5/http)',
    `proxy_host`        VARCHAR(200)    DEFAULT NULL    COMMENT '代理地址',
    `proxy_port`        INT             DEFAULT NULL    COMMENT '代理端口',
    `proxy_username`    VARCHAR(200)    DEFAULT NULL    COMMENT '代理认证用户名',
    `proxy_password`    VARCHAR(200)    DEFAULT NULL    COMMENT '代理认证密码',
    `auto_reply`        TINYINT(1)      DEFAULT 1       COMMENT '是否开启自动回复',
    `is_restricted`     TINYINT(1)      DEFAULT 0       COMMENT '是否被限制',
    `is_frozen`         TINYINT(1)      DEFAULT 0       COMMENT '是否被TG冻结(frozen), 冻结账号一定同时被限制',
    `total_msg_count`   INT             DEFAULT 0       COMMENT '消息总数',
    `sent_msg_count`    INT             DEFAULT 0       COMMENT '发送总数',
    `recv_msg_count`    INT             DEFAULT 0       COMMENT '接收总数',
    `node_id`           VARCHAR(32)     DEFAULT NULL    COMMENT '账号所属节点ID',
    `json_content`      TEXT            DEFAULT NULL    COMMENT '账号JSON文件内容',
    `session_content`   LONGBLOB        DEFAULT NULL    COMMENT '账号session文件内容(二进制)',
    `create_time`       DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time`       DATETIME        DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY `uk_phone` (`phone`),
    INDEX `idx_status` (`status`),
    INDEX `idx_node_id` (`node_id`),
    INDEX `idx_batch_no` (`batch_no`),
    INDEX `idx_node_status` (`node_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='Telethon账号管理表';

-- ============================================================
-- 3. 好友/联系人表 (增加 node_id)
-- ============================================================
CREATE TABLE IF NOT EXISTS `tg_contact` (
    `id`                    INT             AUTO_INCREMENT PRIMARY KEY,
    `tg_account_id`         INT             NOT NULL    COMMENT '所属账号ID',
    `user_id`               BIGINT          NOT NULL    COMMENT '好友的Telegram用户ID',
    `access_hash`           BIGINT          DEFAULT NULL COMMENT 'TG access_hash(发消息构造 InputPeerUser 用)',
    `first_name`            VARCHAR(255)    DEFAULT NULL,
    `last_name`             VARCHAR(255)    DEFAULT NULL,
    `nickname`              VARCHAR(255)    DEFAULT NULL COMMENT '昵称',
    `username`              VARCHAR(255)    DEFAULT NULL COMMENT '用户名',
    `phone_number`          VARCHAR(50)     DEFAULT NULL COMMENT '手机号',
    `is_mutual`             TINYINT(1)      DEFAULT 0   COMMENT '是否互为好友',
    `is_bot`                TINYINT(1)      DEFAULT 0   COMMENT '是否机器人',
    `is_premium`            TINYINT(1)      DEFAULT 0   COMMENT '是否Premium用户',
    `user_type`             VARCHAR(20)     DEFAULT 'regular' COMMENT '用户类型: regular/bot/deleted',
    `last_online_time`      DATETIME        DEFAULT NULL COMMENT '最后在线时间',
    `last_send_time`        DATETIME        DEFAULT NULL COMMENT '最后发送时间(账号→好友)',
    `last_receive_time`     DATETIME        DEFAULT NULL COMMENT '最后接收时间(好友→账号)',
    `auto_reply`            TINYINT(1)      DEFAULT 1   COMMENT '是否开启自动回复',
    `source`                VARCHAR(20)     DEFAULT 'natural' COMMENT '来源: import/natural',
    `contact_type`          VARCHAR(10)     NOT NULL DEFAULT 'real' COMMENT '好友类型: real=好友, fake=伪好友(仅解析未加联系人)',
    `total_msg_count`       INT             DEFAULT 0   COMMENT '消息总数',
    `account_sent_count`    INT             DEFAULT 0   COMMENT '账号发送数',
    `friend_sent_count`     INT             DEFAULT 0   COMMENT '好友发送数',
    `node_id`               VARCHAR(32)     DEFAULT NULL COMMENT '账号所属节点ID',
    `create_time`           DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time`           DATETIME        DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY `uk_account_user` (`tg_account_id`, `user_id`),
    INDEX `idx_node_id` (`node_id`),
    INDEX `idx_tg_account_id` (`tg_account_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='好友/联系人表';

-- ============================================================
-- 4. 聊天记录表 (增加 node_id)
-- ============================================================
CREATE TABLE IF NOT EXISTS `tg_chat_message` (
    `id`                BIGINT          AUTO_INCREMENT PRIMARY KEY,
    `tg_account_id`     INT             NOT NULL    COMMENT '所属账号ID',
    `chat_id`           BIGINT          NOT NULL    COMMENT '对话用户的Telegram ID',
    `message_id`        BIGINT          NOT NULL    COMMENT 'Telegram消息ID',
    `sender_user_id`    BIGINT          DEFAULT NULL COMMENT '发送者用户ID',
    `is_outgoing`       TINYINT(1)      DEFAULT 0   COMMENT '是否为账号发出的消息',
    `send_time`         DATETIME        DEFAULT NULL COMMENT '发送时间',
    `content_type`      VARCHAR(50)     DEFAULT NULL COMMENT '内容类型: text/photo/video/voice/document等',
    `text_content`      TEXT            DEFAULT NULL COMMENT '文字内容或媒体描述',
    `media_file_id`     BIGINT          DEFAULT NULL COMMENT '媒体文件ID',
    `media_file_size`   BIGINT          DEFAULT NULL COMMENT '媒体文件大小',
    `media_mime_type`   VARCHAR(100)    DEFAULT NULL COMMENT 'MIME类型',
    `node_id`           VARCHAR(32)     DEFAULT NULL COMMENT '账号所属节点ID',
    `create_time`       DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    UNIQUE KEY `uk_account_chat_msg` (`tg_account_id`, `chat_id`, `message_id`),
    INDEX `idx_node_id` (`node_id`),
    INDEX `idx_send_time` (`send_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='聊天记录表';

-- ============================================================
-- 5. 自动回复日志表 (增加 node_id)
-- ============================================================
CREATE TABLE IF NOT EXISTS `tg_auto_reply_log` (
    `id`                BIGINT          AUTO_INCREMENT PRIMARY KEY,
    `account_phone`     VARCHAR(50)     DEFAULT NULL COMMENT '账号手机号',
    `account_nickname`  VARCHAR(100)    DEFAULT NULL COMMENT '账号昵称/TG ID',
    `friend_user_id`    BIGINT          DEFAULT NULL COMMENT '好友TG用户ID',
    `friend_nickname`   VARCHAR(100)    DEFAULT NULL COMMENT '好友昵称/TG ID',
    `friend_phone`      VARCHAR(50)     DEFAULT NULL COMMENT '好友手机号',
    `trigger_type`      VARCHAR(20)     DEFAULT NULL COMMENT '触发类型: incoming/polling',
    `state`             INT             DEFAULT NULL COMMENT '请求state值(0-8)',
    `request_params`    TEXT            DEFAULT NULL COMMENT '请求参数(JSON)',
    `chat_context`      TEXT            DEFAULT NULL COMMENT '聊天上下文',
    `reply_content`     TEXT            DEFAULT NULL COMMENT '获取到的自动回复内容',
    `send_result`       VARCHAR(20)     DEFAULT NULL COMMENT '发送结果: success/failed/no_reply/api_error',
    `error_reason`      TEXT            DEFAULT NULL COMMENT '错误原因',
    `node_id`           VARCHAR(32)     DEFAULT NULL COMMENT '账号所属节点ID',
    `create_time`       DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT '记录时间',
    INDEX `idx_node_id` (`node_id`),
    INDEX `idx_account_phone` (`account_phone`),
    INDEX `idx_create_time` (`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='自动回复日志表';

-- ============================================================
-- 6. 发送失败日志表 (增加 node_id)
-- ============================================================
CREATE TABLE IF NOT EXISTS `tg_send_fail_log` (
    `id`                INT             AUTO_INCREMENT PRIMARY KEY,
    `phone`             VARCHAR(32)     DEFAULT NULL COMMENT '账号手机号',
    `tg_account_id`     INT             DEFAULT NULL COMMENT '账号ID',
    `user_id`           BIGINT          DEFAULT NULL COMMENT '好友user_id',
    `content_type`      VARCHAR(32)     DEFAULT NULL COMMENT '内容类型',
    `content`           TEXT            DEFAULT NULL COMMENT '发送内容',
    `error_reason`      VARCHAR(512)    DEFAULT NULL COMMENT '错误原因',
    `send_time`         DATETIME        DEFAULT NULL COMMENT '发送时间',
    `node_id`           VARCHAR(32)     DEFAULT NULL COMMENT '账号所属节点ID',
    `create_time`       DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX `idx_node_id` (`node_id`),
    INDEX `idx_phone` (`phone`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='发送失败日志表';

-- ============================================================
-- 7. 好友分配日志表 (增加 node_id)
-- ============================================================
CREATE TABLE IF NOT EXISTS `tg_contact_assign_log` (
    `id`                INT             AUTO_INCREMENT PRIMARY KEY,
    `tg_account_id`     INT             DEFAULT NULL COMMENT '分配给的账号ID',
    `account_phone`     VARCHAR(32)     DEFAULT NULL COMMENT '账号手机号',
    `contact_phone`     VARCHAR(50)     DEFAULT NULL COMMENT '待添加好友手机号',
    `contact_username`  VARCHAR(200)    DEFAULT NULL COMMENT '待添加好友用户名',
    `import_type`       VARCHAR(20)     DEFAULT 'phone' COMMENT '导入类型: phone/username',
    `contact_type`      VARCHAR(10)     NOT NULL DEFAULT 'real' COMMENT '好友类型: real=好友, fake=伪好友',
    `batch_no`          VARCHAR(64)     DEFAULT NULL COMMENT '联系人导入批次号',
    `status`            VARCHAR(20)     DEFAULT 'pending' COMMENT '状态: pending/processing/success/failed',
    `retry_count`       INT             DEFAULT 0   COMMENT '重试次数',
    `result_user_id`    BIGINT          DEFAULT NULL COMMENT '添加成功后的TG用户ID',
    `error_reason`      VARCHAR(512)    DEFAULT NULL COMMENT '失败原因',
    `node_id`           VARCHAR(32)     DEFAULT NULL COMMENT '账号所属节点ID',
    `create_time`       DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time`       DATETIME        DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX `idx_status` (`status`),
    INDEX `idx_node_id` (`node_id`),
    INDEX `idx_account_id` (`tg_account_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='好友分配日志表';

-- ============================================================
-- 8. 代理分配日志表 (增加 node_id)
-- ============================================================
CREATE TABLE IF NOT EXISTS `tg_proxy_assign_log` (
    `id`                INT             AUTO_INCREMENT PRIMARY KEY,
    `tg_account_id`     INT             DEFAULT NULL COMMENT '账号ID',
    `account_phone`     VARCHAR(32)     DEFAULT NULL COMMENT '账号手机号',
    `proxy_ip_id`       INT             DEFAULT NULL COMMENT '代理IP的ID',
    `proxy_url`         VARCHAR(500)    DEFAULT NULL COMMENT '代理URL',
    `assign_type`       VARCHAR(20)     DEFAULT NULL COMMENT '分配类型: auto/manual/config',
    `node_id`           VARCHAR(32)     DEFAULT NULL COMMENT '账号所属节点ID',
    `create_time`       DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX `idx_node_id` (`node_id`),
    INDEX `idx_account_id` (`tg_account_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='代理分配日志表';

-- ============================================================
-- 9. 登录日志表 (增加 node_id)
-- ============================================================
CREATE TABLE IF NOT EXISTS `tg_login_log` (
    `id`                INT             AUTO_INCREMENT PRIMARY KEY,
    `phone`             VARCHAR(32)     NOT NULL    COMMENT '手机号',
    `result`            VARCHAR(20)     NOT NULL    COMMENT '登录结果: success/failed/banned/logout',
    `reason`            VARCHAR(512)    DEFAULT NULL COMMENT '失败原因',
    `tg_user_id`        BIGINT          DEFAULT NULL COMMENT 'Telegram用户ID',
    `nickname`          VARCHAR(128)    DEFAULT NULL COMMENT '昵称',
    `proxy_info`        VARCHAR(500)    DEFAULT NULL COMMENT '代理信息',
    `node_id`           VARCHAR(32)     DEFAULT NULL COMMENT '节点ID',
    `login_time`        DATETIME        NOT NULL    COMMENT '登录时间',
    `create_time`       DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX `idx_phone` (`phone`),
    INDEX `idx_login_time` (`login_time`),
    INDEX `idx_result` (`result`),
    INDEX `idx_node_id` (`node_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='登录日志表';

-- ============================================================
-- 10. 导入账号明细表 (增加 node_id)
-- ============================================================
CREATE TABLE IF NOT EXISTS `tg_import_account` (
    `id`                INT             AUTO_INCREMENT PRIMARY KEY,
    `batch_no`          VARCHAR(64)     DEFAULT NULL COMMENT '批次号',
    `phone`             VARCHAR(32)     DEFAULT NULL COMMENT '手机号',
    `status`            VARCHAR(20)     DEFAULT 'waiting' COMMENT '状态: waiting/online/failed/banned',
    `tg_user_id`        BIGINT          DEFAULT NULL COMMENT 'TG用户ID',
    `node_id`           VARCHAR(32)     DEFAULT NULL COMMENT '分配的节点ID',
    `create_time`       DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time`       DATETIME        DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX `idx_batch_no` (`batch_no`),
    INDEX `idx_node_id` (`node_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='导入账号明细表';

-- ============================================================
-- 11. 广告问候语表 (不变)
-- ============================================================
CREATE TABLE IF NOT EXISTS `tg_greeting` (
    `id`            INT             AUTO_INCREMENT PRIMARY KEY,
    `title`         VARCHAR(200)    DEFAULT NULL COMMENT '标题',
    `content`       TEXT            DEFAULT NULL COMMENT '问候语内容',
    `image_path`    VARCHAR(500)    DEFAULT NULL COMMENT '图片路径(可选)',
    `is_enabled`    TINYINT(1)      DEFAULT 1   COMMENT '是否启用',
    `sort_order`    INT             DEFAULT 0   COMMENT '排序',
    `remark`        VARCHAR(500)    DEFAULT NULL COMMENT '备注',
    `create_time`   DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time`   DATETIME        DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='广告问候语表';

-- ============================================================
-- 12. 主动开场白表 (不变)
-- ============================================================
CREATE TABLE IF NOT EXISTS `tg_opening` (
    `id`            INT             AUTO_INCREMENT PRIMARY KEY,
    `content`       TEXT            DEFAULT NULL COMMENT '开场白内容(纯文本)',
    `is_enabled`    TINYINT(1)      DEFAULT 1   COMMENT '是否启用',
    `sort_order`    INT             DEFAULT 0   COMMENT '排序',
    `remark`        VARCHAR(500)    DEFAULT NULL COMMENT '备注',
    `create_time`   DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time`   DATETIME        DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='主动开场白表';

-- ============================================================
-- 13. 代理IP表 (不变)
-- ============================================================
CREATE TABLE IF NOT EXISTS `tg_proxy_ip` (
    `id`                    INT             AUTO_INCREMENT PRIMARY KEY,
    `group_no`              VARCHAR(64)     DEFAULT NULL COMMENT '组号',
    `protocol`              VARCHAR(20)     DEFAULT NULL COMMENT '协议: socks5/socks4/http',
    `host`                  VARCHAR(256)    DEFAULT NULL COMMENT '代理地址',
    `port`                  INT             DEFAULT NULL COMMENT '代理端口',
    `username`              VARCHAR(256)    DEFAULT NULL COMMENT '认证用户名',
    `password`              VARCHAR(256)    DEFAULT NULL COMMENT '认证密码',
    `proxy_url`             VARCHAR(512)    DEFAULT NULL COMMENT '完整代理URL',
    `max_bindable`          INT             DEFAULT 1   COMMENT '最大可绑定账号数',
    `current_bind_count`    INT             DEFAULT 0   COMMENT '当前绑定数',
    `status`                VARCHAR(20)     DEFAULT 'active' COMMENT '状态',
    `create_time`           DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time`           DATETIME        DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='代理IP表';

-- ============================================================
-- 14. 代理IP组表
-- ============================================================
CREATE TABLE IF NOT EXISTS `tg_proxy_group` (
    `id`            INT             AUTO_INCREMENT PRIMARY KEY,
    `group_no`      VARCHAR(64)     NOT NULL COMMENT '组号',
    `group_name`    VARCHAR(200)    DEFAULT NULL COMMENT '组名',
    `remark`        VARCHAR(500)    DEFAULT NULL COMMENT '备注',
    `create_time`   DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time`   DATETIME        DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY `uk_group_no` (`group_no`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='代理IP组表';

-- ============================================================
-- 15. 账号导入批次表 (不变)
-- ============================================================
CREATE TABLE IF NOT EXISTS `tg_import_batch` (
    `id`                INT             AUTO_INCREMENT PRIMARY KEY,
    `batch_no`          VARCHAR(64)     NOT NULL COMMENT '批次号',
    `title`             VARCHAR(200)    DEFAULT NULL COMMENT '批次标题',
    `total_count`       INT             DEFAULT 0   COMMENT '总数',
    `success_count`     INT             DEFAULT 0   COMMENT '成功数',
    `failed_count`      INT             DEFAULT 0   COMMENT '失败数',
    `waiting_count`     INT             DEFAULT 0   COMMENT '等待数',
    `create_time`       DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time`       DATETIME        DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY `uk_batch_no` (`batch_no`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='账号导入批次表';

-- ============================================================
-- 16. 联系人导入批次表 (不变)
-- ============================================================
CREATE TABLE IF NOT EXISTS `tg_contact_import_batch` (
    `id`            INT             AUTO_INCREMENT PRIMARY KEY,
    `batch_no`      VARCHAR(64)     NOT NULL COMMENT '批次号',
    `import_type`   VARCHAR(20)     DEFAULT 'phone' COMMENT '导入类型: phone/username',
    `total_count`   INT             DEFAULT 0   COMMENT '总数',
    `used_count`    INT             DEFAULT 0   COMMENT '已用数',
    `create_time`   DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time`   DATETIME        DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY `uk_batch_no` (`batch_no`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='联系人导入批次表';

-- ============================================================
-- 17. 联系人导入记录表 (不变)
-- ============================================================
CREATE TABLE IF NOT EXISTS `tg_contact_import_record` (
    `id`            INT             AUTO_INCREMENT PRIMARY KEY,
    `batch_no`      VARCHAR(64)     DEFAULT NULL COMMENT '批次号',
    `phone`         VARCHAR(32)     DEFAULT NULL COMMENT '联系人手机号',
    `username`      VARCHAR(200)    DEFAULT NULL COMMENT '联系人用户名',
    `is_used`       TINYINT(1)      DEFAULT 0   COMMENT '是否已使用',
    `create_time`   DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX `idx_batch_no` (`batch_no`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='联系人导入记录表';

-- ============================================================
-- 18. 账号配置表 (兼容旧系统，不变)
-- ============================================================
CREATE TABLE IF NOT EXISTS `tg_account_config` (
    `id`                INT             AUTO_INCREMENT PRIMARY KEY,
    `phone`             VARCHAR(32)     DEFAULT NULL COMMENT '手机号',
    `api_id`            INT             DEFAULT NULL COMMENT 'API ID',
    `api_hash`          VARCHAR(64)     DEFAULT NULL COMMENT 'API Hash',
    `device_model`      VARCHAR(200)    DEFAULT NULL COMMENT '设备型号',
    `system_version`    VARCHAR(100)    DEFAULT NULL COMMENT '系统版本',
    `app_version`       VARCHAR(100)    DEFAULT NULL COMMENT 'APP版本',
    `lang_code`         VARCHAR(20)     DEFAULT NULL COMMENT '语言代码',
    `system_lang_code`  VARCHAR(20)     DEFAULT NULL COMMENT '系统语言代码',
    `create_time`       DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time`       DATETIME        DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY `uk_phone` (`phone`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='账号配置表(兼容旧系统)';

-- ============================================================
-- 创建远程访问用户 (允许其他节点连接)
-- ============================================================
-- CREATE USER IF NOT EXISTS 'tg_gc'@'%' IDENTIFIED BY 'TgGc@2026!Secure';
-- GRANT ALL PRIVILEGES ON `tg_gc`.* TO 'tg_gc'@'%';
-- FLUSH PRIVILEGES;
