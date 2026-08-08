-- 强制使用 utf8mb4 导入, 否则客户端默认字符集会把中文二次编码成乱码
SET NAMES utf8mb4;

-- ============================================================
-- 表结构对齐脚本 (init.sql 落后于代码, 这些列/表原先是在生产库手工 ALTER 加的)
-- 用途: 全新部署时在 init.sql 之后执行, 使表结构与后台 Java Mapper / 节点 Python 代码一致
-- 幂等: 重复执行会因列已存在报错, 用 mysql --force 或先确认
-- ============================================================
USE `tg_gc`;

-- ---------- 1. 节点信息表: 端口/状态/类型 ----------
ALTER TABLE `tg_cluster_node`
    ADD COLUMN `node_port`   INT          DEFAULT NULL COMMENT '节点服务端口'                     AFTER `node_dir`,
    ADD COLUMN `node_status` VARCHAR(20)  DEFAULT NULL COMMENT '节点状态: online/offline'          AFTER `node_port`,
    ADD COLUMN `node_type`   VARCHAR(20)  DEFAULT NULL COMMENT '节点类型'                        AFTER `max_account_count`;

-- ---------- 2. 联系人表: 认证/限制/头像/注销标记 ----------
ALTER TABLE `tg_contact`
    ADD COLUMN `is_verified`          TINYINT(1)   DEFAULT 0    COMMENT '是否官方认证'            AFTER `is_premium`,
    ADD COLUMN `restriction_reason`   VARCHAR(255) DEFAULT NULL COMMENT '受限原因'                AFTER `user_type`,
    ADD COLUMN `bio`                  VARCHAR(512) DEFAULT NULL COMMENT '个人简介'                AFTER `restriction_reason`,
    ADD COLUMN `photo_small_file_id`  INT          DEFAULT NULL COMMENT '小头像 file_id'          AFTER `bio`,
    ADD COLUMN `photo_big_file_id`    INT          DEFAULT NULL COMMENT '大头像 file_id'          AFTER `photo_small_file_id`,
    ADD COLUMN `is_deregistered`      TINYINT(1)   DEFAULT 0    COMMENT '对方账号是否已注销'      AFTER `auto_reply`;

-- ---------- 3. 好友分配日志: 账号批次/联系人批次/分配方式/备注 ----------
ALTER TABLE `tg_contact_assign_log`
    ADD COLUMN `account_id`           INT          DEFAULT NULL COMMENT '账号ID(=tg_telethon_account.id)' AFTER `tg_account_id`,
    ADD COLUMN `account_batch_no`     VARCHAR(64)  DEFAULT NULL COMMENT '账号导入批次号'          AFTER `account_phone`,
    ADD COLUMN `account_batch_title`  VARCHAR(255) DEFAULT NULL COMMENT '账号批次标题'            AFTER `account_batch_no`,
    ADD COLUMN `contact_batch_no`     VARCHAR(64)  DEFAULT NULL COMMENT '联系人导入批次号'        AFTER `contact_username`,
    ADD COLUMN `contact_batch_title`  VARCHAR(255) DEFAULT NULL COMMENT '联系人批次标题'          AFTER `contact_batch_no`,
    ADD COLUMN `add_method`           VARCHAR(20)  DEFAULT NULL COMMENT '添加方式: phone/username' AFTER `import_type`,
    ADD COLUMN `remark`               VARCHAR(512) DEFAULT NULL COMMENT '备注'                    AFTER `error_reason`;

-- ---------- 4. 联系人导入批次: 标题/文件名/待用数/无效数/导入时间 ----------
ALTER TABLE `tg_contact_import_batch`
    ADD COLUMN `title`         VARCHAR(255) DEFAULT NULL COMMENT '批次标题'      AFTER `batch_no`,
    ADD COLUMN `file_name`     VARCHAR(255) DEFAULT NULL COMMENT '上传文件名'    AFTER `import_type`,
    ADD COLUMN `waiting_count` INT          DEFAULT 0    COMMENT '待分配数量'    AFTER `used_count`,
    ADD COLUMN `invalid_count` INT          DEFAULT 0    COMMENT '无效/被过滤数' AFTER `waiting_count`,
    ADD COLUMN `import_time`   DATETIME     DEFAULT NULL COMMENT '导入时间'      AFTER `invalid_count`;

-- ---------- 5. 问候语: 状态 ----------
ALTER TABLE `tg_greeting`
    ADD COLUMN `state` INT DEFAULT 0 COMMENT '状态' AFTER `content`;

-- ---------- 6. 导入账号明细: 失败原因/昵称/用户名/登录时间 ----------
ALTER TABLE `tg_import_account`
    ADD COLUMN `reason`     VARCHAR(512) DEFAULT NULL COMMENT '失败原因'   AFTER `status`,
    ADD COLUMN `nickname`   VARCHAR(128) DEFAULT NULL COMMENT '昵称'       AFTER `tg_user_id`,
    ADD COLUMN `username`   VARCHAR(128) DEFAULT NULL COMMENT 'TG用户名'   AFTER `nickname`,
    ADD COLUMN `login_time` DATETIME     DEFAULT NULL COMMENT '登录时间'   AFTER `username`;

-- ---------- 7. 账号导入批次: 文件名/导入时间 ----------
ALTER TABLE `tg_import_batch`
    ADD COLUMN `file_name`   VARCHAR(255) DEFAULT NULL COMMENT '上传文件名' AFTER `title`,
    ADD COLUMN `import_time` DATETIME     DEFAULT NULL COMMENT '导入时间'   AFTER `waiting_count`;

-- ---------- 8. 代理分配日志: 账号批次/代理组 ----------
ALTER TABLE `tg_proxy_assign_log`
    ADD COLUMN `account_id`          INT          DEFAULT NULL COMMENT '账号ID'        AFTER `tg_account_id`,
    ADD COLUMN `account_batch_no`    VARCHAR(64)  DEFAULT NULL COMMENT '账号批次号'    AFTER `account_phone`,
    ADD COLUMN `account_batch_title` VARCHAR(255) DEFAULT NULL COMMENT '账号批次标题'  AFTER `account_batch_no`,
    ADD COLUMN `proxy_group_no`      VARCHAR(64)  DEFAULT NULL COMMENT '代理组编号'    AFTER `proxy_ip_id`,
    ADD COLUMN `proxy_group_title`   VARCHAR(255) DEFAULT NULL COMMENT '代理组标题'    AFTER `proxy_group_no`;

-- ---------- 9. 代理组: 标题/国家/到期/可绑上限/总数/导入时间 ----------
ALTER TABLE `tg_proxy_group`
    ADD COLUMN `title`        VARCHAR(255) DEFAULT NULL COMMENT '代理组标题'      AFTER `group_no`,
    ADD COLUMN `country`      VARCHAR(64)  DEFAULT NULL COMMENT '国家'            AFTER `title`,
    ADD COLUMN `expire_time`  DATETIME     DEFAULT NULL COMMENT '到期时间'        AFTER `country`,
    ADD COLUMN `max_bindable` INT          DEFAULT NULL COMMENT '单IP可绑账号数'  AFTER `expire_time`,
    ADD COLUMN `total_count`  INT          DEFAULT 0    COMMENT 'IP总数'          AFTER `max_bindable`,
    ADD COLUMN `import_time`  DATETIME     DEFAULT NULL COMMENT '导入时间'        AFTER `total_count`;

-- ---------- 10. 代理IP: 历史绑定次数 ----------
ALTER TABLE `tg_proxy_ip`
    ADD COLUMN `history_bind_count` INT DEFAULT 0 COMMENT '历史累计绑定次数' AFTER `current_bind_count`;

-- ---------- 11. 聊天消息: 发送方/媒体扩展字段 ----------
ALTER TABLE `tg_chat_message`
    ADD COLUMN `sender_chat_id`     BIGINT       DEFAULT NULL COMMENT '发送方群组ID'  AFTER `sender_user_id`,
    ADD COLUMN `sender_name`        VARCHAR(255) DEFAULT NULL COMMENT '发送方名称'    AFTER `sender_chat_id`,
    ADD COLUMN `media_file_name`    VARCHAR(255) DEFAULT NULL COMMENT '媒体文件名'    AFTER `media_mime_type`,
    ADD COLUMN `media_duration`     INT          DEFAULT NULL COMMENT '媒体时长(秒)'  AFTER `media_file_name`,
    ADD COLUMN `media_width`        INT          DEFAULT NULL COMMENT '媒体宽'        AFTER `media_duration`,
    ADD COLUMN `media_height`       INT          DEFAULT NULL COMMENT '媒体高'        AFTER `media_width`,
    ADD COLUMN `thumbnail_file_id`  INT          DEFAULT NULL COMMENT '缩略图file_id' AFTER `media_height`;

-- ---------- 12. 账号配置表(遗留页面): 补齐 Mapper 需要的列 ----------
ALTER TABLE `tg_account_config`
    ADD COLUMN `tg_user_id`       BIGINT       DEFAULT NULL COMMENT 'TG用户ID'          AFTER `id`,
    ADD COLUMN `nickname`         VARCHAR(128) DEFAULT NULL COMMENT '昵称'              AFTER `tg_user_id`,
    ADD COLUMN `username`         VARCHAR(128) DEFAULT NULL COMMENT '用户名'            AFTER `nickname`,
    ADD COLUMN `custom_username`  VARCHAR(128) DEFAULT NULL COMMENT '自定义用户名'      AFTER `username`,
    ADD COLUMN `notice_flag`      INT          DEFAULT NULL COMMENT '通知标记'          AFTER `custom_username`,
    ADD COLUMN `phone_num`        VARCHAR(32)  DEFAULT NULL COMMENT '手机号'            AFTER `notice_flag`,
    ADD COLUMN `login_status`     INT          DEFAULT NULL COMMENT '登录状态'          AFTER `phone_num`,
    ADD COLUMN `two_fa_password`  VARCHAR(128) DEFAULT NULL COMMENT '二次验证密码'      AFTER `system_lang_code`,
    ADD COLUMN `last_online_time` DATETIME     DEFAULT NULL COMMENT '最后在线时间'      AFTER `two_fa_password`;

-- ---------- 13. 系统配置表 (自动回复总开关等) ----------
CREATE TABLE IF NOT EXISTS `tg_system_config` (
    `id`           INT          NOT NULL AUTO_INCREMENT COMMENT '主键',
    `config_key`   VARCHAR(64)  NOT NULL                COMMENT '配置键',
    `config_name`  VARCHAR(128) DEFAULT NULL            COMMENT '配置名称',
    `config_value` VARCHAR(512) DEFAULT NULL            COMMENT '配置值',
    `create_time`  DATETIME     DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time`  DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_config_key` (`config_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='TG系统配置表';

INSERT INTO `tg_system_config` (`config_key`, `config_name`, `config_value`)
VALUES ('auto_reply_enabled', '自动回复总开关', '1')
ON DUPLICATE KEY UPDATE `config_name` = VALUES(`config_name`);

-- ---------- 14. 联系人导入被过滤记录表 ----------
CREATE TABLE IF NOT EXISTS `tg_contact_import_filtered` (
    `id`          BIGINT       NOT NULL AUTO_INCREMENT COMMENT '主键',
    `batch_no`    VARCHAR(64)  DEFAULT NULL            COMMENT '导入批次号',
    `phone`       VARCHAR(50)  DEFAULT NULL            COMMENT '被过滤的手机号',
    `filter_type` VARCHAR(32)  DEFAULT NULL            COMMENT '过滤类型: duplicate/self_account/invalid',
    `create_time` DATETIME     DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (`id`),
    KEY `idx_batch_no` (`batch_no`),
    KEY `idx_filter_type` (`filter_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='联系人导入被过滤记录表';

-- ---------- 15. 新增列对应的索引 ----------
ALTER TABLE `tg_contact_assign_log` ADD INDEX `idx_account_batch_no` (`account_batch_no`);
ALTER TABLE `tg_contact_assign_log` ADD INDEX `idx_contact_batch_no` (`contact_batch_no`);
ALTER TABLE `tg_contact_assign_log` ADD INDEX `idx_group_id` (`group_id`);
ALTER TABLE `tg_proxy_assign_log`   ADD INDEX `idx_account_batch_no` (`account_batch_no`);
ALTER TABLE `tg_proxy_assign_log`   ADD INDEX `idx_proxy_group_no` (`proxy_group_no`);
ALTER TABLE `tg_proxy_assign_log`   ADD INDEX `idx_account_phone` (`account_phone`);
