-- 强制使用 utf8mb4 导入, 否则客户端默认字符集会把中文二次编码成乱码
SET NAMES utf8mb4;

-- ============================================================
-- 伪好友(fake): 只用 contacts.resolvePhone 解析用户信息后直接入库,
-- 不调用 importContacts/addContact, 即不加入 TG 联系人。
--   tg_contact.contact_type  real=好友(默认) / fake=伪好友
--   tg_contact.access_hash   发消息需要 InputPeerUser(user_id, access_hash);
--                            伪好友不在 TG 联系人里, 只靠本地 session 缓存不可靠, 必须落库
--   tg_contact_assign_log.contact_type  本次分配要加成好友还是伪好友
-- 历史数据: ADD COLUMN ... DEFAULT 'real' 会把已有行填成 real
-- 幂等: 已存在的列/索引会跳过
-- ============================================================
USE `tg_gc`;

DROP PROCEDURE IF EXISTS `tg_add_column_if_absent`;
DELIMITER $$
CREATE PROCEDURE `tg_add_column_if_absent`(
    IN p_table  VARCHAR(64),
    IN p_column VARCHAR(64),
    IN p_ddl    VARCHAR(512)
)
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = p_table
          AND COLUMN_NAME = p_column
    ) THEN
        SET @sql = CONCAT('ALTER TABLE `', p_table, '` ADD COLUMN ', p_ddl);
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END$$
DELIMITER ;

CALL `tg_add_column_if_absent`('tg_contact', 'contact_type',
    "`contact_type` VARCHAR(10) NOT NULL DEFAULT 'real' COMMENT '好友类型: real=好友, fake=伪好友(仅解析未加联系人)' AFTER `source`");

CALL `tg_add_column_if_absent`('tg_contact', 'access_hash',
    "`access_hash` BIGINT DEFAULT NULL COMMENT 'TG access_hash(发消息构造 InputPeerUser 用)' AFTER `user_id`");

CALL `tg_add_column_if_absent`('tg_contact_assign_log', 'contact_type',
    "`contact_type` VARCHAR(10) NOT NULL DEFAULT 'real' COMMENT '好友类型: real=好友, fake=伪好友' AFTER `add_method`");

DROP PROCEDURE `tg_add_column_if_absent`;

-- 历史数据兜底(列已存在但值为空时)
UPDATE `tg_contact`            SET `contact_type` = 'real' WHERE `contact_type` IS NULL OR `contact_type` = '';
UPDATE `tg_contact_assign_log` SET `contact_type` = 'real' WHERE `contact_type` IS NULL OR `contact_type` = '';
