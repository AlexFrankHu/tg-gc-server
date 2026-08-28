-- 账号新增独立标记: 是否被 TG 冻结(frozen)
-- 冻结账号一定同时被限制, 因此回填时同步置 is_restricted = 1
ALTER TABLE tg_telethon_account
    ADD COLUMN `is_frozen` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否被TG冻结(frozen), 冻结账号一定同时被限制' AFTER `is_restricted`;

UPDATE tg_telethon_account SET is_restricted = 1 WHERE is_frozen = 1 AND (is_restricted = 0 OR is_restricted IS NULL);
