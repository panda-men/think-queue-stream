CREATE TABLE IF NOT EXISTS `{{table}}` (
    `scope` CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    `failure_id` VARCHAR(41) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    `sort_id` CHAR(41) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    `prefix` TEXT NOT NULL,
    `queue` VARCHAR(100) NOT NULL,
    `job_id` VARCHAR(128) NOT NULL,
    `payload` LONGTEXT NOT NULL,
    `attempts` INT UNSIGNED NOT NULL,
    `error` BLOB NOT NULL,
    `failed_at_ms` BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (`scope`, `failure_id`),
    KEY `idx_scope_sort` (`scope`, `sort_id`),
    KEY `idx_scope_age` (`scope`, `failed_at_ms`, `sort_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
