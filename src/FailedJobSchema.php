<?php
declare(strict_types=1);

namespace ThinkQueue\Streams;

use PDO;
use RuntimeException;
use Throwable;

/** 显式失败表升级；DDL 和数据规范化均可中断后重复执行。 */
final class FailedJobSchema
{
    private string $quoted;

    /** @param PDO $pdo 独立数据库连接 @param string $table 已由 FailedJobStore 验证的表名 */
    public function __construct(private PDO $pdo, private string $table)
    {
        $this->quoted = '`' . $table . '`';
    }

    /** 将 v0.4 失败表升级为统一失败归档，保留项目自定义 id 和时间字段。 */
    public function upgrade(): void
    {
        if ($this->pdo->inTransaction()) throw new RuntimeException('请在业务事务之外执行失败表升级。');
        $mysql = $this->pdo->getAttribute(PDO::ATTR_DRIVER_NAME) === 'mysql';
        $rows = $this->pdo->query($mysql ? 'SHOW COLUMNS FROM ' . $this->quoted :
            'PRAGMA table_info(' . $this->quoted . ')')->fetchAll(PDO::FETCH_ASSOC);
        $columns = array_column($rows, null, $mysql ? 'Field' : 'name');
        $definitions = ['failure_kind' => "VARCHAR(16) NOT NULL DEFAULT 'failed'",
            'source' => "VARCHAR(16) NOT NULL DEFAULT ''", 'message_id' => "VARCHAR(41) NOT NULL DEFAULT ''",
            'raw_entry' => 'LONGTEXT NULL', 'payload_base64' => 'LONGTEXT NULL', 'error_base64' => 'LONGTEXT NULL'];
        foreach ($definitions as $name => $definition) {
            if (!isset($columns[$name])) {
                $this->pdo->exec('ALTER TABLE ' . $this->quoted . ' ADD COLUMN `' . $name . '` ' .
                    ($mysql ? $definition : str_replace('LONGTEXT', 'TEXT', $definition)));
            }
        }
        $this->normalizeText();
        if ($mysql) {
            if (strtolower($columns['error']['Type']) !== 'longtext' ||
                strtolower($columns['failure_id']['Type']) !== 'varchar(43)' ||
                strtolower($columns['sort_id']['Type']) !== 'char(42)') {
                $this->pdo->exec('ALTER TABLE ' . $this->quoted .
                    ' MODIFY error LONGTEXT CHARACTER SET utf8mb4 NOT NULL,' .
                    ' MODIFY failure_id VARCHAR(43) NOT NULL, MODIFY sort_id CHAR(42) NOT NULL');
            }
        } elseif (strtoupper($columns['error']['type']) !== 'TEXT') {
            $this->rebuildSqliteText();
        }
    }

    /** 每批读取最多 100 行；非法字节在转换 LONGTEXT 前保存副本，合法内容保持原值。 */
    private function normalizeText(): void
    {
        $scope = $id = '';
        do {
            $query = $this->pdo->prepare('SELECT scope, failure_id, payload, error FROM ' . $this->quoted .
                ' WHERE scope > ? OR (scope = ? AND failure_id > ?) ORDER BY scope, failure_id LIMIT 100');
            $query->execute([$scope, $scope, $id]);
            $rows = $query->fetchAll(PDO::FETCH_ASSOC);
            foreach ($rows as $row) {
                $scope = $row['scope'];
                $id = $row['failure_id'];
                foreach (['payload', 'error'] as $field) {
                    if (preg_match('//u', $row[$field]) === 1) continue;
                    $text = json_decode(json_encode($row[$field], JSON_THROW_ON_ERROR | JSON_INVALID_UTF8_SUBSTITUTE),
                        true, 512, JSON_THROW_ON_ERROR);
                    $update = $this->pdo->prepare('UPDATE ' . $this->quoted . ' SET `' . $field . '` = ?, `' .
                        $field . '_base64` = COALESCE(`' . $field . '_base64`, ?) WHERE scope = ? AND failure_id = ?');
                    $update->execute([$text, base64_encode($row[$field]), $scope, $id]);
                }
            }
        } while (count($rows) === 100);
    }

    /** SQLite 不支持 MODIFY，原样保留字段、唯一约束、索引和触发器后重建错误文本列。 */
    private function rebuildSqliteText(): void
    {
        $foreignKeys = (int) $this->pdo->query('PRAGMA foreign_keys')->fetchColumn();
        // 禁止 DROP 触发外部引用的级联删除；重建后原表名、主键和值完全保留。
        if ($foreignKeys === 1) $this->pdo->exec('PRAGMA foreign_keys=OFF');
        $this->pdo->exec('BEGIN IMMEDIATE');
        try {
            $query = $this->pdo->prepare('SELECT type, sql FROM sqlite_master WHERE tbl_name = ? AND sql IS NOT NULL');
            $query->execute([$this->table]);
            $schema = $query->fetchAll(PDO::FETCH_ASSOC);
            $table = array_values(array_filter($schema, static fn (array $row): bool => $row['type'] === 'table'))[0]['sql'];
            $temporary = 'queue_upgrade_' . bin2hex(random_bytes(8));
            $table = preg_replace('/\ACREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:`[^`]+`|"[^"]+"|\[[^]]+\]|\w+)/i',
                'CREATE TABLE `' . $temporary . '`', $table, 1);
            $table = preg_replace('/((?:`error`|"error"|\berror)\s+)BLOB\b/i', '$1TEXT', $table, 1, $replaced);
            if ($replaced !== 1) throw new RuntimeException('无法识别旧 SQLite 失败表的 error 列，请先核对表结构。');
            $sequence = null;
            if (stripos($table, 'AUTOINCREMENT') !== false) {
                $query = $this->pdo->prepare('SELECT seq FROM sqlite_sequence WHERE name = ?');
                $query->execute([$this->table]);
                $sequence = $query->fetchColumn();
                $query->closeCursor();
            }
            $this->pdo->exec($table);
            $this->pdo->exec('INSERT INTO `' . $temporary . '` SELECT * FROM ' . $this->quoted);
            $this->pdo->exec('DROP TABLE ' . $this->quoted);
            $this->pdo->exec('ALTER TABLE `' . $temporary . '` RENAME TO ' . $this->quoted);
            foreach ($schema as $object) {
                if ($object['type'] !== 'table') $this->pdo->exec($object['sql']);
            }
            if ($sequence !== null && $sequence !== false) {
                $query = $this->pdo->prepare('INSERT INTO sqlite_sequence (name,seq) SELECT ?, ?' .
                    ' WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = ?)');
                $query->execute([$this->table, $sequence, $this->table]);
                $query = $this->pdo->prepare('UPDATE sqlite_sequence SET seq = MAX(seq, ?) WHERE name = ?');
                $query->execute([$sequence, $this->table]);
            }
            $this->pdo->exec('COMMIT');
        } catch (Throwable $exception) {
            $this->pdo->exec('ROLLBACK');
            throw $exception;
        } finally {
            if ($foreignKeys === 1) $this->pdo->exec('PRAGMA foreign_keys=ON');
        }
    }
}
