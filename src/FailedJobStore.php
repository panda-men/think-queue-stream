<?php
declare(strict_types=1);

namespace ThinkQueue\Streams;

use Closure;
use InvalidArgumentException;
use PDO;
use RuntimeException;
use Throwable;

/** 最终失败记录的数据库存储；必须使用独立连接，避免受业务事务回滚影响。 */
final class FailedJobStore
{
    private ?PDO $pdo = null;
    private string $quotedTable;

    /**
     * @param PDO|Closure $connection 专用 PDO，或延迟创建专用 PDO 的工厂
     * @param string $table 完整表名，不自动附加应用表前缀
     */
    public function __construct(private PDO|Closure $connection, private string $table = 'failed_jobs')
    {
        if (!preg_match('/\A[a-zA-Z_][a-zA-Z0-9_]{0,63}\z/D', $table)) {
            throw new InvalidArgumentException('失败表名必须为 1 至 64 位字母、数字或下划线，且不能以数字开头。');
        }
        $this->quotedTable = '`' . $table . '`';
    }

    /** 显式初始化表，消费和查询不会自动执行 DDL。 */
    public function install(): void
    {
        $driver = $this->database()->getAttribute(PDO::ATTR_DRIVER_NAME);
        $sql = file_get_contents(__DIR__ . '/../sql/' . $driver . '/failed_jobs.sql');
        $this->database()->exec(str_replace('{{table}}', $this->table, $sql));
    }

    /**
     * 幂等归档；在行锁内重新确认待归档记录，防止过期的迁移进程复活已删除的历史。
     * 数据库提交后，调用方才能删除 Redis 待落库记录。
     * @param array<string,string> $record 原始失败记录
     */
    public function archive(string $prefix, string $queue, string $id, array $record, Closure $pending): void
    {
        $sort = self::sortId($id);
        $scope = $this->scope($prefix, $queue);
        $this->transaction(function () use ($prefix, $queue, $id, $record, $pending, $sort, $scope): bool {
            $mysql = $this->database()->getAttribute(PDO::ATTR_DRIVER_NAME) === 'mysql';
            // 唯一键上的幂等写入先获取行锁，避免并发归档对不存在的行先读后插造成锁升级死锁。
            $this->execute(($mysql ? 'INSERT INTO ' : 'INSERT OR IGNORE INTO ') . $this->quotedTable .
                ' (scope, failure_id, sort_id, prefix, queue, job_id, payload, attempts, error, failed_at_ms)' .
                ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)' . ($mysql ? ' ON DUPLICATE KEY UPDATE failure_id = failure_id' : ''),
                [$scope, $id, $sort, $prefix, $queue, $record['job_id'] ?? '', $record['payload'] ?? '',
                    (int) ($record['attempts'] ?? 0), $record['error'] ?? '',
                    (int) ($record['failed_at_ms'] ?? explode('-', $id)[0])]);
            // Redis 暂存已被重放/删除时，回滚本次可能新插入的记录，禁止复活旧历史。
            return $pending();
        }, true);
    }

    /** @return array<string,array<string,string>> 按失败记录 ID 分页 */
    public function records(string $prefix, string $queue, int $limit, string $start): array
    {
        $where = 'scope = ?';
        $values = [$this->scope($prefix, $queue)];
        if ($start !== '-') {
            $exclusive = str_starts_with($start, '(');
            $where .= ' AND sort_id ' . ($exclusive ? '>' : '>=') . ' ?';
            $values[] = self::sortId($exclusive ? substr($start, 1) : $start);
        }
        $rows = $this->execute('SELECT * FROM ' . $this->quotedTable . ' WHERE ' . $where .
            ' ORDER BY sort_id LIMIT ' . $limit, $values)->fetchAll(PDO::FETCH_ASSOC);
        $result = [];
        foreach ($rows as $row) $result[$row['failure_id']] = $this->record($row);
        return $result;
    }

    /** 在行锁内执行重放或删除；回调失败则保留数据库记录。 */
    public function remove(string $prefix, string $queue, string $id, Closure $action): mixed
    {
        self::sortId($id);
        $scope = $this->scope($prefix, $queue);
        return $this->transaction(function () use ($scope, $id, $action): mixed {
            $row = $this->find($scope, $id, true);
            if ($row === null) return null;
            $result = $action($this->record($row));
            $this->execute('DELETE FROM ' . $this->quotedTable . ' WHERE scope = ? AND failure_id = ?', [$scope, $id]);
            return $result;
        });
    }

    public function exists(string $prefix, string $queue, string $id): bool
    {
        return $this->find($this->scope($prefix, $queue), $id) !== null;
    }

    public function count(string $prefix, string $queue): int
    {
        return (int) $this->execute('SELECT COUNT(*) FROM ' . $this->quotedTable . ' WHERE scope = ?',
            [$this->scope($prefix, $queue)])->fetchColumn();
    }

    /** @return list<string> 按失败时间选择有界清理候选 */
    public function older(string $prefix, string $queue, int $beforeMs, int $limit): array
    {
        return $this->execute('SELECT failure_id FROM ' . $this->quotedTable .
            ' WHERE scope = ? AND failed_at_ms < ? ORDER BY failed_at_ms, sort_id LIMIT ' . $limit,
            [$this->scope($prefix, $queue), $beforeMs])->fetchAll(PDO::FETCH_COLUMN);
    }

    /** Stream ID 补齐后按字符串排序，避免 10-0 排到 2-0 前面。 */
    private static function sortId(string $id): string
    {
        if (!preg_match('/\A(\d{1,20})-(\d{1,20})\z/D', $id, $parts)) {
            throw new InvalidArgumentException('失败记录 ID 必须为有效的 Stream ID。');
        }
        return str_pad($parts[1], 20, '0', STR_PAD_LEFT) . '-' . str_pad($parts[2], 20, '0', STR_PAD_LEFT);
    }

    private function scope(string $prefix, string $queue): string
    {
        return hash('sha256', $prefix . "\0" . $queue);
    }

    private function find(string $scope, string $id, bool $lock = false): ?array
    {
        $suffix = $lock && $this->database()->getAttribute(PDO::ATTR_DRIVER_NAME) === 'mysql' ? ' FOR UPDATE' : '';
        $row = $this->execute('SELECT * FROM ' . $this->quotedTable . ' WHERE scope = ? AND failure_id = ?' . $suffix,
            [$scope, $id])->fetch(PDO::FETCH_ASSOC);
        return $row === false ? null : $row;
    }

    private function record(array $row): array
    {
        $result = [];
        foreach (['job_id', 'payload', 'attempts', 'error', 'failed_at_ms'] as $key) $result[$key] = (string) $row[$key];
        return $result;
    }

    private function execute(string $sql, array $values): \PDOStatement
    {
        $statement = $this->database()->prepare($sql);
        $statement->execute($values);
        return $statement;
    }

    private function transaction(Closure $action, bool $rollbackOnFalse = false): mixed
    {
        $pdo = $this->database();
        if ($pdo->inTransaction()) throw new RuntimeException('失败存储必须使用独立且不含外部事务的数据库连接。');
        $sqlite = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME) === 'sqlite';
        // SQLite 没有行锁，提前获取写锁，避免两个归档进程先读后写相互争用。
        if ($sqlite) $pdo->exec('BEGIN IMMEDIATE');
        else $pdo->beginTransaction();
        try {
            $result = $action();
            if ($rollbackOnFalse && $result === false) {
                if ($sqlite) $pdo->exec('ROLLBACK');
                else $pdo->rollBack();
                return false;
            }
            if ($sqlite) $pdo->exec('COMMIT');
            else $pdo->commit();
            return $result;
        } catch (Throwable $exception) {
            if ($sqlite) $pdo->exec('ROLLBACK');
            elseif ($pdo->inTransaction()) $pdo->rollBack();
            throw $exception;
        }
    }

    private function database(): PDO
    {
        if ($this->pdo === null) {
            $pdo = $this->connection instanceof Closure ? ($this->connection)() : $this->connection;
            if (!in_array($pdo->getAttribute(PDO::ATTR_DRIVER_NAME), ['mysql', 'sqlite'], true)) {
                throw new RuntimeException('失败记录存储目前支持 MySQL/MariaDB 和 SQLite。');
            }
            $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            if ($pdo->getAttribute(PDO::ATTR_DRIVER_NAME) === 'sqlite') $pdo->exec('PRAGMA busy_timeout = 5000');
            $this->pdo = $pdo;
        }
        return $this->pdo;
    }
}
