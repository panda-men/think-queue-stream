<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Tests\Fixtures;

use PDO;
use ThinkQueue\Streams\FailedJobStore;

/** 每个测试独立 SQLite 文件，也可由 CI 指定 MySQL 连接验证真实行锁。 */
final class FailureDatabase
{
    public static function configuration(string $prefix): array
    {
        $connection = getenv('QUEUE_TEST_DB_HOST') ? [
            'type' => 'mysql', 'hostname' => getenv('QUEUE_TEST_DB_HOST'),
            'hostport' => getenv('QUEUE_TEST_DB_PORT') ?: '3306',
            'database' => getenv('QUEUE_TEST_DB_NAME') ?: 'queue_test',
            'username' => getenv('QUEUE_TEST_DB_USER') ?: 'root',
            'password' => getenv('QUEUE_TEST_DB_PASSWORD') ?: '', 'charset' => 'utf8mb4',
        ] : ['type' => 'sqlite', 'database' => self::path($prefix)];
        return ['default' => 'queue_test', 'connections' => ['queue_test' => $connection]];
    }

    public static function pdo(string $prefix): PDO
    {
        $config = self::configuration($prefix)['connections']['queue_test'];
        $pdo = $config['type'] === 'mysql' ? new PDO('mysql:host=' . $config['hostname'] . ';port=' . $config['hostport'] .
            ';dbname=' . $config['database'] . ';charset=utf8mb4', $config['username'], $config['password']) :
            new PDO('sqlite:' . $config['database']);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        if ($config['type'] === 'sqlite') $pdo->exec('PRAGMA busy_timeout = 5000');
        return $pdo;
    }

    public static function store(string $prefix): FailedJobStore
    {
        $store = new FailedJobStore(self::pdo($prefix));
        $store->install();
        return $store;
    }

    public static function cleanup(string $prefix): void
    {
        if (getenv('QUEUE_TEST_DB_HOST')) {
            $statement = self::pdo($prefix)->prepare('DELETE FROM failed_jobs WHERE prefix LIKE ?');
            $statement->execute([$prefix . '%']);
        } else {
            foreach (glob(self::path($prefix) . '*') as $path) unlink($path);
        }
    }

    private static function path(string $prefix): string
    {
        return sys_get_temp_dir() . '/' . $prefix . '-failed.sqlite';
    }
}
