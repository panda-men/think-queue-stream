<?php
declare(strict_types=1);

namespace ThinkQueue\Streams;

use Redis;
use RuntimeException;

final class RedisConnection
{
    /**
     * 创建专用连接，禁用序列化和客户端前缀，避免污染 Lua 与 Stream 协议。
     *
     * @param array<string, mixed> $config 连接配置
     * @return Redis 已连接的客户端
     */
    public static function connect(array $config): Redis
    {
        $redis = new Redis();
        $redis->connect((string) ($config['host'] ?? '127.0.0.1'), (int) ($config['port'] ?? 6379),
            (float) ($config['connect_timeout'] ?? 3.0));
        if (!empty($config['password'])) {
            $redis->auth($config['password']);
        }
        $redis->select((int) ($config['database'] ?? 0));
        $redis->setOption(Redis::OPT_SERIALIZER, Redis::SERIALIZER_NONE);
        $redis->setOption(Redis::OPT_MAX_RETRIES, 0);
        $redis->setOption(Redis::OPT_READ_TIMEOUT, (float) ($config['read_timeout'] ?? 5.0));
        $info = $redis->info('server');
        if (version_compare($info['redis_version'] ?? '0', '6.2', '<')) {
            throw new RuntimeException('Redis Streams 队列要求 Redis >= 6.2。');
        }
        return $redis;
    }
}
