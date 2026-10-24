<?php
declare(strict_types=1);

namespace ThinkQueue\Streams;

use InvalidArgumentException;
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
        $connectTimeout = $config['connect_timeout'] ?? 3.0;
        $readTimeout = $config['read_timeout'] ?? 5.0;
        foreach (['connect_timeout' => $connectTimeout, 'read_timeout' => $readTimeout] as $name => $value) {
            if (!is_numeric($value) || !is_finite((float) $value) || (float) $value <= 0) {
                throw new InvalidArgumentException("{$name} 必须为有限的正秒数。");
            }
        }
        $redis = new Redis();
        $redis->connect((string) ($config['host'] ?? '127.0.0.1'), (int) ($config['port'] ?? 6379),
            (float) $connectTimeout, null, 0, (float) $readTimeout);
        $redis->setOption(Redis::OPT_MAX_RETRIES, 0);
        $redis->setOption(Redis::OPT_READ_TIMEOUT, (float) $readTimeout);
        if (isset($config['password']) && $config['password'] !== '') {
            $redis->auth($config['password']);
        }
        $redis->select((int) ($config['database'] ?? 0));
        $redis->setOption(Redis::OPT_SERIALIZER, Redis::SERIALIZER_NONE);
        $info = $redis->info('server');
        if (version_compare($info['redis_version'] ?? '0', '6.2', '<')) {
            throw new RuntimeException('Redis Streams 队列要求 Redis >= 6.2。');
        }
        return $redis;
    }
}
