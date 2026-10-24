<?php
declare(strict_types=1);

return [
    'host' => '127.0.0.1',
    'port' => 6379,
    'password' => null,
    'database' => 0,
    'connect_timeout' => 3.0,
    'read_timeout' => 5.0,
    'prefix' => 'think-queue',
    'queue' => 'default',
    // 必须大于 Worker 的 timeout；毫秒。
    'retry_after_ms' => 90000,
    'block_ms' => 1000,
    'claim_interval_ms' => 1000,
    'promotion_batch' => 100,
];
