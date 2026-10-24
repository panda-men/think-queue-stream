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
    'promotion_interval_ms' => 1000,
    'heartbeat_interval_ms' => 10000,
    'restart_interval_ms' => 1000,
    // 0 表示不限制活跃任务数量；单条限制包含完整 JSON 信封。
    'max_active_jobs' => 0,
    'max_payload_bytes' => 1048576,
    'retry_jitter' => true,
];
