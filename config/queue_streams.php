<?php
declare(strict_types=1);

return [
    // Redis 服务器地址。使用队列专用连接，不复用启用了序列化或客户端键前缀的缓存连接。
    'host' => '127.0.0.1',
    // Redis TCP 端口。
    'port' => 6379,
    // Redis 认证密码；null 表示不认证。部署时从环境变量读取，不把真实密码写入仓库。
    'password' => null,
    // Redis 逻辑数据库编号；生产者、消费者和管理命令必须使用同一编号。
    'database' => 0,

    // 建立 Redis 连接的超时，单位：秒；允许小数，必须是有限正数，不能用 0 表示无限等待。
    'connect_timeout' => 3.0,
    // Redis 命令读取超时，单位：秒；必须是有限正数，且 read_timeout * 1000 > block_ms。
    'read_timeout' => 5.0,
    // Redis 键名前缀，用于区分项目与环境；仅允许字母、数字、冒号、下划线和连字符。
    'prefix' => 'think-queue',
    // 未显式指定队列时使用的默认队列名；不是只允许消费这一个队列。
    'queue' => 'default',

    // 待确认消息的接管等待时间，单位：毫秒；超过此空闲时间后，其他消费者可接管。
    // 必须满足任务 timeout * 1000 < retry_after_ms；不是失败重试的延迟，也不会自动续租。
    'retry_after_ms' => 90000,
    // 空闲时阻塞读取的最长等待，单位：毫秒；范围 0 至 5000，0 表示不使用阻塞读取。
    'block_ms' => 1000,
    // 检查并接管超时待确认消息的间隔，单位：毫秒，必须大于 0。
    'claim_interval_ms' => 1000,
    // 每次把到期延迟任务移入就绪队列的数量上限；必须大于 0，避免单批过大阻塞 Redis。
    'promotion_batch' => 100,
    // 检查到期延迟任务的间隔，单位：毫秒，必须大于 0；实际执行时间还受消费者负载影响。
    'promotion_interval_ms' => 1000,
    // 消费循环更新心跳的间隔，单位：毫秒，必须大于 0；执行长任务期间可能不更新。
    'heartbeat_interval_ms' => 10000,
    // 检查 queue:manage restart 重启标记的间隔，单位：毫秒，必须大于 0。
    // 收到标记后等待当前任务结束再退出；需由 Supervisor/systemd 等进程管理器重新拉起。
    'restart_interval_ms' => 1000,

    // 每个队列的活跃任务数量上限；0 表示不限。包含等待、延迟和处理中任务，不含失败、隔离历史。
    // 达到上限时拒绝新增任务并抛出 QueueFull；历史记录仍需单独清理。
    'max_active_jobs' => 0,
    // 单条完整 JSON 消息的字节上限，包含任务类、业务参数及元数据；1048576 字节 = 1 MiB，0 表示不限。
    // 超限入队抛出 PayloadTooLarge；调小前先检查旧消息，消费到超限旧消息会将其隔离。
    'max_payload_bytes' => 1048576,
    // 是否对异常重试增加随机抖动；true 在指数退避上限的一半（向上取整）至上限间随机取整数秒。
    // false 使用固定指数退避；初始退避由 Worker 的 --delay 指定，主动 release() 不增加此抖动。
    'retry_jitter' => true,
];
