<?php
declare(strict_types=1);

namespace ThinkQueue\Streams;

use InvalidArgumentException;
use Redis;
use ThinkQueue\Streams\Exception\FailureStorageException;
use ThinkQueue\Streams\Exception\JobIdConflict;
use ThinkQueue\Streams\Exception\PayloadTooLarge;
use ThinkQueue\Streams\Exception\QueueFull;

final class Transport
{
    private const GROUP = 'workers';
    private array $claimCursors = [];
    private array $claimTimes = [];
    private array $initialized = [];
    private array $promotionTimes = [];
    private array $heartbeatTimes = [];
    private ?int $restartTime = null;
    private string $restartValue = '';
    private string $consumer;
    private array $config;
    private static array $scripts = [];
    private array $replayCursors = [];

    /**
     * 初始化专用 Redis 传输；每个实例使用唯一消费者名称。
     *
     * @param Redis $redis 专用连接，不能启用客户端前缀或序列化
     * @param array<string, mixed> $config 队列配置
     * @param FailedJobStore|null $failedJobs 独立数据库失败存储，ThinkPHP 服务自动注入
     */
    public function __construct(private Redis $redis, array $config = [], private ?FailedJobStore $failedJobs = null)
    {
        $this->config = array_replace(require __DIR__ . '/../config/queue_streams.php', $config);
        foreach (['retry_after_ms', 'claim_interval_ms', 'promotion_batch', 'promotion_interval_ms',
            'heartbeat_interval_ms', 'restart_interval_ms'] as $name) {
            if (filter_var($this->config[$name], FILTER_VALIDATE_INT) === false || (int) $this->config[$name] < 1) {
                throw new InvalidArgumentException("{$name} 必须为正整数。");
            }
        }
        foreach (['max_active_jobs', 'max_payload_bytes'] as $name) {
            if (filter_var($this->config[$name], FILTER_VALIDATE_INT) === false || (int) $this->config[$name] < 0) {
                throw new InvalidArgumentException("{$name} 必须为非负整数。");
            }
        }
        if (!is_bool($this->config['retry_jitter'])) {
            throw new InvalidArgumentException('retry_jitter 必须为布尔值。');
        }
        if (filter_var($this->config['block_ms'], FILTER_VALIDATE_INT) === false ||
            (int) $this->config['block_ms'] < 0 || (int) $this->config['block_ms'] > 5000) {
            throw new InvalidArgumentException('block_ms 必须在 0 至 5000 之间，0 表示非阻塞。');
        }
        if (!preg_match('/\A[a-zA-Z0-9:_-]+\z/D', (string) $this->config['prefix'])) {
            throw new InvalidArgumentException('prefix 只能包含字母、数字、冒号、下划线和连字符。');
        }
        if ($redis->getOption(Redis::OPT_SERIALIZER) !== Redis::SERIALIZER_NONE || $redis->getOption(Redis::OPT_PREFIX)) {
            throw new InvalidArgumentException('队列要求不带客户端前缀、序列化的专用 Redis 连接。');
        }
        $readTimeout = (float) $redis->getOption(Redis::OPT_READ_TIMEOUT);
        if ($readTimeout > 0 && $readTimeout * 1000 <= (int) $this->config['block_ms']) {
            throw new InvalidArgumentException('Redis read_timeout 必须大于 block_ms。');
        }
        $this->consumer = (gethostname() ?: 'worker') . '-' . getmypid() . '-' . bin2hex(random_bytes(8));
    }

    /**
     * 获取当前传输的可见性超时。
     *
     * @return int 毫秒
     */
    public function retryAfterMs(): int
    {
        return (int) $this->config['retry_after_ms'];
    }

    /**
     * 获取本传输实例实际使用的消费者名称。
     * @return string 消费者名称
     */
    public function consumerName(): string
    {
        return $this->consumer;
    }

    public function retryJitter(): bool
    {
        return $this->config['retry_jitter'];
    }

    public function blockMilliseconds(): int
    {
        return (int) $this->config['block_ms'];
    }

    /**
     * 为队列生成固定同槽键；第一版只允许一个消费组。
     *
     * @param string $queue 队列名称
     * @return list<string> 就绪、延迟、消息体、次数、租约、待落库失败、隔离、心跳、重放回执键
     */
    public function keys(string $queue): array
    {
        if (!preg_match('/\A[a-zA-Z0-9:_-]{1,100}\z/D', $queue)) {
            throw new InvalidArgumentException('队列名必须为 1 至 100 位字母、数字、冒号、下划线或连字符。');
        }
        $base = '{' . $this->config['prefix'] . ':' . $queue . '}';
        return array_map(static fn (string $suffix): string => $base . ':' . $suffix,
            ['ready', 'delayed', 'payloads', 'attempts', 'leases', 'failed', 'quarantine', 'heartbeats', 'replays']);
    }

    /**
     * 入队；Redis 连接异常直接向调用方传播，不盲目重试结果不明的写入。
     *
     * @param string $queue 队列名称
     * @param array<string, mixed> $payload 已验证消息体
     * @param int $delayMs 延迟毫秒
     * @param bool $deduplicate 是否允许将相同内容的活动任务作为重复投递返回
     * @return string 稳定任务 ID
     */
    public function enqueue(string $queue, array $payload, int $delayMs = 0, bool $deduplicate = true): string
    {
        if ($delayMs < 0) {
            throw new InvalidArgumentException('延迟不能为负数。');
        }
        $json = json_encode($payload, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE);
        $result = $this->script('enqueue', $queue, $payload['id'], $json, $delayMs,
            (int) $this->config['max_active_jobs'], (int) $this->config['max_payload_bytes']);
        if ($result[0] === 'created' || ($deduplicate && Payload::sameTask($result[1], $json))) {
            return $payload['id'];
        }
        throw new JobIdConflict('活动任务 ID ' . $payload['id'] . ' 已被占用，未创建新任务。');
    }

    /**
     * 将到期任务有界地迁入 Stream，多消费者可并行调用。
     *
     * @param string $queue 队列名称
     * @return int 本次迁移数量
     */
    public function promote(string $queue): int
    {
        $result = $this->script('promote', $queue, (int) $this->config['promotion_batch']);
        foreach ($result[3] as $id) $this->persistFailure($queue, 'q:' . $id);
        $wait = $result[1] === '' ? (int) $this->config['promotion_interval_ms'] :
            min((int) $this->config['promotion_interval_ms'], max(0, (int) $result[1] - (int) $result[2]));
        $this->promotionTimes[$queue] = $this->monotonicMs() + $wait;
        return (int) $result[0];
    }

    /**
     * 领取一个新任务或超时任务，并记录本次执行租约和尝试次数。
     *
     * @param string $queue 队列名称
     * @return array<string, mixed>|null 消息预留信息
     */
    public function pop(string $queue): ?array
    {
        // 恢复后先归档历史/待落库失败。数据库异常会直接退出，不再制造新的失败积压。
        $this->migrateFailed($queue);
        if ($this->pendingFailures($queue) > 0) return null;
        $this->cleanupReplayReceipts($queue);
        $this->initialize($queue);
        $time = $this->monotonicMs();
        if ($time >= ($this->promotionTimes[$queue] ?? 0)) {
            $this->promote($queue);
        }
        if (!isset($this->heartbeatTimes[$queue]) ||
            $time - $this->heartbeatTimes[$queue] >= (int) $this->config['heartbeat_interval_ms']) {
            $this->script('consumer', $queue, 'heartbeat', $this->consumer, 0);
            $this->heartbeatTimes[$queue] = $time;
        }
        // 每轮最多隔离 100 条异常消息，避免损坏数据造成无界循环。
        for ($scanned = 0; $scanned < 100; $scanned++) {
            $entry = $this->readEntry($queue);
            if ($entry === null) {
                return null;
            }
            $id = '';
            $token = '';
            try {
                $fields = $this->fields($entry[1] ?? []);
                $id = $fields['job_id'] ?? '';
                if (!Payload::validId($id)) {
                    throw new \UnexpectedValueException('Stream 消息缺少合法的 job_id。');
                }
                $token = bin2hex(random_bytes(16));
                $reserved = $this->script('reserve', $queue, self::GROUP, $entry[0], $this->consumer, $token, $id);
                if (!$reserved) {
                    continue;
                }
                if ($reserved[0] === false || $reserved[0] === null) {
                    throw new \UnexpectedValueException('QUEUE_MISSING_PAYLOAD');
                }
                if ((int) $this->config['max_payload_bytes'] > 0 && strlen($reserved[0]) > (int) $this->config['max_payload_bytes']) {
                    throw new \UnexpectedValueException('消息超过 max_payload_bytes。');
                }
                $payload = Payload::decode($reserved[0], $id);
                return ['payload' => $payload, 'attempts' => (int) $reserved[1],
                    'message_id' => $entry[0], 'token' => $token];
            } catch (\JsonException|\UnexpectedValueException $exception) {
                $failure = $this->script('quarantine', $queue, self::GROUP, $entry[0], $this->consumer, $token, $id,
                    substr($exception::class . ': ' . $exception->getMessage(), 0, 8192),
                    json_encode($entry[1] ?? [], JSON_THROW_ON_ERROR | JSON_INVALID_UTF8_SUBSTITUTE));
                if ($failure) $this->persistFailure($queue, 'q:' . $failure);
            }
        }
        return null;
    }

    /** 领取单条记录；恢复与新任务读取均有界。 */
    private function readEntry(string $queue): ?array
    {
        $stream = $this->keys($queue)[0];
        $entry = null;
        $time = $this->monotonicMs();
        if (!isset($this->claimTimes[$queue]) ||
            $time - $this->claimTimes[$queue] >= (int) $this->config['claim_interval_ms']) {
            $this->claimTimes[$queue] = $time;
            $claimed = $this->command('XAUTOCLAIM', $stream, self::GROUP, $this->consumer,
                $this->retryAfterMs(), $this->claimCursors[$queue] ?? '0-0', 'COUNT', 1);
            $this->claimCursors[$queue] = $claimed[0] ?? '0-0';
            $entry = $claimed[1][0] ?? null;
            if ($entry !== null) {
                // 已发现待恢复任务时，下一轮继续恢复，避免恢复吞吐被检查间隔限制。
                $this->claimTimes[$queue] = 0;
            }
        }
        if ($entry === null) {
            $time = $this->monotonicMs();
            $args = ['GROUP', self::GROUP, $this->consumer, 'COUNT', 1];
            if ((int) $this->config['block_ms'] > 0) {
                // 及时执行下一次到期搬运和恢复扫描；BLOCK=0 在 Redis 中是无限等待。
                $block = min((int) $this->config['block_ms'],
                    max(1, ($this->promotionTimes[$queue] ?? $time + 1) - $time),
                    max(1, (int) $this->config['claim_interval_ms'] - ($time - ($this->claimTimes[$queue] ?? 0))));
                array_push($args, 'BLOCK', $block);
            }
            array_push($args, 'STREAMS', $stream, '>');
            $response = $this->command('XREADGROUP', ...$args);
            $entry = $response[0][1][0] ?? null;
        }
        return $entry;
    }

    /**
     * 校验处理权并确认完成、延迟重试或转入失败队列。
     *
     * @param string $action ack、release 或 fail
     * @param string $queue 队列名称
     * @param array<string, mixed> $reservation 领取结果
     * @param int|string $extra 延迟毫秒或错误详情
     * @return bool|string 成功标志或失败记录 ID；失去处理权时返回 false
     */
    public function settle(string $action, string $queue, array $reservation, int|string $extra = 0): bool|string
    {
        if (!in_array($action, ['ack', 'release', 'fail'], true)) {
            throw new InvalidArgumentException('未知任务状态转换。');
        }
        $result = $this->script($action, $queue, self::GROUP, $reservation['message_id'],
            $this->consumer, $reservation['token'], $reservation['payload']['id'], $extra);
        if ($action === 'fail' && $result) {
            $this->persistFailure($queue, (string) $result);
        }
        return $action === 'fail' ? ($result ?: false) : $result === 1;
    }

    /**
     * 查看失败记录，单次查询有界，避免一次载入全部失败任务。
     *
     * @param string $queue 队列名称
     * @param int $limit 最大条数
     * @param string $start 起始 ID，可使用括号表示排除端点
     * @return array<string, array<string, string>> 失败记录
     */
    public function failures(string $queue, int $limit = 100, string $start = '-'): array
    {
        $this->validatePage($limit);
        $this->migrateFailed($queue, 1000);
        return $this->failureStore()->records($this->config['prefix'], $queue, $limit, $start);
    }

    /** 迁移两类历史和待落库记录，合计最多 limit 条，提交成功后再删除 Redis 暂存。 */
    public function migrateFailed(string $queue, int $limit = 100): int
    {
        $this->validatePage($limit);
        $migrated = 0;
        // 每次只读取一条消息体，避免迁移上千条大载荷时把整批历史载入 PHP 内存。
        for ($processed = 0; $processed < $limit; $processed++) {
            $entries = $this->command('XRANGE', $this->keys($queue)[5], '-', '+', 'COUNT', 1);
            $prefix = '';
            if ($entries === []) {
                $entries = $this->command('XRANGE', $this->keys($queue)[6], '-', '+', 'COUNT', 1);
                $prefix = 'q:';
            }
            if ($entries === []) break;
            $migrated += $this->archiveFailure($queue, $prefix . $entries[0][0]);
        }
        return $migrated;
    }

    private function archiveFailure(string $queue, string $id): int
    {
        [$key, $streamId] = $this->failureLocation($queue, $id);
        $entries = $this->command('XRANGE', $key, $streamId, $streamId, 'COUNT', 1);
        if ($entries === []) return 0;
        $this->failureStore()->archive($this->config['prefix'], $queue, $id, $this->fields($entries[0][1]),
            fn (): bool => $this->command('XRANGE', $key, $streamId, $streamId, 'COUNT', 1) !== []);
        return (int) $this->command('XDEL', $key, $streamId);
    }

    /** 两个 Redis Stream 可以产生相同 ID，隔离记录使用 q: 前缀避免归档覆盖。 */
    private function failureLocation(string $queue, string $id): array
    {
        if (!preg_match('/\A(q:)?\d{1,20}-\d{1,20}\z/D', $id)) {
            throw new InvalidArgumentException('失败记录 ID 必须是 Stream ID，隔离记录可带 q: 前缀。');
        }
        $quarantined = str_starts_with($id, 'q:');
        return [$this->keys($queue)[$quarantined ? 6 : 5], $quarantined ? substr($id, 2) : $id];
    }

    /** 原子结算后归档失败必须让消费者退出，禁止将坏消息或已结算任务重新执行业务。 */
    private function persistFailure(string $queue, string $id): void
    {
        try {
            $this->archiveFailure($queue, $id);
        } catch (\Throwable $exception) {
            throw new FailureStorageException('失败记录尚未确认写入数据库，已保留 Redis 待落库记录；请修复数据库后重启消费者。',
                0, $exception);
        }
    }

    /** 查看两类待落库总数，管理脚本可在数据库恢复前只读检查。 */
    public function pendingFailures(string $queue): int
    {
        $keys = $this->keys($queue);
        return (int) $this->command('XLEN', $keys[5]) + (int) $this->command('XLEN', $keys[6]);
    }

    /**
     * 重放一条数据库失败消息，行锁与 Redis 回执防止并发重放和响应丢失后重复发布。
     *
     * @param string $queue 队列名称
     * @param string $failureId 失败 Stream ID
     * @return string|null 任务 ID，记录不存在时返回 null
     */
    public function retryFailed(string $queue, string $failureId): ?string
    {
        $this->archiveFailure($queue, $failureId);
        $result = $this->failureStore()->remove($this->config['prefix'], $queue, $failureId,
            function (array $record) use ($queue, $failureId): string {
                if ($record['failure_kind'] === 'quarantined') {
                    throw new InvalidArgumentException('隔离坏消息不能直接重放，请先修复消息格式，再用合法参数重新发布。');
                }
                $payload = Payload::decode($record['payload'], $record['job_id']);
                // 先清除已落库的同 ID 暂存，防止迁移进程在重放后重新归档旧历史。
                $this->command('XDEL', $this->keys($queue)[5], $failureId);
                return $this->script('replay', $queue, $failureId, $payload['id'], $record['payload'],
                    (int) $this->config['max_active_jobs'], (int) $this->config['max_payload_bytes']);
            });
        // 数据库提交后才清回执；崩溃遗留回执由后续消费有界回收。
        $this->command('HDEL', $this->keys($queue)[8], $failureId);
        return $result;
    }

    /** 从统一失败表筛选坏消息；保留原有 quarantine_id 查询接口。 */
    public function quarantined(string $queue, int $limit = 100, string $start = '-'): array
    {
        $this->validatePage($limit);
        $result = [];
        $this->migrateFailed($queue, 1000);
        $exclusive = str_starts_with($start, '(');
        $id = $exclusive ? substr($start, 1) : $start;
        $cursor = $id === '-' ? '-' : ($exclusive ? '(' : '') . (str_starts_with($id, 'q:') ? $id : 'q:' . $id);
        foreach ($this->failureStore()->records($this->config['prefix'], $queue, $limit, $cursor, 'quarantined') as $id => $record) {
            $result[substr($id, 2)] = ['failure_id' => $id] + $record;
        }
        return $result;
    }

    public function forgetQuarantined(string $queue, string $id): bool
    {
        return $this->forgetFailed($queue, str_starts_with($id, 'q:') ? $id : 'q:' . $id);
    }

    /** 按年龄有界清理；默认由调用方显式触发，dry-run 只返回候选 ID。 */
    public function prune(string $queue, string $kind, int $olderThanSeconds, int $limit = 100, bool $dryRun = true): array
    {
        $this->validatePage($limit);
        if (!in_array($kind, ['failed', 'quarantined'], true) || $olderThanSeconds < 1) {
            throw new InvalidArgumentException('kind 必须为 failed/quarantined，older-than 必须为正秒数。');
        }
        $this->migrateFailed($queue, 1000);
        $ids = $this->failureStore()->older($this->config['prefix'], $queue,
            (int) floor(microtime(true) * 1000) - $olderThanSeconds * 1000, $limit,
            $kind === 'quarantined' ? 'quarantined' : null);
        if (!$dryRun) foreach ($ids as $id) $this->forgetFailed($queue, $id);
        return $ids;
    }

    /** 活跃队列为 Redis 原子快照；failed 单独采样数据库，failed_pending 为待落库数量。 */
    public function stats(string $queue, int $limit = 100, int $offset = 0): array
    {
        $this->validatePage($limit);
        if ($offset < 0) {
            throw new InvalidArgumentException('offset 不能为负数。');
        }
        $data = $this->script('stats', $queue, $limit, $offset);
        $consumers = [];
        for ($i = 0; $i < count($data[9]); $i += 2) {
            $consumers[] = ['name' => $data[9][$i], 'last_seen_ms' => (int) $data[9][$i + 1],
                'heartbeat_age_ms' => max(0, (int) $data[0] - (int) $data[9][$i + 1])];
        }
        $first = $data[8][0] ?? null;
        return ['queue' => $queue, 'group' => self::GROUP, 'sampled_at_ms' => (int) $data[0],
            'active' => (int) $data[1], 'ready_entries' => (int) $data[2],
            'waiting' => max(0, (int) $data[2] - (int) $data[3]), 'pending' => (int) $data[3],
            'delayed' => (int) $data[4], 'failed' => $this->failureStore()->count($this->config['prefix'], $queue),
            'failed_pending' => (int) $data[5] + (int) $data[6],
            'quarantined' => $this->failureStore()->count($this->config['prefix'], $queue, 'quarantined'),
            'quarantined_pending' => (int) $data[6],
            'consumer_count' => (int) $data[7], 'heartbeat_count' => (int) $data[10],
            'consumers' => $consumers, 'next_offset' => $offset + count($consumers) < (int) $data[10] ? $offset + count($consumers) : null,
            'first_pending' => $first === null ? null : ['message_id' => $first[0], 'consumer' => $first[1],
                'idle_ms' => (int) $first[2], 'deliveries' => (int) $first[3]],
            'next_delayed_at_ms' => $data[11] === '' ? null : (int) $data[11],
            'counts_consistent' => (int) $data[1] === (int) $data[2] + (int) $data[4]];
    }

    /** 只清理超过阈值且没有 Pending 的指定消费者；不接管或删除任务。 */
    public function cleanupConsumer(string $queue, string $consumer, int $idleMs): bool
    {
        if ($consumer === '' || $idleMs < max($this->retryAfterMs(), (int) $this->config['heartbeat_interval_ms'] * 2)) {
            throw new InvalidArgumentException('idle-ms 至少为可见性超时与两倍心跳间隔中的较大值。');
        }
        return $this->script('consumer', $queue, 'cleanup', $consumer, $idleMs) === 1;
    }

    /**
     * 删除指定失败记录。
     *
     * @param string $queue 队列名称
     * @param string $failureId 失败 Stream ID
     * @return bool 是否删除
     */
    public function forgetFailed(string $queue, string $failureId): bool
    {
        $this->archiveFailure($queue, $failureId);
        $deleted = $this->failureStore()->remove($this->config['prefix'], $queue, $failureId,
            function () use ($queue, $failureId): bool {
                [$key, $id] = $this->failureLocation($queue, $failureId);
                $this->command('XDEL', $key, $id);
                return true;
            });
        $this->command('HDEL', $this->keys($queue)[8], $failureId);
        return $deleted === true;
    }

    /** 仅回收数据库记录已移除的重放回执；遗留量不会随正常失败历史增长。 */
    private function cleanupReplayReceipts(string $queue): void
    {
        $key = $this->keys($queue)[8];
        $page = $this->command('HSCAN', $key, $this->replayCursors[$queue] ?? '0', 'COUNT', 100);
        $this->replayCursors[$queue] = $page[0];
        for ($i = 0; $i < count($page[1]); $i += 2) {
            $id = $page[1][$i];
            if (!$this->failureStore()->exists($this->config['prefix'], $queue, $id)) $this->command('HDEL', $key, $id);
        }
    }

    private function failureStore(): FailedJobStore
    {
        return $this->failedJobs ?? throw new \RuntimeException('请通过 ThinkPHP 服务创建队列，或向 Transport 第三个参数注入 FailedJobStore。');
    }

    /**
     * 统计尚未完成的任务，包含处理中任务和延迟任务。
     *
     * @param string $queue 队列名称
     * @return int 未完成数量
     */
    public function size(string $queue): int
    {
        $keys = $this->keys($queue);
        return (int) $this->command('XLEN', $keys[0]) + (int) $this->command('ZCARD', $keys[1]);
    }

    /**
     * 获取或更新消费者重启标记。
     *
     * @param bool $write 是否产生新的重启标记
     * @return string 当前标记
     */
    public function restartToken(bool $write = false): string
    {
        $key = $this->config['prefix'] . ':restart';
        if ($write) {
            $this->command('SET', $key, bin2hex(random_bytes(16)));
        }
        $time = $this->monotonicMs();
        if ($write || $this->restartTime === null || $time - $this->restartTime >= (int) $this->config['restart_interval_ms']) {
            $this->restartValue = (string) $this->command('GET', $key);
            $this->restartTime = $time;
        }
        return $this->restartValue;
    }

    /**
     * 在消费者停止后移除没有待确认任务的消费者元数据。
     *
     * @return void
     */
    public function closeConsumer(): void
    {
        foreach (array_keys($this->initialized) as $queue) {
            $this->script('consumer', $queue, 'close', $this->consumer, 0);
        }
        $this->heartbeatTimes = [];
    }

    /**
     * 从队列开头创建唯一消费组，仅忽略已经存在的分组错误。
     *
     * @param string $queue 队列名称
     * @return void
     */
    private function initialize(string $queue): void
    {
        if (!isset($this->initialized[$queue])) {
            try {
                $this->command('XGROUP', 'CREATE', $this->keys($queue)[0], self::GROUP, '0', 'MKSTREAM');
            } catch (\RedisException $exception) {
                if (!str_contains($exception->getMessage(), 'BUSYGROUP')) {
                    throw $exception;
                }
            }
            $this->initialized[$queue] = true;
        }
    }

    /**
     * 执行预先缓存的本地 Lua 源码。
     *
     * @param string $name 脚本名
     * @param string $queue 队列名称
     * @param mixed ...$arguments 脚本参数
     * @return mixed Redis 返回值
     */
    private function script(string $name, string $queue, mixed ...$arguments): mixed
    {
        self::$scripts[$name] ??= file_get_contents(__DIR__ . '/Lua/common.lua') . "\n" .
            file_get_contents(__DIR__ . '/Lua/' . $name . '.lua');
        $source = self::$scripts[$name];
        $keys = $this->keys($queue);
        try {
            try {
                return $this->command('EVALSHA', sha1($source), count($keys), ...$keys, ...$arguments);
            } catch (\RedisException $exception) {
                if (!str_starts_with($exception->getMessage(), 'NOSCRIPT')) {
                    throw $exception;
                }
                // 仅 NOSCRIPT 确认脚本尚未执行；连接超时等结果不明的错误不重发。
                $this->command('SCRIPT', 'LOAD', $source);
                return $this->command('EVALSHA', sha1($source), count($keys), ...$keys, ...$arguments);
            }
        } catch (\RedisException $exception) {
            // Redis 7 的 error_reply 可能带 ERR 前缀；只匹配完整错误码。
            if (in_array($exception->getMessage(), ['QUEUE_PAYLOAD_TOO_LARGE', 'ERR QUEUE_PAYLOAD_TOO_LARGE'], true)) {
                throw new PayloadTooLarge('消息超过 max_payload_bytes。', 0, $exception);
            }
            if (in_array($exception->getMessage(), ['QUEUE_FULL', 'ERR QUEUE_FULL'], true)) {
                throw new QueueFull('活跃任务数量达到 max_active_jobs。', 0, $exception);
            }
            throw $exception;
        }
    }

    /**
     * 发送 RESP 原生命令，区分空结果和 Redis 错误。
     *
     * @param string $name 命令名称
     * @param mixed ...$arguments 命令参数
     * @return mixed Redis 返回值
     */
    private function command(string $name, mixed ...$arguments): mixed
    {
        $this->redis->clearLastError();
        $result = $this->redis->rawCommand($name, ...$arguments);
        if ($result === false && ($error = $this->redis->getLastError())) {
            throw new \RedisException($error);
        }
        return $result;
    }

    /**
     * 解码 Stream 返回的交错字段数组。
     *
     * @param list<string> $values 字段和值
     * @return array<string, string> 字段映射
     */
    private function fields(array $values): array
    {
        if (count($values) % 2 !== 0) {
            throw new \UnexpectedValueException('Stream 字段和值数量不匹配。');
        }
        $result = [];
        for ($index = 0; $index < count($values); $index += 2) {
            $result[$values[$index]] = $values[$index + 1];
        }
        return $result;
    }

    private function validatePage(int $limit): void
    {
        if ($limit < 1 || $limit > 1000) {
            throw new InvalidArgumentException('limit 必须在 1 至 1000 之间。');
        }
    }

    private function monotonicMs(): int
    {
        return (int) (hrtime(true) / 1000000);
    }
}
