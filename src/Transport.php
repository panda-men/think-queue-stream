<?php
declare(strict_types=1);

namespace ThinkQueue\Streams;

use InvalidArgumentException;
use Redis;
use RuntimeException;

final class Transport
{
    private const GROUP = 'workers';
    private array $claimCursors = [];
    private array $claimTimes = [];
    private array $initialized = [];
    private string $consumer;
    private array $config;
    private static array $scripts = [];

    /**
     * 初始化专用 Redis 传输；每个实例使用唯一消费者名称。
     *
     * @param Redis $redis 专用连接，不能启用客户端前缀或序列化
     * @param array<string, mixed> $config 队列配置
     */
    public function __construct(private Redis $redis, array $config = [])
    {
        $this->config = array_replace(require __DIR__ . '/../config/queue_streams.php', $config);
        foreach (['retry_after_ms', 'claim_interval_ms', 'promotion_batch'] as $name) {
            if (filter_var($this->config[$name], FILTER_VALIDATE_INT) === false || (int) $this->config[$name] < 1) {
                throw new InvalidArgumentException("{$name} 必须为正整数。");
            }
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
     * 为队列生成固定同槽键；第一版只允许一个消费组。
     *
     * @param string $queue 队列名称
     * @return list<string> 就绪、延迟、消息体、次数、租约、失败键
     */
    public function keys(string $queue): array
    {
        if (!preg_match('/\A[a-zA-Z0-9:_-]{1,100}\z/D', $queue)) {
            throw new InvalidArgumentException('队列名必须为 1 至 100 位字母、数字、冒号、下划线或连字符。');
        }
        $base = '{' . $this->config['prefix'] . ':' . $queue . '}';
        return array_map(static fn (string $suffix): string => $base . ':' . $suffix,
            ['ready', 'delayed', 'payloads', 'attempts', 'leases', 'failed']);
    }

    /**
     * 入队；Redis 连接异常直接向调用方传播，不盲目重试结果不明的写入。
     *
     * @param string $queue 队列名称
     * @param array<string, mixed> $payload 已验证消息体
     * @param int $delayMs 延迟毫秒
     * @return string 稳定任务 ID
     */
    public function enqueue(string $queue, array $payload, int $delayMs = 0): string
    {
        if ($delayMs < 0) {
            throw new InvalidArgumentException('延迟不能为负数。');
        }
        return (string) $this->script('enqueue', $queue, $payload['id'],
            json_encode($payload, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE), $delayMs);
    }

    /**
     * 将到期任务有界地迁入 Stream，多消费者可并行调用。
     *
     * @param string $queue 队列名称
     * @return int 本次迁移数量
     */
    public function promote(string $queue): int
    {
        return (int) $this->script('promote', $queue, (int) $this->config['promotion_batch']);
    }

    /**
     * 领取一个新任务或超时任务，并记录本次执行租约和尝试次数。
     *
     * @param string $queue 队列名称
     * @return array<string, mixed>|null 消息预留信息
     */
    public function pop(string $queue): ?array
    {
        $this->initialize($queue);
        $this->promote($queue);
        $stream = $this->keys($queue)[0];
        $entry = null;
        $time = (int) (hrtime(true) / 1000000);
        if ($time - ($this->claimTimes[$queue] ?? 0) >= (int) $this->config['claim_interval_ms']) {
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
            $args = ['GROUP', self::GROUP, $this->consumer, 'COUNT', 1];
            if ((int) $this->config['block_ms'] > 0) {
                array_push($args, 'BLOCK', (int) $this->config['block_ms']);
            }
            array_push($args, 'STREAMS', $stream, '>');
            $response = $this->command('XREADGROUP', ...$args);
            $entry = $response[0][1][0] ?? null;
        }
        if ($entry === null) {
            return null;
        }
        $fields = $this->fields($entry[1]);
        $id = $fields['job_id'] ?? throw new RuntimeException('Stream 消息缺少 job_id。');
        $token = bin2hex(random_bytes(16));
        $reserved = $this->script('reserve', $queue, self::GROUP, $entry[0], $this->consumer, $token, $id);
        if (!$reserved) {
            return null;
        }
        return ['payload' => json_decode($reserved[0], true, 512, JSON_THROW_ON_ERROR),
            'attempts' => (int) $reserved[1], 'message_id' => $entry[0], 'token' => $token];
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
        if ($limit < 1 || $limit > 1000) {
            throw new InvalidArgumentException('limit 必须在 1 至 1000 之间。');
        }
        $result = [];
        foreach ($this->command('XRANGE', $this->keys($queue)[5], $start, '+', 'COUNT', $limit) as $entry) {
            $result[$entry[0]] = $this->fields($entry[1]);
        }
        return $result;
    }

    /**
     * 原子重放一条失败消息，保留业务任务 ID 并重置尝试次数。
     *
     * @param string $queue 队列名称
     * @param string $failureId 失败 Stream ID
     * @return string|null 任务 ID，记录不存在时返回 null
     */
    public function retryFailed(string $queue, string $failureId): ?string
    {
        return $this->script('replay', $queue, $failureId) ?: null;
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
        return $this->command('XDEL', $this->keys($queue)[5], $failureId) === 1;
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
        return (string) $this->command('GET', $key);
    }

    /**
     * 在消费者停止后移除没有待确认任务的消费者元数据。
     *
     * @return void
     */
    public function closeConsumer(): void
    {
        foreach (array_keys($this->initialized) as $queue) {
            $stream = $this->keys($queue)[0];
            if (!$this->command('XPENDING', $stream, self::GROUP, '-', '+', 1, $this->consumer)) {
                $this->command('XGROUP', 'DELCONSUMER', $stream, self::GROUP, $this->consumer);
            }
        }
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
        return $this->command('EVAL', self::$scripts[$name], 6, ...$this->keys($queue), ...$arguments);
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
        $result = [];
        for ($index = 0; $index < count($values); $index += 2) {
            $result[$values[$index]] = $values[$index + 1];
        }
        return $result;
    }
}
