<?php
declare(strict_types=1);

namespace ThinkQueue\Streams;

use InvalidArgumentException;

/** 主进程配置快照；完整验证后才允许替换正在使用的配置。 */
final class SupervisorConfig
{
    private const WORKER_DEFAULTS = [
        'processes' => 1, 'tries' => 3, 'timeout' => 60, 'delay' => 3,
        'memory' => 128, 'max_jobs' => 1000, 'max_time' => 3600,
    ];

    private function __construct(
        public readonly array $workers,
        public readonly int $maxProcesses,
        public readonly int $stopWaitSeconds,
        public readonly int $restartDelay,
        public readonly int $restartMaxDelay,
    ) {}

    /** 每次从磁盘重新执行 PHP 配置，避免常驻进程的配置缓存和 CLI OPcache 影响重载。 */
    public static function load(string $file): self
    {
        clearstatcache(true, $file);
        if (!is_file($file) || !is_readable($file)) {
            throw new InvalidArgumentException('队列配置文件不存在或不可读：' . $file);
        }
        if (function_exists('opcache_invalidate')) opcache_invalidate($file, true);
        $config = (static fn (string $path) => require $path)($file);
        if (!is_array($config)) throw new InvalidArgumentException('队列配置文件必须返回数组。');
        return self::fromArray($config);
    }

    /** 校验队列名称、并发预算和现有 stream:work 参数；空 workers 表示暂停全部消费。 */
    public static function fromArray(array $config): self
    {
        $config += ['supervisor' => [], 'retry_after_ms' => 90000];
        $supervisor = $config['supervisor'];
        if (!is_array($supervisor)) throw new InvalidArgumentException('supervisor 必须是数组。');
        self::knownKeys($supervisor, ['workers', 'max_processes', 'stop_wait_seconds', 'restart_delay', 'restart_max_delay'], 'supervisor');
        // 只为缺失项补默认值；显式 null 属于配置错误，不能静默清空 Worker。
        $supervisor += ['workers' => [], 'max_processes' => 32, 'stop_wait_seconds' => 120,
            'restart_delay' => 1, 'restart_max_delay' => 30];
        $max = self::integer($supervisor['max_processes'], 'max_processes', 1);
        $wait = self::integer($supervisor['stop_wait_seconds'], 'stop_wait_seconds', 1);
        $delay = self::integer($supervisor['restart_delay'], 'restart_delay', 1);
        $maxDelay = self::integer($supervisor['restart_max_delay'], 'restart_max_delay', $delay);
        $retryAfter = self::integer($config['retry_after_ms'], 'retry_after_ms', 1);
        $workers = $supervisor['workers'];
        if (!is_array($workers)) throw new InvalidArgumentException('supervisor.workers 必须是队列名称到配置的数组。');
        $normalized = [];
        $total = 0;
        foreach ($workers as $queue => $options) {
            $queue = (string) $queue;
            if (!preg_match('/\A[a-zA-Z0-9:_-]{1,100}\z/D', $queue) || !is_array($options)) {
                throw new InvalidArgumentException('workers 必须使用合法队列名称作为键，配置必须是数组。');
            }
            self::knownKeys($options, array_keys(self::WORKER_DEFAULTS), 'workers.' . $queue);
            $options = array_replace(self::WORKER_DEFAULTS, $options);
            foreach ($options as $key => $value) {
                $options[$key] = self::integer($value, $queue . '.' . $key, in_array($key, ['processes', 'delay'], true) ? 0 : 1);
            }
            if ($options['delay'] > 3600 || $options['timeout'] > intdiv($retryAfter - 1, 1000)) {
                throw new InvalidArgumentException('Worker delay 必须在 0 至 3600 秒之间，timeout * 1000 必须小于 retry_after_ms。');
            }
            if ($options['processes'] > $max - $total) {
                throw new InvalidArgumentException('Worker 总并发不能超过 supervisor.max_processes。');
            }
            $total += $options['processes'];
            $normalized[$queue] = $options;
        }
        return new self($normalized, $max, $wait, $delay, $maxDelay);
    }

    /** 将配置展开为有稳定编号的 Worker 槽位，避免自动重启改变并发数。 */
    public function slots(): array
    {
        $slots = [];
        foreach ($this->workers as $queue => $options) {
            for ($index = 0; $index < $options['processes']; $index++) {
                $slots[$queue . ':' . $index] = ['queue' => (string) $queue, 'options' => $options];
            }
        }
        return $slots;
    }

    private static function integer(mixed $value, string $name, int $minimum): int
    {
        $number = is_int($value) || is_string($value) ? filter_var($value, FILTER_VALIDATE_INT) : false;
        if ($number === false || $number < $minimum) {
            throw new InvalidArgumentException($name . ' 必须是大于等于 ' . $minimum . ' 的整数。');
        }
        return $number;
    }

    private static function knownKeys(array $options, array $allowed, string $name): void
    {
        if (array_diff(array_keys($options), $allowed) !== []) {
            throw new InvalidArgumentException($name . ' 包含未知配置项。');
        }
    }
}
