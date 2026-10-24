<?php
declare(strict_types=1);

namespace ThinkQueue\Streams;

use Closure;
use DateInterval;
use DateTimeImmutable;
use DateTimeInterface;
use InvalidArgumentException;
use think\App;
use ThinkQueue\Streams\Exception\JobIdConflict;

final class Queue
{
    private const MAX_ID_ATTEMPTS = 5;

    /**
     * 创建队列入口。
     *
     * @param Transport $transport Redis 传输
     * @param App $app ThinkPHP 容器
     * @param string $defaultQueue 默认队列
     * @param Closure|null $idGenerator 可选的 ID 生成器，每次返回一个合法字符串 ID
     * @param Monitor|null $monitor 可选的运行日志
     */
    public function __construct(private Transport $transport, private App $app, private string $defaultQueue = 'default',
        private ?Closure $idGenerator = null, private ?Monitor $monitor = null)
    {
        $transport->keys($defaultQueue);
    }

    /**
     * 发布即时任务，不支持 PHP 对象序列化。
     *
     * @param string $job 任务类或任务类@方法
     * @param mixed $data JSON 可表示的业务参数
     * @param string|null $queue 队列名称
     * @param array<string, mixed> $options id、max_tries、timeout 配置
     * @return string 任务 ID
     */
    public function push(string $job, mixed $data = [], ?string $queue = null, array $options = []): string
    {
        return $this->later(0, $job, $data, $queue, $options);
    }

    /**
     * 发布延迟任务，整数延迟的单位保持为秒。
     *
     * @param int|DateTimeInterface|DateInterval $delay 秒数、目标时间或时间间隔
     * @param string $job 任务类或任务类@方法
     * @param mixed $data JSON 可表示的业务参数
     * @param string|null $queue 队列名称
     * @param array<string, mixed> $options 任务选项
     * @return string 任务 ID
     */
    public function later(int|DateTimeInterface|DateInterval $delay, string $job, mixed $data = [],
        ?string $queue = null, array $options = []): string
    {
        if (!Payload::validJob($job)) {
            throw new InvalidArgumentException('任务名称必须是类名或类名@方法。');
        }
        $customId = isset($options['id']);
        if ($customId && (!is_string($options['id']) || !Payload::validId($options['id']))) {
            throw new InvalidArgumentException('任务 ID 格式不正确。');
        }
        foreach (['max_tries', 'timeout'] as $name) {
            if (isset($options[$name]) && (!is_int($options[$name]) || $options[$name] < 1)) {
                throw new InvalidArgumentException("{$name} 必须为正整数。");
            }
        }
        $now = new DateTimeImmutable();
        $target = $delay instanceof DateInterval ? $now->add($delay) : $delay;
        $seconds = $target instanceof DateTimeInterface ? max(0, $target->getTimestamp() - $now->getTimestamp()) : $target;
        if ($seconds < 0 || $seconds > 31536000) {
            throw new InvalidArgumentException('延迟必须为 0 至 31536000 秒。');
        }
        $payload = ['version' => 1, 'id' => '', 'job' => ltrim($job, '\\'), 'data' => $data,
            'max_tries' => $options['max_tries'] ?? null, 'timeout' => $options['timeout'] ?? null,
            'created_at' => time()];
        $conflict = null;
        for ($attempt = 0; $attempt < self::MAX_ID_ATTEMPTS; $attempt++) {
            $id = $customId ? $options['id'] : ($this->idGenerator !== null ? ($this->idGenerator)() :
                bin2hex(random_bytes(16)));
            if (!is_string($id) || !Payload::validId($id)) {
                throw new InvalidArgumentException('任务 ID 生成器必须返回合法的字符串 ID。');
            }
            $payload['id'] = $id;
            try {
                // 只重试 Redis 明确返回的 ID 冲突；网络异常等结果不明的写入直接传播。
                $result = $this->transport->enqueue($queue ?? $this->defaultQueue, $payload, $seconds * 1000, $customId);
                $this->monitor?->record('published', $queue ?? $this->defaultQueue,
                    ['job_id' => $result, 'job' => $payload['job'], 'delay_seconds' => $seconds]);
                return $result;
            } catch (JobIdConflict $exception) {
                if ($customId) {
                    $this->monitor?->record('publish_failed', $queue ?? $this->defaultQueue,
                        ['job_id' => $id, 'job' => $payload['job'], 'exception' => $exception::class]);
                    throw $exception;
                }
                $conflict = $exception;
            } catch (\Throwable $exception) {
                $this->monitor?->record('publish_failed', $queue ?? $this->defaultQueue,
                    ['job_id' => $id, 'job' => $payload['job'], 'exception' => $exception::class]);
                throw $exception;
            }
        }
        $this->monitor?->record('publish_failed', $queue ?? $this->defaultQueue,
            ['job' => $payload['job'], 'exception' => JobIdConflict::class, 'reason' => 'id_conflicts_exhausted']);
        throw new JobIdConflict('连续 5 次生成的任务 ID 均已被活动任务占用，未创建新任务。', 0, $conflict);
    }

    /**
     * 领取任务并注入应用容器。
     *
     * @param string|null $queue 队列名称
     * @return Job|null 任务对象
     */
    public function pop(?string $queue = null): ?Job
    {
        $queue ??= $this->defaultQueue;
        $reservation = $this->transport->pop($queue);
        return $reservation === null ? null : new Job($this->transport, $this->app, $queue, $reservation);
    }

    /**
     * 获取底层传输，用于队列管理与消费者配置。
     *
     * @return Transport Redis 传输
     */
    public function transport(): Transport
    {
        return $this->transport;
    }

    /**
     * 获取默认队列。
     *
     * @return string 队列名称
     */
    public function defaultQueue(): string
    {
        return $this->defaultQueue;
    }

    /**
     * 获取未完成任务数量。
     *
     * @param string|null $queue 队列名称
     * @return int 数量
     */
    public function size(?string $queue = null): int
    {
        return $this->transport->size($queue ?? $this->defaultQueue);
    }
}
