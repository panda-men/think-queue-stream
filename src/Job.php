<?php
declare(strict_types=1);

namespace ThinkQueue\Streams;

use InvalidArgumentException;
use LogicException;
use Throwable;
use think\App;
use ThinkQueue\Streams\Exception\LeaseLost;

final class Job
{
    private bool $deleted = false;
    private bool $released = false;
    private bool $failed = false;
    private ?object $handler = null;

    /**
     * 创建携带本次处理权的任务对象。
     *
     * @param Transport $transport Redis 传输
     * @param App $app 应用容器
     * @param string $queue 队列名称
     * @param array<string, mixed> $reservation 消息预留信息
     */
    public function __construct(private Transport $transport, private App $app, private string $queue,
        private array $reservation)
    {
    }

    /**
     * 调用任务处理器，每次执行创建新的任务类实例。
     *
     * @return void
     */
    public function fire(): void
    {
        [$class, $method] = array_pad(explode('@', $this->payload('job'), 2), 2, 'fire');
        if (!str_contains($class, '\\')) {
            $class = $this->app->getNamespace() . '\\job\\' . $class;
        }
        $this->handler = $this->app->make($class, [], true);
        $this->handler->{$method}($this, $this->payload('data'));
    }

    /**
     * 持久化最终失败记录后调用业务失败回调。
     *
     * @param Throwable $exception 失败原因
     * @return string 失败记录 ID
     */
    public function fail(Throwable $exception): string
    {
        $this->assertUnsettled();
        $id = $this->transport->settle('fail', $this->queue, $this->reservation,
            substr($exception::class . ': ' . $exception->getMessage(), 0, 8192));
        if ($id === false) {
            throw new LeaseLost('任务已经被其他消费者接管或完成。');
        }
        $this->failed = $this->deleted = true;
        if ($this->handler !== null && method_exists($this->handler, 'failed')) {
            $this->handler->failed($this->payload('data'), $exception);
        }
        return (string) $id;
    }

    /**
     * 确认任务完成，同一任务对象重复确认安全。
     *
     * @return void
     */
    public function delete(): void
    {
        if ($this->deleted) {
            return;
        }
        $this->assertUnsettled();
        if (!$this->transport->settle('ack', $this->queue, $this->reservation)) {
            throw new LeaseLost('任务已经被其他消费者接管或完成。');
        }
        $this->deleted = true;
    }

    /**
     * 原子重投任务，保留稳定任务 ID 和尝试次数。
     *
     * @param int $delay 延迟秒数
     * @return void
     */
    public function release(int $delay = 0): void
    {
        if ($this->released) {
            return;
        }
        if ($delay < 0 || $delay > 31536000) {
            throw new InvalidArgumentException('重试延迟必须为 0 至 31536000 秒。');
        }
        $this->assertUnsettled();
        if (!$this->transport->settle('release', $this->queue, $this->reservation, $delay * 1000)) {
            throw new LeaseLost('任务已经被其他消费者接管或完成。');
        }
        $this->released = true;
    }

    /**
     * 获取消息字段，支持点号路径。
     *
     * @param string|null $name 字段路径
     * @param mixed $default 默认值
     * @return mixed 字段值或完整消息
     */
    public function payload(?string $name = null, mixed $default = null): mixed
    {
        $value = $this->reservation['payload'];
        foreach ($name === null ? [] : explode('.', $name) as $part) {
            if (!is_array($value) || !array_key_exists($part, $value)) {
                return $default;
            }
            $value = $value[$part];
        }
        return $value;
    }

    /**
     * 获取跨重投保持不变的任务 ID。
     *
     * @return string 任务 ID
     */
    public function getJobId(): string
    {
        return $this->payload('id');
    }

    /**
     * 获取本次投递使用的 Stream ID。
     *
     * @return string Stream ID
     */
    public function getMessageId(): string
    {
        return $this->reservation['message_id'];
    }

    /**
     * 获取领取次数，包含崩溃后的接管和主动重投。
     *
     * @return int 当前尝试次数
     */
    public function attempts(): int
    {
        return $this->reservation['attempts'];
    }

    /**
     * 判断本次任务是否已完成状态转换。
     *
     * @return bool 是否完成或重投
     */
    public function isDeletedOrReleased(): bool
    {
        return $this->deleted || $this->released;
    }

    /**
     * 判断是否已确认任务。
     *
     * @return bool 是否确认
     */
    public function isDeleted(): bool
    {
        return $this->deleted;
    }

    /**
     * 判断是否已重新投递任务。
     *
     * @return bool 是否重投
     */
    public function isReleased(): bool
    {
        return $this->released;
    }

    /**
     * 判断是否已存入失败队列。
     *
     * @return bool 是否最终失败
     */
    public function hasFailed(): bool
    {
        return $this->failed;
    }

    /**
     * 阻止完成后的任务再次转移，避免错误覆盖其他投递。
     *
     * @return void
     */
    private function assertUnsettled(): void
    {
        if ($this->isDeletedOrReleased()) {
            throw new LogicException('当前任务已经完成或重新投递。');
        }
    }
}
