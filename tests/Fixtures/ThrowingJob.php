<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Tests\Fixtures;

use Redis;
use Throwable;
use ThinkQueue\Streams\Job;

final class ThrowingJob
{
    /**
     * 注入测试连接。
     *
     * @param Redis $redis 测试连接
     */
    public function __construct(private Redis $redis)
    {
    }

    /**
     * 记录执行并模拟业务异常。
     *
     * @param Job $job 任务对象
     * @param array<string, mixed> $data 测试数据
     * @return void
     */
    public function fire(Job $job, array $data): void
    {
        $this->redis->incr($data['counter']);
        throw new \RuntimeException('模拟业务失败');
    }

    /**
     * 验证最终失败回调在失败记录持久化后执行。
     *
     * @param array<string, mixed> $data 测试数据
     * @param Throwable $exception 失败原因
     * @return void
     */
    public function failed(array $data, Throwable $exception): void
    {
        $this->redis->incr($data['counter'] . ':failed');
    }
}
