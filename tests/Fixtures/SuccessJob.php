<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Tests\Fixtures;

use Redis;
use ThinkQueue\Streams\Job;

final class SuccessJob
{
    /**
     * 注入测试专用 Redis。
     *
     * @param Redis $redis 测试连接
     */
    public function __construct(private Redis $redis)
    {
    }

    /**
     * 记录实际业务执行次数并确认任务。
     *
     * @param Job $job 任务对象
     * @param array<string, mixed> $data 测试数据
     * @return void
     */
    public function fire(Job $job, array $data): void
    {
        $this->redis->incr($data['counter']);
        $job->delete();
    }
}
