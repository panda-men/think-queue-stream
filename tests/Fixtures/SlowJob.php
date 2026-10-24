<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Tests\Fixtures;

use Redis;
use ThinkQueue\Streams\Job;

final class SlowJob
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
     * 模拟处理完成前进程退出的长任务。
     *
     * @param Job $job 任务对象
     * @param array<string, mixed> $data 测试数据
     * @return void
     */
    public function fire(Job $job, array $data): void
    {
        $this->redis->incr($data['counter']);
        sleep(5);
        $job->delete();
    }
}
