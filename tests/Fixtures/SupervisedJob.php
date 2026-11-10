<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Tests\Fixtures;

use Redis;
use ThinkQueue\Streams\Job;

/** 通过真实 Redis 标记任务开始、结束和 Worker PID，验证进程切换期间的执行结果。 */
final class SupervisedJob
{
    public function __construct(private Redis $redis) {}

    public function fire(Job $job, array $data): void
    {
        $this->redis->set($data['started'], (string) getmypid());
        $until = hrtime(true) + ($data['wait_ms'] ?? 0) * 1000000;
        while (hrtime(true) < $until) usleep(10000);
        $this->redis->incr($data['completed']);
        $job->delete();
    }
}
