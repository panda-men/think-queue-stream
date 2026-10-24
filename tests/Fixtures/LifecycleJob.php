<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Tests\Fixtures;

use ThinkQueue\Streams\Job;

final class LifecycleJob
{
    /**
     * 模拟成功、主动重投及包含敏感内容的异常。
     * @param Job $job 任务
     * @param array<string,mixed> $data 测试参数
     * @return void
     */
    public function fire(Job $job, array $data): void
    {
        if (($data['action'] ?? '') === 'safe-throw') throw new DiagnosticFailure('报表不存在');
        if (($data['action'] ?? '') === 'throw') throw new \RuntimeException('smtp-password SECRET receiver@example.test');
        if (($data['action'] ?? '') === 'release') { $job->release(7); return; }
        $job->delete();
        if (($data['action'] ?? '') === 'ack-throw') throw new \RuntimeException('SECRET after ack');
    }
}
