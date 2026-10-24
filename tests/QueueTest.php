<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Tests;

use PHPUnit\Framework\TestCase;
use Redis;
use think\App;
use think\Console;
use ThinkQueue\Streams\Exception\LeaseLost;
use ThinkQueue\Streams\Queue;
use ThinkQueue\Streams\RedisConnection;
use ThinkQueue\Streams\Transport;
use ThinkQueue\Streams\Worker;
use ThinkQueue\Streams\Tests\Fixtures\SlowJob;
use ThinkQueue\Streams\Tests\Fixtures\SuccessJob;
use ThinkQueue\Streams\Tests\Fixtures\ThrowingJob;

final class QueueTest extends TestCase
{
    private Redis $redis;
    private App $app;
    private Queue $queue;
    private array $config;

    /**
     * 为每个测试建立隔离的键前缀，Redis 不可用时让测试失败。
     *
     * @return void
     */
    protected function setUp(): void
    {
        $this->config = ['host' => getenv('REDIS_HOST') ?: '127.0.0.1',
            'port' => (int) (getenv('REDIS_PORT') ?: 6379), 'prefix' => 'test-' . bin2hex(random_bytes(8)),
            'block_ms' => 0, 'retry_after_ms' => 2000, 'claim_interval_ms' => 1];
        $this->redis = RedisConnection::connect($this->config);
        $this->app = new App();
        $this->app->instance(Redis::class, $this->redis);
        $this->queue = $this->newQueue();
        $this->app->instance(Queue::class, $this->queue);
    }

    /**
     * 仅删除本测试拥有的键，绝不清空 Redis 数据库。
     *
     * @return void
     */
    protected function tearDown(): void
    {
        foreach ($this->redis->keys('*' . $this->config['prefix'] . '*') as $key) {
            $this->redis->del($key);
        }
        Console::flushStartCallbacks();
    }

    /**
     * 验证即时投递、JSON 参数、手动确认和元数据清理。
     *
     * @return void
     */
    public function testImmediateTaskAndAcknowledgement(): void
    {
        $id = $this->queue->push(SuccessJob::class, ['nested' => ['value' => '中文']]);
        self::assertSame(1, $this->queue->size());
        $job = $this->queue->pop();
        self::assertSame($id, $job->getJobId());
        self::assertSame('中文', $job->payload('data.nested.value'));
        self::assertSame(1, $job->attempts());
        $job->delete();
        $job->delete();
        self::assertSame(0, $this->queue->size());
        foreach (array_slice($this->queue->transport()->keys('default'), 2, 3) as $key) {
            self::assertSame(0, $this->redis->hLen($key));
        }
    }

    /**
     * 验证真实延迟到期前不可领取，到期后由消费者迁移并执行。
     *
     * @return void
     */
    public function testDelayedTask(): void
    {
        $this->queue->later(1, SuccessJob::class);
        self::assertNull($this->queue->pop());
        usleep(1100000);
        self::assertNotNull($this->queue->pop());
    }

    /**
     * 验证重投保留任务 ID、改变投递 ID 并递增领取次数。
     *
     * @return void
     */
    public function testReleaseRetainsIdentityAndAttempts(): void
    {
        $id = $this->queue->push(SuccessJob::class);
        $first = $this->queue->pop();
        $first->release(0);
        $first->release(0);
        self::assertSame(1, $this->queue->size());
        $second = $this->queue->pop();
        self::assertSame($id, $second->getJobId());
        self::assertNotSame($first->getMessageId(), $second->getMessageId());
        self::assertSame(2, $second->attempts());
        $second->delete();
    }

    /**
     * 验证重复执行状态转换不会产生第二条重试消息。
     *
     * @return void
     */
    public function testRepeatedTransitionIsSafe(): void
    {
        $this->queue->push(SuccessJob::class);
        $transport = $this->queue->transport();
        $reservation = $transport->pop('default');
        self::assertTrue($transport->settle('release', 'default', $reservation, 0));
        self::assertFalse($transport->settle('release', 'default', $reservation, 0));
        self::assertSame(1, $this->queue->size());
    }

    /**
     * 验证延迟重投路径在到期前不可执行，到期后保留原 ID 和次数。
     *
     * @return void
     */
    public function testDelayedReleaseWaitsForDueTime(): void
    {
        $id = $this->queue->push(SuccessJob::class);
        $this->queue->pop()->release(1);
        self::assertSame(1, $this->queue->size());
        self::assertNull($this->queue->pop());
        usleep(1100000);
        $retried = $this->queue->pop();
        self::assertSame($id, $retried->getJobId());
        self::assertSame(2, $retried->attempts());
        $retried->delete();
    }

    /**
     * 验证失败记录可重放一次且重新计数。
     *
     * @return void
     */
    public function testFailureAndReplay(): void
    {
        $id = $this->queue->push(SuccessJob::class);
        $failure = $this->queue->pop()->fail(new \RuntimeException('失败详情'));
        $transport = $this->queue->transport();
        self::assertSame(0, $this->queue->size());
        self::assertCount(1, $transport->failures('default'));
        self::assertSame($id, $transport->retryFailed('default', $failure));
        self::assertNull($transport->retryFailed('default', $failure));
        self::assertSame(1, $this->queue->pop()->attempts());
        self::assertCount(0, $transport->failures('default'));
    }

    /**
     * 验证真实业务异常最多执行三次，最终失败回调只执行一次。
     *
     * @return void
     */
    public function testWorkerRetriesAndFailedCallback(): void
    {
        $counter = $this->config['prefix'] . ':counter';
        $this->queue->push(ThrowingJob::class, ['counter' => $counter]);
        $worker = new Worker($this->queue);
        for ($attempt = 0; $attempt < 3; $attempt++) {
            self::assertTrue($worker->runNextJob(null, 3, 1, 0));
        }
        self::assertFalse($worker->runNextJob(null, 3, 1, 0));
        self::assertSame('3', $this->redis->get($counter));
        self::assertSame('1', $this->redis->get($counter . ':failed'));
        self::assertCount(1, $this->queue->transport()->failures('default'));
    }

    /**
     * 验证 ThinkPHP 容器注入与成功处理。
     *
     * @return void
     */
    public function testWorkerSuccessfulExecution(): void
    {
        $counter = $this->config['prefix'] . ':counter';
        $this->queue->push(SuccessJob::class, ['counter' => $counter]);
        self::assertTrue((new Worker($this->queue))->runNextJob(null, 3, 1, 0));
        self::assertSame('1', $this->redis->get($counter));
        self::assertSame(0, $this->queue->size());
    }

    /**
     * 验证接管后的旧消费者不能确认或重投新消费者持有的任务。
     *
     * @return void
     */
    public function testStaleOwnerCannotAcknowledgeOrRelease(): void
    {
        $this->queue->push(SuccessJob::class);
        $old = $this->queue->pop();
        usleep(100000);
        $new = $this->newQueue(['retry_after_ms' => 50])->pop();
        self::assertNotNull($new);
        self::assertSame(2, $new->attempts());
        foreach (['delete', 'release'] as $method) {
            try {
                $old->{$method}();
                self::fail('旧消费者不应继续拥有处理权。');
            } catch (LeaseLost) {
                self::assertSame(1, $this->queue->size());
            }
        }
        $new->delete();
        self::assertSame(0, $this->queue->size());
    }

    /**
     * 验证 Worker 被 SIGKILL 后，待确认任务能够被新进程恢复。
     *
     * @return void
     */
    public function testKilledConsumerRecovery(): void
    {
        $id = $this->queue->push(SuccessJob::class);
        [$process, $pipes] = $this->startProcess('claim');
        try {
            stream_set_timeout($pipes[1], 5);
            self::assertSame($id, trim((string) fgets($pipes[1])));
        } finally {
            proc_terminate($process, 9);
            $this->finishProcess($process, $pipes);
        }
        usleep(100000);
        $recovered = $this->newQueue(['retry_after_ms' => 50])->pop();
        self::assertSame($id, $recovered->getJobId());
        self::assertSame(2, $recovered->attempts());
        $recovered->delete();
    }

    /**
     * 验证任务超时真正结束进程并保留待恢复任务，超过次数后转入失败队列。
     *
     * @return void
     */
    public function testTimeoutPersistsPendingTask(): void
    {
        $counter = $this->config['prefix'] . ':counter';
        $this->queue->push(SlowJob::class, ['counter' => $counter]);
        [$process, $pipes] = $this->startProcess('timeout');
        self::assertSame(124, $this->finishProcess($process, $pipes));
        self::assertSame('1', $this->redis->get($counter));
        usleep(1100000);
        self::assertTrue((new Worker($this->newQueue()))->runNextJob(null, 1, 1, 0));
        self::assertSame('1', $this->redis->get($counter));
        self::assertCount(1, $this->queue->transport()->failures('default'));
    }

    /**
     * 验证多个真实 PHP 进程竞争消费时，每个即时任务只被执行一次。
     *
     * @return void
     */
    public function testConcurrentConsumers(): void
    {
        $counter = $this->config['prefix'] . ':counter';
        for ($index = 0; $index < 30; $index++) {
            $this->queue->push(SuccessJob::class, ['counter' => $counter]);
        }
        $processes = [];
        for ($index = 0; $index < 3; $index++) {
            $processes[] = $this->startProcess('drain');
        }
        $deadline = microtime(true) + 5;
        while ($this->redis->get($this->config['prefix'] . ':ready') !== '3' && microtime(true) < $deadline) {
            usleep(1000);
        }
        $ready = $this->redis->get($this->config['prefix'] . ':ready');
        $this->redis->set($this->config['prefix'] . ':start', '1');
        foreach ($processes as [$process, $pipes]) {
            self::assertSame(0, $this->finishProcess($process, $pipes));
        }
        self::assertSame('3', $ready);
        self::assertGreaterThanOrEqual(2, $this->redis->hLen($this->config['prefix'] . ':consumers'));
        self::assertSame('30', $this->redis->get($counter));
        self::assertSame(0, $this->queue->size());
    }

    /**
     * 验证确认前连接断开不会移除任务。
     *
     * @return void
     */
    public function testDisconnectBeforeAcknowledgement(): void
    {
        $this->queue->push(SuccessJob::class);
        [$proxy, $pipes] = $this->startProcess('proxy');
        try {
            stream_set_timeout($pipes[1], 5);
            $port = (int) trim((string) fgets($pipes[1]));
            self::assertGreaterThan(0, $port);
            $connection = RedisConnection::connect(array_replace($this->config, ['host' => '127.0.0.1', 'port' => $port]));
            $queue = new Queue(new Transport($connection, $this->config), $this->app);
            $job = $queue->pop();
        } finally {
            proc_terminate($proxy, 9);
            $this->finishProcess($proxy, $pipes);
        }
        try {
            $job->delete();
            self::fail('连接断开时不应确认成功。');
        } catch (\RedisException) {
            self::assertSame(1, $this->queue->size());
        }
        usleep(100000);
        self::assertNotNull($this->newQueue(['retry_after_ms' => 50])->pop());
    }

    /**
     * 验证错误键类型在 Lua 写入前被发现。
     *
     * @return void
     */
    public function testLuaTypePreflightPreventsPartialWrite(): void
    {
        $keys = $this->queue->transport()->keys('default');
        $this->redis->set($keys[1], '错误类型');
        try {
            $this->queue->push(SuccessJob::class);
            self::fail('错误键类型应阻止入队。');
        } catch (\RedisException) {
            self::assertFalse($this->redis->exists($keys[2]) > 0);
            self::assertFalse($this->redis->exists($keys[0]) > 0);
        }
    }

    /**
     * 验证负数延迟在任何 Redis 写入之前被拒绝。
     *
     * @return void
     */
    public function testNegativeDelayIsRejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->queue->later(-1, SuccessJob::class);
    }

    /**
     * 验证不支持 JSON 的参数不会留下半条消息。
     *
     * @return void
     */
    public function testInvalidJsonIsRejectedBeforeWrite(): void
    {
        $data = [];
        $data['self'] = &$data;
        try {
            $this->queue->push(SuccessJob::class, $data);
            self::fail('循环参数应拒绝入队。');
        } catch (\JsonException) {
            self::assertSame(0, $this->queue->size());
        }
    }

    /**
     * 验证可见性超时不允许短于任务执行超时。
     *
     * @return void
     */
    public function testUnsafeWorkerTimeoutIsRejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        (new Worker($this->queue))->runNextJob(null, 3, 2, 0);
    }

    /**
     * 验证同一活动任务 ID 的重复发布只保留一条任务。
     *
     * @return void
     */
    public function testActiveJobIdDeduplication(): void
    {
        $options = ['id' => 'business-123'];
        $this->queue->push(SuccessJob::class, [], null, $options);
        $this->queue->push(SuccessJob::class, [], null, $options);
        self::assertSame(1, $this->queue->size());
    }

    /**
     * 验证服务注册、Facade 和命令解析，覆盖字符串零延迟。
     *
     * @return void
     */
    public function testThinkPhpServiceFacadeAndCommands(): void
    {
        [$process, $pipes] = $this->startProcess('commands');
        $output = stream_get_contents($pipes[1]);
        self::assertSame(0, $this->finishProcess($process, $pipes));
        $result = json_decode($output, true, 512, JSON_THROW_ON_ERROR);
        self::assertStringContainsString('领取任务数：1', $result['work_output']);
        self::assertSame('1', $result['record']['attempts']);
        self::assertSame(0, $result['failures_after_retry']);
        self::assertNotSame('', $result['restart_token']);
        self::assertSame(1, $result['stats']['waiting']);
        self::assertTrue($result['stats']['counts_consistent']);
        self::assertTrue($result['prune']['dry_run']);
        self::assertSame('invalid-json', $result['quarantined']['payload']);
        self::assertSame(0, $result['quarantined_after_forget']);
        self::assertStringContainsString('已清理', $result['cleanup']);
        self::assertFalse($result['pruned']['dry_run']);
        self::assertSame(['1-0'], $result['pruned']['ids']);
    }

    /**
     * 创建独立消费者实例。
     *
     * @param array<string, mixed> $overrides 覆盖配置
     * @return Queue 队列入口
     */
    private function newQueue(array $overrides = []): Queue
    {
        $config = array_replace($this->config, $overrides);
        return new Queue(new Transport(RedisConnection::connect($config), $config), $this->app);
    }

    /**
     * 启动真实子进程，使用参数数组防止 shell 转义问题。
     *
     * @param string $mode 进程运行模式
     * @return array{resource, array<int, resource>} 进程和管道
     */
    private function startProcess(string $mode): array
    {
        $process = proc_open([PHP_BINARY, __DIR__ . '/Fixtures/process.php', $mode, $this->config['prefix']],
            [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
        self::assertIsResource($process);
        fclose($pipes[0]);
        unset($pipes[0]);
        return [$process, $pipes];
    }

    /**
     * 回收测试进程，输出错误以便定位失败。
     *
     * @param resource $process 进程
     * @param array<int, resource> $pipes 输出管道
     * @return int 退出码
     */
    private function finishProcess($process, array $pipes): int
    {
        $error = stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);
        $status = proc_close($process);
        self::assertSame('', $error);
        return $status;
    }
}
