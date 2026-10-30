<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Tests;

use PHPUnit\Framework\TestCase;
use Redis;
use think\App;
use ThinkQueue\Streams\Exception\PayloadTooLarge;
use ThinkQueue\Streams\Exception\JobIdConflict;
use ThinkQueue\Streams\Exception\QueueFull;
use ThinkQueue\Streams\Queue;
use ThinkQueue\Streams\RedisConnection;
use ThinkQueue\Streams\Transport;
use ThinkQueue\Streams\Tests\Fixtures\FailureDatabase;
use ThinkQueue\Streams\Worker;
use ThinkQueue\Streams\Tests\Fixtures\SuccessJob;
use ThinkQueue\Streams\Tests\Fixtures\ThrowingJob;

final class HardeningTest extends TestCase
{
    private Redis $redis;
    private array $config;
    private Queue $queue;

    protected function setUp(): void
    {
        $this->config = ['host' => getenv('REDIS_HOST') ?: '127.0.0.1',
            'port' => (int) (getenv('REDIS_PORT') ?: 6379), 'prefix' => 'hardening-' . bin2hex(random_bytes(8)),
            'block_ms' => 0, 'retry_after_ms' => 2000, 'claim_interval_ms' => 1];
        $this->redis = RedisConnection::connect($this->config);
        $this->queue = $this->newQueue();
    }

    protected function tearDown(): void
    {
        FailureDatabase::cleanup($this->config['prefix']);
        foreach ($this->redis->keys('*' . $this->config['prefix'] . '*') as $key) {
            $this->redis->del($key);
        }
    }

    public function testMalformedJsonIsQuarantinedAndNextJobRuns(): void
    {
        $bad = $this->queue->push(SuccessJob::class);
        $keys = $this->queue->transport()->keys('default');
        $this->redis->hSet($keys[2], $bad, "{broken\xff");
        $counter = $this->config['prefix'] . ':counter';
        $this->queue->push(SuccessJob::class, ['counter' => $counter]);
        self::assertTrue((new Worker($this->queue))->runNextJob(null, 3, 1, 0));
        self::assertSame('1', $this->redis->get($counter));
        $record = array_values($this->queue->transport()->quarantined('default'))[0];
        self::assertSame("{broken\xff", $record['payload']);
        self::assertSame($bad, $record['job_id']);
        self::assertSame(0, $this->queue->size());
    }

    public function testInvalidVersionsFieldsAndTypesAreQuarantined(): void
    {
        $mutations = [
            fn (array $p) => array_replace($p, ['version' => 2]),
            fn (array $p) => array_replace($p, ['timeout' => '1']),
            fn (array $p) => array_replace($p, ['max_tries' => 0]),
            fn (array $p) => array_replace($p, ['id' => 'different-id']),
            fn (array $p) => array_diff_key($p, ['data' => true]),
            fn (array $p) => array_replace($p, ['job' => ['invalid']]),
            fn (array $p) => array_replace($p, ['created_at' => -1]),
        ];
        $keys = $this->queue->transport()->keys('default');
        foreach ($mutations as $mutate) {
            $id = $this->queue->push(SuccessJob::class);
            $payload = json_decode($this->redis->hGet($keys[2], $id), true, 512, JSON_THROW_ON_ERROR);
            $this->redis->hSet($keys[2], $id, json_encode($mutate($payload), JSON_THROW_ON_ERROR));
            self::assertNull($this->queue->pop());
            self::assertSame(0, $this->queue->size());
        }
        self::assertCount(count($mutations), $this->queue->transport()->quarantined('default'));
    }

    public function testMissingStreamIdAndPayloadDoNotBlockValidJobs(): void
    {
        $keys = $this->queue->transport()->keys('default');
        $this->redis->xAdd($keys[0], '*', ['unrecognized' => 'field']);
        $bad = $this->queue->push(SuccessJob::class);
        $this->redis->hDel($keys[2], $bad);
        $good = $this->queue->push(SuccessJob::class);
        self::assertSame($good, $this->queue->pop()->getJobId());
        self::assertCount(2, $this->queue->transport()->quarantined('default'));
        self::assertSame(1, $this->queue->size());
        self::assertSame(1, $this->redis->hLen($keys[2]));
    }

    public function testOrphanDelayedEntryIsIsolatedAndOtherDueJobPromotes(): void
    {
        $bad = $this->queue->later(1, SuccessJob::class);
        $good = $this->queue->later(1, SuccessJob::class);
        $keys = $this->queue->transport()->keys('default');
        $this->redis->hDel($keys[2], $bad);
        foreach ([$bad, $good] as $id) {
            $this->redis->zAdd($keys[1], (time() - 1) * 1000, $id);
        }
        self::assertSame(1, $this->queue->transport()->promote('default'));
        self::assertSame($good, $this->queue->pop()->getJobId());
        $records = $this->queue->transport()->quarantined('default');
        self::assertCount(1, $records);
        self::assertSame('delayed', array_values($records)[0]['source']);
        self::assertSame(0, $this->redis->zCard($keys[1]));
    }

    public function testCapacityIncludesPendingAndDelayedAndAllowsDedupAndRelease(): void
    {
        $queue = $this->newQueue(['max_active_jobs' => 2, 'max_payload_bytes' => 1000]);
        $id = $queue->push(SuccessJob::class, [], null, ['id' => 'same-id']);
        $queue->later(60, SuccessJob::class);
        $pending = $queue->pop();
        self::assertSame($id, $queue->push(SuccessJob::class, [], null, ['id' => $id]));
        try {
            $queue->push(SuccessJob::class, str_repeat('x', 2000), null, ['id' => $id]);
            self::fail('相同 ID 的不同任务不能作为成功去重。');
        } catch (JobIdConflict) {
            self::assertSame(2, $queue->size());
        }
        try {
            $queue->push(SuccessJob::class);
            self::fail('满队列必须拒绝新任务。');
        } catch (QueueFull) {
            self::assertSame(2, $queue->size());
        }
        $pending->release();
        $queue->pop()->delete();
        $queue->push(SuccessJob::class);
        self::assertSame(2, $queue->size());
    }

    public function testOversizedPayloadDoesNotLeavePartialState(): void
    {
        $queue = $this->newQueue(['max_payload_bytes' => 100]);
        try {
            $queue->push(SuccessJob::class, str_repeat('x', 200));
            self::fail('超大消息必须拒绝。');
        } catch (PayloadTooLarge) {
            self::assertSame(0, $queue->size());
            self::assertSame(0, $this->redis->hLen($queue->transport()->keys('default')[2]));
        }
    }

    public function testOversizedExistingMessageIsQuarantinedOnConsumption(): void
    {
        $id = $this->queue->push(SuccessJob::class, str_repeat('x', 2000));
        $limited = $this->newQueue(['max_payload_bytes' => 1000]);
        self::assertNull($limited->pop());
        $records = $limited->transport()->quarantined('default');
        self::assertSame($id, array_values($records)[0]['job_id']);
        self::assertSame(0, $limited->size());
    }

    public function testReplayRespectsCapacityAndPreservesFailureOnRejection(): void
    {
        $queue = $this->newQueue(['max_active_jobs' => 1]);
        $id = $queue->push(SuccessJob::class);
        $failure = $queue->pop()->fail(new \RuntimeException('failure'));
        $queue->push(SuccessJob::class);
        try {
            $queue->transport()->retryFailed('default', $failure);
            self::fail('满队列必须拒绝重放。');
        } catch (QueueFull) {
            self::assertCount(1, $queue->transport()->failures('default'));
        }
        $queue->pop()->delete();
        self::assertSame($id, $queue->transport()->retryFailed('default', $failure));
    }

    public function testConcurrentProducersCannotExceedCapacity(): void
    {
        $children = [];
        for ($i = 0; $i < 4; $i++) {
            $children[] = $this->startProcess('produce');
        }
        $deadline = microtime(true) + 5;
        while ($this->redis->get($this->config['prefix'] . ':ready') !== '4' && microtime(true) < $deadline) {
            usleep(1000);
        }
        $ready = $this->redis->get($this->config['prefix'] . ':ready');
        $this->redis->set($this->config['prefix'] . ':start', '1');
        foreach ($children as [$process, $pipes]) {
            self::assertSame(0, $this->finishProcess($process, $pipes));
        }
        self::assertSame('4', $ready);
        self::assertSame('7', $this->redis->get($this->config['prefix'] . ':accepted'));
        self::assertSame('33', $this->redis->get($this->config['prefix'] . ':rejected'));
        self::assertSame(7, $this->queue->size());
    }

    public function testStatsDistinguishWaitingPendingDelayedAndHistory(): void
    {
        $transport = $this->queue->transport();
        self::assertSame(0, $transport->stats('default')['active']);
        self::assertFalse((bool) $this->redis->exists($transport->keys('default')[0]));
        $this->queue->push(SuccessJob::class);
        $this->queue->pop()->fail(new \RuntimeException('failed'));
        $bad = $this->queue->push(SuccessJob::class);
        $this->redis->hSet($transport->keys('default')[2], $bad, 'invalid-json');
        self::assertNull($this->queue->pop());
        $this->queue->push(SuccessJob::class);
        $this->queue->pop();
        $this->queue->push(SuccessJob::class);
        $this->queue->later(60, SuccessJob::class);
        $stats = $transport->stats('default', 1);
        foreach (['active' => 3, 'waiting' => 1, 'pending' => 1, 'delayed' => 1, 'failed' => 1, 'quarantined' => 1] as $name => $value) {
            self::assertSame($value, $stats[$name], $name);
        }
        self::assertTrue($stats['counts_consistent']);
        self::assertSame(1, $stats['first_pending']['deliveries']);
        self::assertCount(1, $stats['consumers']);
        self::assertNotNull($stats['next_delayed_at_ms']);
        self::assertSame([], $transport->stats('default', 1, 1)['consumers']);
    }

    public function testConsumerCleanupProtectsPendingAndFreshHeartbeats(): void
    {
        $transport = $this->queue->transport();
        $this->queue->push(SuccessJob::class);
        $pending = $this->queue->pop();
        $consumer = $transport->stats('default')['consumers'][0]['name'];
        self::assertFalse($transport->cleanupConsumer('default', $consumer, 20000));
        $this->redis->zAdd($transport->keys('default')[7], (time() - 100) * 1000, $consumer);
        self::assertFalse($transport->cleanupConsumer('default', $consumer, 20000));
        $pending->delete();
        self::assertTrue($transport->cleanupConsumer('default', $consumer, 20000));
        self::assertSame(0, $transport->stats('default')['consumer_count']);
    }

    public function testPruneIsBoundedAndDryRunPreservesRecords(): void
    {
        $transport = $this->queue->transport();
        $key = $transport->keys('default')[5];
        foreach (['1-0', '2-0', time() * 1000 . '-0'] as $id) {
            $this->redis->xAdd($key, $id, ['error' => 'old']);
        }
        self::assertSame(['1-0'], $transport->prune('default', 'failed', 60, 1));
        self::assertCount(3, $transport->failures('default'));
        self::assertSame(['1-0'], $transport->prune('default', 'failed', 60, 1, false));
        self::assertSame(['2-0'], $transport->prune('default', 'failed', 60, 100, false));
        self::assertCount(1, $transport->failures('default'));
    }

    public function testScriptCacheFlushDoesNotLoseOrDuplicateTransitions(): void
    {
        $this->redis->script('FLUSH');
        $id = $this->queue->push(SuccessJob::class);
        $this->redis->script('FLUSH');
        $job = $this->queue->pop();
        self::assertSame($id, $job->getJobId());
        $this->redis->script('FLUSH');
        $job->release();
        $job = $this->queue->pop();
        self::assertSame(2, $job->attempts());
        $this->redis->script('FLUSH');
        $job->delete();
        self::assertSame(0, $this->queue->size());
    }

    public function testRetryJitterAndOptOutRespectDelayBounds(): void
    {
        foreach ([true, false] as $jitter) {
            $queue = $this->newQueue(['retry_jitter' => $jitter]);
            $id = $queue->push(ThrowingJob::class, ['counter' => $this->config['prefix'] . ':counter']);
            $before = (int) floor(microtime(true) * 1000);
            self::assertTrue((new Worker($queue))->runNextJob(null, 3, 1, 2));
            $score = (int) $this->redis->zScore($queue->transport()->keys('default')[1], $id);
            $after = (int) floor(microtime(true) * 1000);
            self::assertGreaterThanOrEqual($before + ($jitter ? 1000 : 2000), $score);
            self::assertLessThanOrEqual($after + 2000, $score);
        }
    }

    public function testBlockingReadWakesOnNewTask(): void
    {
        $queue = $this->newQueue(['block_ms' => 1000, 'claim_interval_ms' => 1000]);
        [$process, $pipes] = $this->startProcess('wake');
        $start = microtime(true);
        $job = $queue->pop();
        self::assertSame(0, $this->finishProcess($process, $pipes));
        self::assertNotNull($job);
        self::assertLessThan(1.0, microtime(true) - $start);
        $job->delete();
    }

    public function testKnownDelayedDeadlineShortensBlockingRead(): void
    {
        $queue = $this->newQueue(['block_ms' => 4000, 'read_timeout' => 6.0,
            'claim_interval_ms' => 10000, 'promotion_interval_ms' => 10000]);
        $queue->later(1, SuccessJob::class);
        $start = microtime(true);
        self::assertNull($queue->pop());
        self::assertNotNull($queue->pop());
        self::assertLessThan(2.0, microtime(true) - $start);
    }

    public function testRestartPollingIsThrottledAndEventuallyObservesChanges(): void
    {
        $transport = $this->newQueue(['restart_interval_ms' => 60000])->transport();
        $before = $this->getCalls();
        for ($i = 0; $i < 100; $i++) {
            $transport->restartToken();
        }
        $after = $this->getCalls();
        self::assertSame(1, $after - $before);
        $fast = $this->newQueue(['restart_interval_ms' => 1])->transport();
        $first = $fast->restartToken();
        $next = $this->queue->transport()->restartToken(true);
        self::assertNotSame($first, $next);
        usleep(5000);
        self::assertSame($next, $fast->restartToken());
    }

    public function testAckResponseLossDoesNotReplayCompletedTask(): void
    {
        $this->queue->push(SuccessJob::class);
        [$process, $pipes] = $this->startProcess('drop-ack-response');
        $port = (int) trim((string) fgets($pipes[1]));
        $queue = $this->newQueue(['host' => '127.0.0.1', 'port' => $port]);
        $job = $queue->pop();
        try {
            $job->delete();
            self::fail('响应丢失时调用方应收到连接异常。');
        } catch (\RedisException) {
            self::assertSame('ack-applied', trim((string) fgets($pipes[1])));
            self::assertSame(0, $this->queue->size());
            self::assertNull($this->queue->pop());
        } finally {
            self::assertSame(0, $this->finishProcess($process, $pipes));
        }
    }

    public function testPasswordStringZeroAuthenticates(): void
    {
        [$process, $pipes] = $this->startProcess('auth-zero');
        $port = (int) trim((string) fgets($pipes[1]));
        try {
            $connection = RedisConnection::connect(['host' => '127.0.0.1', 'port' => $port, 'password' => '0']);
            self::assertSame('6.2.0', $connection->info('server')['redis_version']);
            $connection->close();
        } finally {
            self::assertSame(0, $this->finishProcess($process, $pipes));
        }
    }

    public function testAuthenticationFailureIsPropagated(): void
    {
        [$process, $pipes] = $this->startProcess('auth-zero');
        $port = (int) trim((string) fgets($pipes[1]));
        try {
            RedisConnection::connect(['host' => '127.0.0.1', 'port' => $port, 'password' => 'incorrect']);
            self::fail('认证失败应传播。');
        } catch (\RedisException $exception) {
            self::assertStringContainsString('WRONGPASS', $exception->getMessage());
        } finally {
            self::assertSame(0, $this->finishProcess($process, $pipes));
        }
    }

    public function testAuthAndSelectUseFiniteReadTimeout(): void
    {
        foreach (['stall-auth', 'stall-select'] as $mode) {
            [$process, $pipes] = $this->startProcess($mode);
            $port = (int) trim((string) fgets($pipes[1]));
            $start = microtime(true);
            try {
                $config = ['host' => '127.0.0.1', 'port' => $port, 'read_timeout' => 0.1];
                if ($mode === 'stall-auth') {
                    $config['password'] = 'test';
                }
                RedisConnection::connect($config);
                self::fail('初始化无响应时应超时退出。');
            } catch (\RedisException) {
                self::assertLessThan(1.0, microtime(true) - $start);
            } finally {
                proc_terminate($process, 9);
                $this->finishProcess($process, $pipes);
            }
        }
    }

    public function testInvalidTimeoutsAreRejectedBeforeConnecting(): void
    {
        foreach ([0, -1, INF, NAN, 'invalid'] as $timeout) {
            try {
                RedisConnection::connect(['read_timeout' => $timeout]);
                self::fail('非法超时应拒绝。');
            } catch (\InvalidArgumentException) {
                self::assertTrue(true);
            }
        }
    }

    public function testRedisErrorsContainingQueueNameAreNotMisclassified(): void
    {
        $queue = $this->newQueue(['prefix' => $this->config['prefix'] . '-QUEUE_FULL']);
        $this->redis->set($queue->transport()->keys('default')[1], 'wrong-type');
        try {
            $queue->push(SuccessJob::class);
            self::fail('键类型错误应传播。');
        } catch (\RedisException $exception) {
            self::assertStringContainsString('QUEUE_WRONGTYPE', $exception->getMessage());
        }
    }

    private function newQueue(array $overrides = []): Queue
    {
        $config = array_replace($this->config, $overrides);
        $app = new App();
        $redis = RedisConnection::connect($config);
        $app->instance(Redis::class, $redis);
        return new Queue(new Transport($redis, $config, FailureDatabase::store($this->config['prefix'])), $app);
    }

    private function getCalls(): int
    {
        $stats = $this->redis->info('commandstats')['cmdstat_get'] ?? '';
        preg_match('/(?:^|,)calls=(\d+)/', $stats, $matches);
        return (int) ($matches[1] ?? 0);
    }

    private function startProcess(string $mode): array
    {
        $process = proc_open([PHP_BINARY, __DIR__ . '/Fixtures/process.php', $mode, $this->config['prefix']],
            [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
        self::assertIsResource($process);
        fclose($pipes[0]);
        unset($pipes[0]);
        stream_set_timeout($pipes[1], 5);
        return [$process, $pipes];
    }

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
