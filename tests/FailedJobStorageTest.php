<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Tests;

use PDO;
use PHPUnit\Framework\TestCase;
use Redis;
use think\App;
use ThinkQueue\Streams\Exception\FailureStorageException;
use ThinkQueue\Streams\Exception\LeaseLost;
use ThinkQueue\Streams\Exception\PayloadTooLarge;
use ThinkQueue\Streams\FailedJobStore;
use ThinkQueue\Streams\Queue;
use ThinkQueue\Streams\RedisConnection;
use ThinkQueue\Streams\Service;
use ThinkQueue\Streams\Tests\Fixtures\FailureDatabase;
use ThinkQueue\Streams\Tests\Fixtures\SuccessJob;
use ThinkQueue\Streams\Tests\Fixtures\ThrowingJob;
use ThinkQueue\Streams\Transport;
use ThinkQueue\Streams\Worker;

final class FailedJobStorageTest extends TestCase
{
    private Redis $redis;
    private array $config;
    private FailedJobStore $store;
    private Queue $queue;

    protected function setUp(): void
    {
        $this->config = ['host' => getenv('REDIS_HOST') ?: '127.0.0.1', 'port' => (int) (getenv('REDIS_PORT') ?: 6379),
            'prefix' => 'failed-db-' . bin2hex(random_bytes(8)), 'block_ms' => 0,
            'retry_after_ms' => 2000, 'claim_interval_ms' => 1];
        $this->redis = RedisConnection::connect($this->config);
        $this->store = FailureDatabase::store($this->config['prefix']);
        $this->queue = $this->newQueue();
    }

    protected function tearDown(): void
    {
        foreach ($this->redis->keys('*' . $this->config['prefix'] . '*') as $key) $this->redis->del($key);
        FailureDatabase::cleanup($this->config['prefix']);
    }

    public function testFailuresLiveInDatabaseAndDoNotAccumulateRedisHistory(): void
    {
        for ($i = 0; $i < 25; $i++) {
            $id = $this->queue->push(SuccessJob::class, ['report_id' => $i, 'body' => str_repeat('正文', 1000)]);
            $failure = $this->queue->pop()->fail(new \RuntimeException("最终错误\xff"));
        }
        $rows = $this->queue->transport()->failures('default');
        self::assertCount(25, $rows);
        self::assertSame($id, $rows[$failure]['job_id']);
        self::assertSame('1', $rows[$failure]['attempts']);
        self::assertSame("RuntimeException: 最终错误\xff", $rows[$failure]['error']);
        self::assertSame(24, json_decode($rows[$failure]['payload'], true)['data']['report_id']);
        self::assertGreaterThan(0, (int) $rows[$failure]['failed_at_ms']);
        self::assertSame(25, $this->store->count($this->config['prefix'], 'default'));
        self::assertSame(0, $this->redis->xLen($this->queue->transport()->keys('default')[5]));
        self::assertSame(0, $this->queue->size());
        self::assertSame(0, $this->queue->transport()->stats('default')['failed_pending']);
    }

    public function testDatabaseOutagePreservesOutboxAndStopsConsumptionUntilRecovery(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $brokenStore = new FailedJobStore($pdo); // 故意不创建表，模拟数据库不可写。
        $queue = $this->newQueue([], $brokenStore);
        $counter = $this->config['prefix'] . ':counter';
        $id = $queue->push(ThrowingJob::class, ['counter' => $counter]);
        $next = $queue->push(SuccessJob::class);
        try {
            (new Worker($queue))->runNextJob(null, 1, 1, 0);
            self::fail('写库失败必须抛出异常让消费者退出。');
        } catch (FailureStorageException $exception) {
            self::assertInstanceOf(\PDOException::class, $exception->getPrevious());
        }
        $key = $queue->transport()->keys('default')[5];
        $outbox = $this->redis->xRange($key, '-', '+');
        self::assertCount(1, $outbox);
        self::assertSame($id, array_values($outbox)[0]['job_id']);
        self::assertSame(1, $queue->size());
        self::assertFalse($this->redis->get($counter . ':failed'));
        try {
            $queue->pop();
            self::fail('待落库记录未恢复前不能继续领取新任务。');
        } catch (\PDOException) {
            self::assertSame(1, $queue->size());
        }
        $brokenStore->install();
        self::assertSame($next, $queue->pop()->getJobId());
        self::assertSame(0, $this->redis->xLen($key));
        self::assertSame(1, $brokenStore->count($this->config['prefix'], 'default'));
        self::assertSame('1', $this->redis->get($counter));
    }

    public function testMigrationIsBoundedIdempotentAndDoesNotResurrectDeletedRecords(): void
    {
        $transport = $this->queue->transport();
        $key = $transport->keys('default')[5];
        $record = ['job_id' => 'old-job', 'payload' => '{}', 'attempts' => '3', 'error' => 'old', 'failed_at_ms' => '1'];
        foreach (['1-0', '2-0', '10-0'] as $id) $this->redis->xAdd($key, $id, $record);
        // 模拟数据库提交成功、Redis XDEL 前进程退出。
        $this->store->archive($this->config['prefix'], 'default', '1-0', $record, static fn (): bool => true);
        self::assertSame(1, $transport->migrateFailed('default', 1));
        self::assertSame(1, $this->store->count($this->config['prefix'], 'default'));
        self::assertSame(2, $this->redis->xLen($key));
        self::assertSame(2, $transport->migrateFailed('default', 100));
        self::assertSame(0, $transport->migrateFailed('default'));
        self::assertSame(3, $this->store->count($this->config['prefix'], 'default'));
        self::assertTrue($transport->forgetFailed('default', '1-0'));
        // 模拟另一个迁移进程早已读到记录、随后才拿到数据库行锁。
        $this->store->archive($this->config['prefix'], 'default', '1-0', $record, static fn (): bool => false);
        self::assertFalse($this->store->exists($this->config['prefix'], 'default', '1-0'));
    }

    public function testDatabasePagingUsesNumericStreamOrderAndIsolatesQueuesAndPrefixes(): void
    {
        $transport = $this->queue->transport();
        foreach (['2-0', '10-0', '10-2', '10-10'] as $id) {
            $this->redis->xAdd($transport->keys('default')[5], $id, ['error' => 'old']);
        }
        self::assertSame(['2-0', '10-0'], array_keys($transport->failures('default', 2)));
        self::assertSame(['10-2', '10-10'], array_keys($transport->failures('default', 2, '(10-0')));
        self::assertSame(['10-0'], array_keys($transport->failures('default', 1, '10-0')));
        self::assertSame([], $transport->failures('other'));
        self::assertSame([], $this->newQueue(['prefix' => $this->config['prefix'] . '-other'])->transport()->failures('default'));
    }

    public function testReplayRechecksByteLimitAndPreservesRejectedRecord(): void
    {
        $id = $this->queue->push(SuccessJob::class, ['data' => str_repeat('x', 1000)]);
        $failure = $this->queue->pop()->fail(new \RuntimeException('failed'));
        try {
            $this->newQueue(['max_payload_bytes' => 500])->transport()->retryFailed('default', $failure);
            self::fail('超限重放必须被拒绝。');
        } catch (PayloadTooLarge) {
            self::assertTrue($this->store->exists($this->config['prefix'], 'default', $failure));
            self::assertSame(0, $this->queue->size());
        }
        self::assertSame($id, $this->queue->transport()->retryFailed('default', $failure));
        self::assertSame(1, $this->queue->pop()->attempts());
    }

    public function testStaleConsumerCannotCreateDatabaseFailure(): void
    {
        $this->queue->push(SuccessJob::class);
        $old = $this->queue->pop();
        usleep(60000);
        $current = $this->newQueue(['retry_after_ms' => 50])->pop();
        try {
            $old->fail(new \RuntimeException('stale'));
            self::fail('旧租约不能结算失败。');
        } catch (LeaseLost) {
            self::assertSame(0, $this->store->count($this->config['prefix'], 'default'));
        }
        $current->delete();
    }

    public function testReplayResponseLossCanRecoverEvenAfterReplayedTaskCompletes(): void
    {
        $id = $this->queue->push(SuccessJob::class);
        $failure = $this->queue->pop()->fail(new \RuntimeException('failed'));
        [$process, $pipes] = $this->startProcess('drop-replay-response');
        $port = (int) trim((string) fgets($pipes[1]));
        try {
            $this->newQueue(['host' => '127.0.0.1', 'port' => $port])->transport()->retryFailed('default', $failure);
            self::fail('丢失响应必须传播连接异常。');
        } catch (\RedisException) {
            self::assertSame('replay-applied', trim((string) fgets($pipes[1])));
            self::assertTrue($this->store->exists($this->config['prefix'], 'default', $failure));
        } finally {
            $this->finishProcess($process, $pipes);
        }
        $job = $this->queue->pop();
        self::assertSame($id, $job->getJobId());
        $job->delete();
        // 重放已实际成功且任务已完成，回执仍能阻止再次发布。
        self::assertSame($id, $this->queue->transport()->retryFailed('default', $failure));
        self::assertNull($this->queue->pop());
        self::assertSame(0, $this->store->count($this->config['prefix'], 'default'));
        self::assertSame(0, $this->redis->hLen($this->queue->transport()->keys('default')[8]));
    }

    public function testConcurrentReplayPublishesOnlyOnce(): void
    {
        $id = $this->queue->push(SuccessJob::class);
        $failure = $this->queue->pop()->fail(new \RuntimeException('failed'));
        $children = [];
        for ($i = 0; $i < 4; $i++) $children[] = $this->startProcess('retry-failure', $failure);
        $this->releaseProcesses();
        $results = [];
        foreach ($children as [$process, $pipes]) {
            $results[] = json_decode(stream_get_contents($pipes[1]), true, 512, JSON_THROW_ON_ERROR);
            $this->finishProcess($process, $pipes);
        }
        self::assertCount(1, array_filter($results, static fn ($result): bool => $result === $id));
        self::assertSame(1, $this->queue->size());
        self::assertSame(0, $this->store->count($this->config['prefix'], 'default'));
    }

    public function testConcurrentMigrationKeepsOneDatabaseRowPerFailure(): void
    {
        $key = $this->queue->transport()->keys('default')[5];
        for ($i = 1; $i <= 20; $i++) $this->redis->xAdd($key, $i . '-0', ['error' => 'old']);
        $children = [];
        for ($i = 0; $i < 4; $i++) $children[] = $this->startProcess('migrate-failures');
        $this->releaseProcesses();
        foreach ($children as [$process, $pipes]) {
            stream_get_contents($pipes[1]);
            $this->finishProcess($process, $pipes);
        }
        self::assertSame(20, $this->store->count($this->config['prefix'], 'default'));
        self::assertSame(0, $this->redis->xLen($key));
    }

    public function testOrphanReplayReceiptsAreReclaimedDuringConsumption(): void
    {
        $key = $this->queue->transport()->keys('default')[8];
        $this->redis->hSet($key, '1-0', 'completed-job');
        self::assertNull($this->queue->pop());
        self::assertSame(0, $this->redis->hLen($key));
    }

    public function testServiceUsesConfiguredConnectionAndCommitsOutsideBusinessTransaction(): void
    {
        $app = new App();
        $app->config->set(['default' => 'file', 'stores' => ['file' => ['type' => 'File',
            'path' => sys_get_temp_dir() . '/' . $this->config['prefix'] . '/cache/']]], 'cache');
        $app->config->set(['default' => 'file', 'channels' => ['file' => ['type' => 'File',
            'path' => sys_get_temp_dir() . '/' . $this->config['prefix'] . '/log/']]], 'log');
        $config = FailureDatabase::configuration($this->config['prefix']);
        $config['default'] = 'missing-default';
        $app->config->set($config, 'database');
        $app->config->set($this->config + ['failed_connection' => 'queue_test'], 'queue_streams');
        (new Service($app))->register();
        $business = $app->db->connect('queue_test');
        $business->startTrans();
        try {
            $queue = $app->make(Queue::class);
            $queue->push(SuccessJob::class);
            $queue->pop()->fail(new \RuntimeException('failed'));
        } finally {
            $business->rollback();
        }
        self::assertSame(1, $this->store->count($this->config['prefix'], 'default'));
    }

    public function testCustomTableAndInvalidIdentifier(): void
    {
        $pdo = FailureDatabase::pdo($this->config['prefix']);
        $table = 'queue_failed_' . bin2hex(random_bytes(4));
        $store = new FailedJobStore($pdo, $table);
        try {
            $store->install();
            $queue = $this->newQueue([], $store);
            $queue->push(SuccessJob::class);
            $queue->pop()->fail(new \RuntimeException('failed'));
            self::assertSame(1, $store->count($this->config['prefix'], 'default'));
            self::assertSame(0, $this->store->count($this->config['prefix'], 'default'));
        } finally {
            $pdo->exec('DROP TABLE ' . $table);
        }
        $this->expectException(\InvalidArgumentException::class);
        new FailedJobStore($pdo, 'failed_jobs; DROP TABLE failed_jobs');
    }

    private function newQueue(array $overrides = [], ?FailedJobStore $store = null): Queue
    {
        $config = array_replace($this->config, $overrides);
        $app = new App();
        $redis = RedisConnection::connect($config);
        $app->instance(Redis::class, $redis);
        return new Queue(new Transport($redis, $config, $store ?? $this->store), $app);
    }

    private function startProcess(string $mode, string $id = ''): array
    {
        $process = proc_open([PHP_BINARY, __DIR__ . '/Fixtures/process.php', $mode, $this->config['prefix'], $id],
            [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
        self::assertIsResource($process);
        fclose($pipes[0]);
        unset($pipes[0]);
        stream_set_timeout($pipes[1], 10);
        return [$process, $pipes];
    }

    private function releaseProcesses(): void
    {
        $deadline = microtime(true) + 5;
        while ($this->redis->get($this->config['prefix'] . ':ready') !== '4' && microtime(true) < $deadline) usleep(1000);
        $ready = $this->redis->get($this->config['prefix'] . ':ready');
        $this->redis->set($this->config['prefix'] . ':start', '1');
        self::assertSame('4', $ready);
    }

    private function finishProcess($process, array $pipes): void
    {
        $error = stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);
        $status = proc_close($process);
        self::assertSame('', $error);
        self::assertSame(0, $status);
    }
}
