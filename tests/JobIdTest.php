<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Tests;

use Closure;
use PHPUnit\Framework\TestCase;
use Redis;
use think\App;
use ThinkQueue\Streams\Exception\JobIdConflict;
use ThinkQueue\Streams\Exception\PayloadTooLarge;
use ThinkQueue\Streams\Exception\QueueFull;
use ThinkQueue\Streams\Queue;
use ThinkQueue\Streams\RedisConnection;
use ThinkQueue\Streams\Tests\Fixtures\SuccessJob;
use ThinkQueue\Streams\Tests\Fixtures\ThrowingJob;
use ThinkQueue\Streams\Transport;

final class JobIdTest extends TestCase
{
    private Redis $redis;
    private array $config;
    private Queue $queue;

    protected function setUp(): void
    {
        $this->config = ['host' => getenv('REDIS_HOST') ?: '127.0.0.1',
            'port' => (int) (getenv('REDIS_PORT') ?: 6379), 'prefix' => 'job-id-' . bin2hex(random_bytes(8)),
            'block_ms' => 0, 'retry_after_ms' => 2000, 'claim_interval_ms' => 1];
        $this->redis = RedisConnection::connect($this->config);
        $this->queue = $this->newQueue();
    }

    protected function tearDown(): void
    {
        foreach ($this->redis->keys('*' . $this->config['prefix'] . '*') as $key) {
            $this->redis->del($key);
        }
    }

    public function testDefaultIdUsesFullRandomPart(): void
    {
        $id = $this->queue->push(SuccessJob::class, ['report_id' => 10001]);
        self::assertMatchesRegularExpression('/\A[0-9a-f]{32}\z/', $id);
        $delayedId = $this->queue->later(60, SuccessJob::class);
        self::assertMatchesRegularExpression('/\A[0-9a-f]{32}\z/', $delayedId);
        self::assertNotSame($id, $delayedId);
        $job = $this->queue->pop();
        self::assertSame($id, $job->getJobId());
        self::assertSame(10001, $job->payload('data.report_id'));
    }

    public function testAutomaticCollisionNeverDeduplicatesAndRegeneratesUntilFree(): void
    {
        $occupied = str_repeat('a', 32);
        $available = str_repeat('b', 32);
        $this->queue->push(SuccessJob::class, [], null, ['id' => $occupied]);
        $keys = $this->queue->transport()->keys('default');
        $original = $this->redis->hGet($keys[2], $occupied);
        $attempts = 0;
        $queue = $this->newQueue(static function () use (&$attempts, $occupied, $available): string {
            return ++$attempts < 3 ? $occupied : $available;
        });
        self::assertSame($available, $queue->push(SuccessJob::class));
        self::assertSame(3, $attempts);
        self::assertSame(2, $queue->size());
        self::assertSame(2, $this->redis->xLen($keys[0]));
        self::assertSame($original, $this->redis->hGet($keys[2], $occupied));
    }

    public function testBusinessRerunCreatesNewJobWhileInternalRetryRetainsId(): void
    {
        $data = ['report_id' => 10001];
        $first = $this->queue->push(SuccessJob::class, $data);
        $second = $this->queue->push(SuccessJob::class, $data);
        self::assertNotSame($first, $second);
        self::assertSame(2, $this->queue->size());
        $job = $this->queue->pop();
        self::assertSame($first, $job->getJobId());
        $job->release();
        $other = $this->queue->pop();
        self::assertSame($second, $other->getJobId());
        $other->delete();
        $retry = $this->queue->pop();
        self::assertSame($first, $retry->getJobId());
        self::assertSame(2, $retry->attempts());
        self::assertSame($data, $retry->payload('data'));
    }

    public function testDelayedCollisionKeepsExistingPendingTaskAndCreatesNewDelay(): void
    {
        $this->queue->push(SuccessJob::class, ['original' => true], null, ['id' => 'occupied']);
        $pending = $this->queue->pop();
        $attempts = 0;
        $queue = $this->newQueue(static function () use (&$attempts): string {
            return ++$attempts === 1 ? 'occupied' : 'available';
        });
        self::assertSame('available', $queue->later(60, SuccessJob::class, ['original' => false]));
        self::assertSame(2, $attempts);
        $keys = $queue->transport()->keys('default');
        self::assertSame(1, $this->redis->xLen($keys[0]));
        self::assertSame(1, $this->redis->zCard($keys[1]));
        self::assertGreaterThan(time() * 1000, $this->redis->zScore($keys[1], 'available'));
        self::assertSame(true, $pending->payload('data.original'));
        $pending->delete();
        self::assertSame(1, $queue->size());
    }

    public function testRepeatedAutomaticCollisionsFailAfterFiveCandidates(): void
    {
        $this->queue->push(SuccessJob::class, [], null, ['id' => 'occupied']);
        $attempts = 0;
        $queue = $this->newQueue(static function () use (&$attempts): string {
            $attempts++;
            return 'occupied';
        });
        try {
            $queue->push(SuccessJob::class);
            self::fail('持续冲突必须有界失败。');
        } catch (JobIdConflict $exception) {
            self::assertStringContainsString('5', $exception->getMessage());
            self::assertInstanceOf(JobIdConflict::class, $exception->getPrevious());
            self::assertSame(5, $attempts);
            self::assertSame(1, $queue->size());
            self::assertSame(1, $this->redis->xLen($queue->transport()->keys('default')[0]));
        }
    }

    public function testCustomIdDeduplicatesEquivalentJsonAndKeepsOriginalSchedule(): void
    {
        $options = ['id' => 'same-task', 'timeout' => 1, 'max_tries' => 3];
        $data = ['object' => (object) ['b' => 2, 'a' => 1], 'list' => [1, 2]];
        $this->queue->later(60, SuccessJob::class, $data, null, $options);
        $keys = $this->queue->transport()->keys('default');
        $stored = json_decode($this->redis->hGet($keys[2], 'same-task'), false, 512, JSON_THROW_ON_ERROR);
        $stored->created_at -= 100;
        $this->redis->hSet($keys[2], 'same-task', json_encode($stored, JSON_THROW_ON_ERROR));
        $original = $this->redis->hGet($keys[2], 'same-task');
        $due = $this->redis->zScore($keys[1], 'same-task');
        $sameData = ['list' => [1, 2], 'object' => (object) ['a' => 1, 'b' => 2]];
        self::assertSame('same-task', $this->queue->push(SuccessJob::class, $sameData, null, $options));
        self::assertSame(1, $this->queue->size());
        self::assertSame(0, $this->redis->xLen($keys[0]));
        self::assertSame($due, $this->redis->zScore($keys[1], 'same-task'));
        self::assertSame($original, $this->redis->hGet($keys[2], 'same-task'));
    }

    public function testDifferentTaskWithSameCustomIdFailsWithoutOverwriting(): void
    {
        $data = ['report_id' => 10001, 'list' => [1, 2], 'object' => (object) []];
        $options = ['id' => 'same-task', 'max_tries' => 3, 'timeout' => 1];
        $this->queue->push(SuccessJob::class, $data, null, $options);
        $keys = $this->queue->transport()->keys('default');
        $original = $this->redis->hGet($keys[2], 'same-task');
        $cases = [
            [ThrowingJob::class, $data, $options],
            [SuccessJob::class, array_replace($data, ['report_id' => 10002]), $options],
            [SuccessJob::class, array_replace($data, ['report_id' => '10001']), $options],
            [SuccessJob::class, array_replace($data, ['list' => [2, 1]]), $options],
            [SuccessJob::class, array_replace($data, ['object' => []]), $options],
            [SuccessJob::class, $data, array_replace($options, ['max_tries' => 4])],
            [SuccessJob::class, $data, array_replace($options, ['timeout' => 2])],
        ];
        foreach ($cases as [$job, $parameters, $settings]) {
            try {
                $this->queue->push($job, $parameters, null, $settings);
                self::fail('不同任务不能被静默去重。');
            } catch (JobIdConflict) {
                self::assertSame(1, $this->queue->size());
                self::assertSame(1, $this->redis->xLen($keys[0]));
                self::assertSame($original, $this->redis->hGet($keys[2], 'same-task'));
            }
        }
    }

    public function testCorruptExistingPayloadCannotReportSuccessfulDeduplication(): void
    {
        $options = ['id' => 'same-task'];
        $this->queue->push(SuccessJob::class, [], null, $options);
        $key = $this->queue->transport()->keys('default')[2];
        $original = json_decode($this->redis->hGet($key, 'same-task'), true, 512, JSON_THROW_ON_ERROR);
        $corruptions = ['invalid-json', '{}',
            json_encode(array_replace($original, ['id' => 'another-task']), JSON_THROW_ON_ERROR),
            json_encode(array_replace($original, ['created_at' => -1]), JSON_THROW_ON_ERROR),
            json_encode(array_diff_key($original, ['created_at' => true]), JSON_THROW_ON_ERROR)];
        foreach ($corruptions as $corrupt) {
            $this->redis->hSet($key, 'same-task', $corrupt);
            try {
                $this->queue->push(SuccessJob::class, [], null, $options);
                self::fail('损坏消息不能作为同一任务去重。');
            } catch (JobIdConflict) {
                self::assertSame($corrupt, $this->redis->hGet($key, 'same-task'));
            }
        }
    }

    public function testOldIdsRemainUsableAcrossRetryAndFailureReplay(): void
    {
        $id = str_repeat('a', 32);
        self::assertSame($id, $this->queue->push(SuccessJob::class, [], null, ['id' => $id]));
        $job = $this->queue->pop();
        $message = $job->getMessageId();
        $job->release();
        $job = $this->queue->pop();
        self::assertSame($id, $job->getJobId());
        self::assertNotSame($message, $job->getMessageId());
        $failure = $job->fail(new \RuntimeException('failed'));
        self::assertSame($id, $this->queue->transport()->retryFailed('default', $failure));
        $job = $this->queue->pop();
        self::assertSame($id, $job->getJobId());
        $job->delete();
        self::assertSame($id, $this->queue->push(SuccessJob::class, ['new' => true], null, ['id' => $id]));
        self::assertSame(1, $this->queue->size());
    }

    public function testAdmissionErrorsDoNotRegenerateId(): void
    {
        foreach ([['max_active_jobs' => 1], ['max_payload_bytes' => 1]] as $limits) {
            $attempts = 0;
            $queue = $this->newQueue(static function () use (&$attempts): string {
                return 'generated-' . ++$attempts;
            }, $limits);
            if (isset($limits['max_active_jobs'])) {
                $this->queue->push(SuccessJob::class);
            }
            try {
                $queue->push(SuccessJob::class);
                self::fail('容量和字节错误不能自动重试。');
            } catch (QueueFull|PayloadTooLarge) {
                self::assertSame(1, $attempts);
                self::assertSame(1, $this->queue->size());
            }
        }
    }

    public function testLostEnqueueResponseDoesNotGenerateAnotherIdOrResend(): void
    {
        [$process, $pipes] = $this->startProcess('drop-enqueue-response');
        $attempts = 0;
        try {
            $port = (int) trim((string) fgets($pipes[1]));
            self::assertGreaterThan(0, $port);
            $queue = $this->newQueue(static function () use (&$attempts): string {
                return 'uncertain-' . ++$attempts;
            }, ['host' => '127.0.0.1', 'port' => $port]);
            try {
                $queue->push(SuccessJob::class);
                self::fail('入队响应丢失时必须传播异常。');
            } catch (\RedisException) {
                self::assertSame('enqueue-applied', trim((string) fgets($pipes[1])));
                self::assertSame(1, $attempts);
                self::assertSame(1, $this->queue->size());
                self::assertSame('uncertain-1', $this->queue->pop()->getJobId());
            }
        } finally {
            self::assertSame(0, $this->finishProcess($process, $pipes));
        }
    }

    public function testConcurrentAutomaticIdsAndForcedCollisionsKeepEveryTask(): void
    {
        foreach (['produce-ids', 'produce-collisions'] as $mode) {
            $before = $this->queue->size();
            $this->runConcurrentProducers($mode);
            self::assertSame($before + 40, $this->queue->size());
            self::assertSame($before + 40, $this->redis->xLen($this->queue->transport()->keys('default')[0]));
            self::assertSame(40, $this->redis->hLen($this->config['prefix'] . ':returned-ids'));
            foreach ($this->redis->hGetAll($this->config['prefix'] . ':returned-ids') as $id => $count) {
                self::assertMatchesRegularExpression('/\A[0-9a-f]{32}\z/', $id);
                self::assertSame('1', $count);
            }
        }
    }

    public function testConcurrentCustomIdsDeduplicateOnlyIdenticalTasks(): void
    {
        foreach (['produce-duplicates', 'produce-id-conflicts'] as $mode) {
            if ($mode === 'produce-id-conflicts') {
                $this->queue->pop()->delete();
            }
            $this->runConcurrentProducers($mode);
            self::assertSame(1, $this->queue->size());
            self::assertSame(1, $this->redis->xLen($this->queue->transport()->keys('default')[0]));
            self::assertSame(1, $this->redis->hLen($this->config['prefix'] . ':returned-ids'));
            self::assertSame($mode === 'produce-duplicates' ? '40' : '10',
                $this->redis->get($this->config['prefix'] . ':accepted'));
            self::assertSame($mode === 'produce-duplicates' ? false : '30',
                $this->redis->get($this->config['prefix'] . ':rejected'));
        }
    }

    private function newQueue(?Closure $generator = null, array $overrides = []): Queue
    {
        $config = array_replace($this->config, $overrides);
        return new Queue(new Transport(RedisConnection::connect($config), $config), new App(), 'default', $generator);
    }

    private function runConcurrentProducers(string $mode): void
    {
        foreach (['ready', 'start', 'accepted', 'rejected', 'returned-ids'] as $suffix) {
            $this->redis->del($this->config['prefix'] . ':' . $suffix);
        }
        $children = [];
        try {
            for ($i = 0; $i < 4; $i++) {
                $children[] = $this->startProcess($mode);
            }
            $deadline = microtime(true) + 5;
            while ($this->redis->get($this->config['prefix'] . ':ready') !== '4' && microtime(true) < $deadline) {
                usleep(1000);
            }
            $ready = $this->redis->get($this->config['prefix'] . ':ready');
            $this->redis->set($this->config['prefix'] . ':start', '1');
        } finally {
            foreach ($children as [$process, $pipes]) {
                self::assertSame(0, $this->finishProcess($process, $pipes));
            }
        }
        self::assertSame('4', $ready);
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
