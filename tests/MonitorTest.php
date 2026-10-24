<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Tests;

use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;
use Stringable;
use think\App;
use think\Console;
use ThinkQueue\Streams\Monitor;
use ThinkQueue\Streams\Queue;
use ThinkQueue\Streams\RedisConnection;
use ThinkQueue\Streams\Transport;
use ThinkQueue\Streams\Worker;
use ThinkQueue\Streams\Tests\Fixtures\LifecycleJob;
use ThinkQueue\Streams\Tests\Fixtures\SlowJob;

final class MonitorTest extends TestCase
{
    private \Redis $redis;
    private array $config;
    private App $app;
    private Queue $queue;
    private Monitor $monitor;
    private array $lines = [];

    /**
     * 使用随机前缀隔离队列并收集运行日志。
     * @return void
     */
    protected function setUp(): void
    {
        $this->config = ['host' => getenv('REDIS_HOST') ?: '127.0.0.1', 'port' => (int) (getenv('REDIS_PORT') ?: 6379),
            'prefix' => 'monitor-' . bin2hex(random_bytes(8)), 'block_ms' => 0, 'retry_after_ms' => 2000];
        $this->redis = RedisConnection::connect($this->config);
        $this->app = new App();
        $this->app->instance(\Redis::class, $this->redis);
        $this->monitor = new Monitor();
        $this->monitor->setOutput(function (string $line): void { $this->lines[] = $line; });
        $this->queue = new Queue(new Transport($this->redis, $this->config), $this->app, 'default', null, $this->monitor);
    }

    /**
     * 只清理当前测试前缀。
     * @return void
     */
    protected function tearDown(): void
    {
        foreach ($this->redis->keys('*' . $this->config['prefix'] . '*') as $key) $this->redis->del($key);
        Console::flushStartCallbacks();
    }

    /**
     * 验证领取、成功、主动重投、异常重试和最终失败的实际结果与统计。
     * @return void
     */
    public function testLifecycleLogsAndCountersExcludeBusinessData(): void
    {
        $this->monitor->start('default', $this->queue->transport()->consumerName());
        $worker = new Worker($this->queue, null, $this->monitor);
        foreach (['success', 'release', 'throw'] as $action) {
            $this->queue->push(LifecycleJob::class, ['action' => $action, 'body' => 'SECRET'], null,
                ['max_tries' => $action === 'throw' ? 3 : 2]);
            $worker->runNextJob(null, 2, 1, 0);
        }
        $worker->runNextJob(null, 2, 1, 0);
        $worker->runNextJob(null, 2, 1, 0);
        $this->monitor->stop('default', $this->queue->transport()->consumerName(), 'test');
        $log = implode("\n", $this->lines);
        foreach (['启动', '投递确认', '领取', '成功', '主动重投', '重试 第1次/最多2次', '重试 第2次/最多2次', '最终失败', '退出'] as $event) {
            self::assertStringContainsString('][' . $event . ']', $log);
        }
        self::assertMatchesRegularExpression('/^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}[+-]\d{2}:\d{2}\]/', $log);
        self::assertStringContainsString('queue=default pid=' . getmypid(), $log);
        self::assertStringContainsString('consumer=' . $this->queue->transport()->consumerName(), $log);
        foreach (['job_id=', 'message_id=', 'attempt=2', 'elapsed_ms=', 'delay_seconds=7', 'exception=RuntimeException'] as $field) {
            self::assertStringContainsString($field, $log);
        }
        self::assertMatchesRegularExpression('/\[重试 第1次\/最多2次\].*attempt=1 max_tries=3.*retry_number=1 max_retries=2/', $log);
        self::assertMatchesRegularExpression('/\[重试 第2次\/最多2次\].*attempt=2 max_tries=3.*retry_number=2 max_retries=2/', $log);
        self::assertStringContainsString('claimed=5 succeeded=1 released=1 retried=2 failed=1', end($this->lines));
        self::assertStringNotContainsString('SECRET', $log);
        self::assertStringNotContainsString('receiver@example.test', $log);
    }

    /**
     * 确认后抛出异常不能误报成功；日志端异常不能阻止确认。
     * @return void
     */
    public function testAckThenExceptionAndBrokenLogger(): void
    {
        $this->queue->push(LifecycleJob::class, ['action' => 'ack-throw']);
        (new Worker($this->queue, null, $this->monitor))->runNextJob(null, 1, 1, 0);
        self::assertStringContainsString('reason=exception_after_ack', end($this->lines));
        self::assertStringNotContainsString('[成功]', end($this->lines));
        $logger = new class extends AbstractLogger {
            /** 抛出日志端故障以验证隔离。 */
            public function log($level, string|Stringable $message, array $context = []): void
            {
                throw new \RuntimeException('logger unavailable');
            }
        };
        $monitor = new Monitor($logger);
        $monitor->setOutput(static function (): void { throw new \RuntimeException('output unavailable'); });
        $queue = new Queue(new Transport($this->redis, $this->config), $this->app, 'default', null, $monitor);
        $queue->push(LifecycleJob::class);
        self::assertTrue((new Worker($queue, null, $monitor))->runNextJob(null, 1, 1, 0));
        self::assertSame(0, $queue->size());
    }

    /**
     * 静默处理器隐藏例行输出和容量推迟，但不隐藏真实失败，也不改变统计。
     * @return void
     */
    public function testSilentJobsSuppressRoutineLogsAndKeepFailures(): void
    {
        $logger = new class extends AbstractLogger {
            public array $lines = [];
            /** 收集持久化端收到的日志。 */
            public function log($level, string|Stringable $message, array $context = []): void
            {
                $this->lines[] = (string) $message;
            }
        };
        $lines = [];
        $monitor = new Monitor($logger, [LifecycleJob::class]);
        $monitor->setOutput(static function (string $line) use (&$lines): void { $lines[] = $line; });
        $queue = new Queue(new Transport($this->redis, $this->config), $this->app, 'default', null, $monitor);
        $worker = new Worker($queue, null, $monitor);
        $monitor->start('default', $queue->transport()->consumerName());
        $queue->push(LifecycleJob::class);
        $worker->runNextJob(null, 2, 1, 0);
        $context = ['job' => LifecycleJob::class . '@fire', 'exception' => \ThinkQueue\Streams\Exception\QueueFull::class];
        foreach (['dispatch', 'dispatch_deferred', 'publish_failed'] as $event) $monitor->record($event, 'default', $context);
        self::assertCount(1, $lines);
        self::assertStringContainsString('[启动]', $lines[0]);
        self::assertSame($lines, $logger->lines);

        $queue->push(LifecycleJob::class, ['action' => 'throw']);
        $worker->runNextJob(null, 2, 1, 0);
        $worker->runNextJob(null, 2, 1, 0);
        $monitor->record('publish_failed', 'default', ['job' => LifecycleJob::class, 'exception' => \RedisException::class]);
        $monitor->stop('default', $queue->transport()->consumerName(), 'test');
        $log = implode("\n", $lines);
        foreach (['[重试 第1次/最多1次]', '[最终失败]', '[投递未确认]', 'exception=RedisException', 'claimed=3 succeeded=1 released=0 retried=1 failed=1'] as $value) {
            self::assertStringContainsString($value, $log);
        }
        self::assertStringNotContainsString('[成功]', $log);
        self::assertStringNotContainsString('[领取]', $log);
        self::assertSame($lines, $logger->lines);
        $monitor->record('published', 'default', ['job' => LifecycleJob::class . '@send']);
        self::assertStringContainsString('@send', end($lines)); // 同类的其他方法仍然可见。
    }

    /**
     * 从实际服务配置读取静默处理器，空闲命令只有启动及退出日志。
     * @return void
     */
    public function testSilentJobConfigurationInRealCommand(): void
    {
        $process = proc_open([PHP_BINARY, __DIR__ . '/Fixtures/process.php', 'monitor-commands', $this->config['prefix'], 'silent'],
            [0 => ['file', '/dev/null', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
        $output = stream_get_contents($pipes[1]);
        $error = stream_get_contents($pipes[2]);
        fclose($pipes[1]); fclose($pipes[2]);
        self::assertSame(0, proc_close($process), $error . $output);
        $result = json_decode($output, true, 512, JSON_THROW_ON_ERROR);
        self::assertSame(2, count(explode("\n", trim($result['work']))));
        self::assertStringContainsString('[启动]', $result['work']);
        self::assertStringContainsString('[退出]', $result['work']);
        self::assertStringContainsString('claimed=1 succeeded=1', $result['work']);
        self::assertSame([false, true], $result['events']);
    }

    /**
     * 验证真实命令触发循环事件、输出退出原因并恢复原输出端。
     * @return void
     */
    public function testCommandLoopingAndOutputRestoration(): void
    {
        $process = proc_open([PHP_BINARY, __DIR__ . '/Fixtures/process.php', 'monitor-commands', $this->config['prefix']],
            [0 => ['file', '/dev/null', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
        $output = stream_get_contents($pipes[1]);
        $error = stream_get_contents($pipes[2]);
        fclose($pipes[1]); fclose($pipes[2]);
        self::assertSame(0, proc_close($process), $error . $output);
        $result = json_decode($output, true, 512, JSON_THROW_ON_ERROR);
        self::assertStringContainsString('reason=max_jobs', $result['work']);
        self::assertStringContainsString('claimed=1 succeeded=1', $result['work']);
        self::assertStringContainsString('reason=once', $result['once']);
        self::assertStringContainsString('claimed=0', $result['once']);
        self::assertSame([false, true], $result['events']);
        self::assertStringContainsString('job_id=' . $result['id'], $result['restored']);
    }

    /**
     * 超时日志包含任务及退出统计，任务仍保留在 PEL 供接管。
     * @return void
     */
    public function testTimeoutLogsAndPendingReservation(): void
    {
        $id = $this->queue->push(SlowJob::class, ['counter' => $this->config['prefix'] . ':counter']);
        $process = proc_open([PHP_BINARY, __DIR__ . '/Fixtures/process.php', 'logged-timeout', $this->config['prefix']],
            [0 => ['file', '/dev/null', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
        $output = stream_get_contents($pipes[1]);
        $error = stream_get_contents($pipes[2]);
        fclose($pipes[1]); fclose($pipes[2]);
        self::assertSame(124, proc_close($process), $error);
        self::assertStringContainsString('[执行超时]', $output);
        self::assertStringContainsString('job_id=' . $id, $output);
        self::assertStringContainsString('reason=timeout', $output);
        self::assertStringContainsString('timed_out=1', $output);
        self::assertSame(1, $this->queue->transport()->stats('default')['pending']);
    }
}
