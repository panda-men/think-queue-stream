<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Tests;

use PHPUnit\Framework\TestCase;
use PHPUnit\Framework\Attributes\DataProvider;
use Redis;
use think\App;
use ThinkQueue\Streams\Queue;
use ThinkQueue\Streams\RedisConnection;
use ThinkQueue\Streams\Supervisor;
use ThinkQueue\Streams\Transport;
use ThinkQueue\Streams\Tests\Fixtures\FailureDatabase;
use ThinkQueue\Streams\Tests\Fixtures\SupervisedJob;

final class SupervisorTest extends TestCase
{
    private string $root;
    private array $config;
    private Redis $redis;
    private Queue $queue;
    private array $processes = [];

    protected function setUp(): void
    {
        $prefix = 'supervisor-' . bin2hex(random_bytes(8));
        // 路径含空格，验证参数数组执行时不会被 shell 拆分。
        $this->root = sys_get_temp_dir() . '/' . $prefix . ' project';
        mkdir($this->root . '/config', 0775, true);
        mkdir($this->root . '/app');
        file_put_contents($this->root . '/think', '<?php require ' . var_export(__DIR__ . '/Fixtures/supervised.php', true) . ';');
        $this->config = ['host' => getenv('REDIS_HOST') ?: '127.0.0.1',
            'port' => (int) (getenv('REDIS_PORT') ?: 6379), 'prefix' => $prefix,
            'block_ms' => 50, 'retry_after_ms' => 5000, 'claim_interval_ms' => 50,
            'supervisor' => ['stop_wait_seconds' => 3, 'restart_delay' => 1, 'restart_max_delay' => 4, 'workers' => []]];
        $this->redis = RedisConnection::connect($this->config);
        $this->queue = new Queue(new Transport($this->redis, $this->config, FailureDatabase::store($prefix)), new App());
        $this->writePhp('database', FailureDatabase::configuration($prefix));
        $this->writePhp('cache', ['default' => 'file', 'stores' => ['file' => ['type' => 'File', 'path' => $this->root . '/runtime/cache/']]]);
        $this->writePhp('log', ['default' => 'file', 'channels' => ['file' => ['type' => 'File', 'path' => $this->root . '/runtime/log/']]]);
        $this->writeConfig();
    }

    protected function tearDown(): void
    {
        foreach (array_reverse(array_keys($this->processes)) as $index) $this->stop($index);
        foreach ($this->redis->keys('*' . $this->config['prefix'] . '*') as $key) $this->redis->del($key);
        FailureDatabase::cleanup($this->config['prefix']);
        $files = new \RecursiveIteratorIterator(new \RecursiveDirectoryIterator($this->root, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::CHILD_FIRST);
        foreach ($files as $file) $file->isDir() ? rmdir($file->getPathname()) : unlink($file->getPathname());
        rmdir($this->root);
    }

    public function testConcurrentQueuesConsumeAndNormalWorkerExitIsReplaced(): void
    {
        $this->workers(['mail' => ['processes' => 2, 'max_jobs' => 1], 'sync' => ['processes' => 1], '0' => ['processes' => 1]]);
        foreach (['a' => 'mail', 'b' => 'mail', 'c' => 'mail', 'd' => 'sync', 'zero' => '0'] as $id => $name) $this->push($name, $id);
        $master = $this->start();
        $this->until(fn (): bool => $this->completed('c') === 1 && $this->completed('d') === 1 && $this->completed('zero') === 1);
        self::assertSame(1, $this->completed('a'));
        self::assertSame(1, $this->completed('b'));
        self::assertGreaterThanOrEqual(4, count($this->events($master, '子进程启动')));
        self::assertStringContainsString('"exit_code":0', $this->processLog($master));
        self::assertStringContainsString('reason=max_jobs', $this->processLog($master));
        self::assertSame(0, $this->queue->size('mail'));
        self::assertSame(0, $this->queue->size('sync'));
        self::assertSame(0, $this->queue->size('0'));
    }

    public function testHardTimeoutIsReplacedAndLaterTaskRuns(): void
    {
        $this->workers(['mail' => ['processes' => 1, 'timeout' => 1]]);
        $this->push('mail', 'slow', 1800);
        $this->push('mail', 'next');
        $master = $this->start();
        $this->until(fn (): bool => $this->completed('next') === 1);
        self::assertSame(0, $this->completed('slow'));
        self::assertStringContainsString('"exit_code":124', $this->processLog($master));
        self::assertNotSame($this->started('slow'), $this->started('next'));
    }

    public function testExistingRestartMarkerAndReloadedWorkerOptionsTakeEffect(): void
    {
        $this->workers(['mail' => ['processes' => 1]]);
        $master = $this->start();
        $this->until(fn (): bool => str_contains($this->processLog($master), '[启动] queue=mail'));
        $old = array_keys($this->liveChildren($master));
        $this->queue->transport()->restartToken(true);
        $this->until(fn (): bool => count($this->events($master, '子进程启动')) >= 2);
        self::assertStringContainsString('reason=restart', $this->processLog($master));
        self::assertSame([], array_intersect($old, array_keys($this->liveChildren($master))));
        $this->workers(['mail' => ['processes' => 1, 'max_jobs' => 1], 'paused' => ['processes' => 0]]);
        Supervisor::requestReload($this->pidFile());
        $this->until(fn (): bool => str_contains($this->processLog($master), '[配置已重载]') &&
            count($this->events($master, '子进程启动')) >= 3);
        $this->push('mail', 'options');
        $this->until(fn (): bool => $this->completed('options') === 1 && str_contains($this->processLog($master), 'reason=max_jobs'));
        self::assertNotContains('paused', array_column($this->events($master, '子进程启动'), 'queue'));
    }

    public function testReloadAddsRemovesAndScalesQueuesWithoutExceedingBudget(): void
    {
        $this->config['supervisor']['max_processes'] = 3;
        $this->workers(['mail' => ['processes' => 2], 'sync' => ['processes' => 1]]);
        $master = $this->start();
        $this->until(fn (): bool => count($this->liveChildren($master)) === 3);
        $pid = trim(file_get_contents($this->pidFile()));
        $old = array_keys($this->liveChildren($master));
        $this->workers(['mail' => ['processes' => 1], 'export' => ['processes' => 2]]);
        $reload = $this->start(['--reload']);
        $this->until(fn (): bool => str_contains($this->processLog($master), '[配置已重载]') &&
            count($this->liveChildren($master)) === 3 && array_intersect($old, array_keys($this->liveChildren($master))) === []);
        $this->waitExit($reload);
        self::assertStringContainsString('已发送配置重载请求', $this->processLog($reload));
        self::assertSame($pid, trim(file_get_contents($this->pidFile())));
        self::assertSame(['export', 'export', 'mail'], $this->liveQueues($master));
        $this->assertPeakConcurrency($master, 3);
        $this->push('export', 'new');
        $this->until(fn (): bool => $this->completed('new') === 1);
        // SIGHUP 同样支持重载；清空配置可停止全部 Worker，主进程继续运行。
        $this->workers([]);
        proc_terminate($this->processes[$master]['handle'], SIGHUP);
        $this->until(fn (): bool => count($this->events($master, '配置已重载')) === 2 && $this->liveChildren($master) === []);
        self::assertTrue(proc_get_status($this->processes[$master]['handle'])['running']);
    }

    public function testInvalidReloadKeepsExistingWorkersAndCanRecover(): void
    {
        $this->workers(['mail' => ['processes' => 1]]);
        $master = $this->start();
        $this->until(fn (): bool => count($this->liveChildren($master)) === 1);
        $before = array_keys($this->liveChildren($master));
        $this->workers(['mail' => ['processes' => -1]]);
        Supervisor::requestReload($this->pidFile());
        $this->until(fn (): bool => str_contains($this->processLog($master), '[配置重载失败]'));
        self::assertSame($before, array_keys($this->liveChildren($master)));
        $this->push('mail', 'old');
        $this->until(fn (): bool => $this->completed('old') === 1);
        $this->config['supervisor']['workers'] = null;
        $this->writeConfig();
        $reload = $this->start(['--reload']);
        $this->waitExit($reload);
        $this->until(fn (): bool => count($this->events($master, '配置重载失败')) === 2);
        self::assertSame($before, array_keys($this->liveChildren($master)));
        file_put_contents($this->root . '/config/queue_streams.php', '<?php return [;');
        proc_terminate($this->processes[$master]['handle'], SIGHUP);
        $this->until(fn (): bool => count($this->events($master, '配置重载失败')) === 3);
        self::assertSame($before, array_keys($this->liveChildren($master)));
        $this->workers(['mail' => ['processes' => 2]]);
        Supervisor::requestReload($this->pidFile());
        $this->until(fn (): bool => count($this->liveChildren($master)) === 2);
    }

    public function testRepeatedReloadWhileBusyConvergesToLatestQueueAndLowerBudget(): void
    {
        $this->config['supervisor']['max_processes'] = 2;
        $this->workers(['mail' => ['processes' => 2]]);
        $this->push('mail', 'busy-a', 1200);
        $this->push('mail', 'busy-b', 1200);
        $master = $this->start();
        $this->until(fn (): bool => $this->started('busy-a') !== false && $this->started('busy-b') !== false);
        $this->config['supervisor']['max_processes'] = 1;
        $this->workers(['mail' => ['processes' => 1]]);
        Supervisor::requestReload($this->pidFile());
        $this->until(fn (): bool => count($this->events($master, '配置已重载')) === 1);
        $this->workers(['export' => ['processes' => 1]]);
        Supervisor::requestReload($this->pidFile());
        $this->until(fn (): bool => count($this->events($master, '配置已重载')) === 2 && $this->liveQueues($master) === ['export']);
        self::assertSame(1, $this->completed('busy-a'));
        self::assertSame(1, $this->completed('busy-b'));
        self::assertCount(2, $this->events($master, '子进程停止请求'));
        $this->assertPeakConcurrency($master, 2);
        self::assertStringNotContainsString('[子进程强制退出]', $this->processLog($master));
        $this->push('export', 'latest');
        $this->until(fn (): bool => $this->completed('latest') === 1);
    }

    public function testGracefulReloadAndStopFinishCurrentJobs(): void
    {
        $this->workers(['mail' => ['processes' => 1]]);
        $this->push('mail', 'reload', 800);
        $master = $this->start();
        $this->until(fn (): bool => $this->started('reload') !== false);
        $this->workers(['sync' => ['processes' => 1]]);
        Supervisor::requestReload($this->pidFile());
        $this->until(fn (): bool => $this->completed('reload') === 1 && $this->liveQueues($master) === ['sync']);
        $this->push('sync', 'stop', 800);
        $this->until(fn (): bool => $this->started('stop') !== false);
        proc_terminate($this->processes[$master]['handle'], SIGTERM);
        $this->waitExit($master);
        self::assertSame(1, $this->completed('stop'));
        self::assertSame([], $this->liveChildren($master));
        self::assertStringNotContainsString('[子进程强制退出]', $this->processLog($master));
        self::assertStringContainsString('[主进程退出]', $this->processLog($master));
    }

    public function testStopDuringLoopingDoesNotClaimAnotherJob(): void
    {
        file_put_contents($this->root . '/worker-mode', 'stop-before-claim');
        $this->workers(['mail' => ['processes' => 1]]);
        $this->push('mail', 'unclaimed');
        $master = $this->start();
        $this->until(fn (): bool => is_file($this->root . '/loop-entered'));
        $this->stop($master);
        self::assertSame(0, $this->completed('unclaimed'));
        self::assertFalse($this->started('unclaimed'));
        self::assertSame(1, $this->queue->size('mail'));
    }

    #[DataProvider('disabledProcessFunctions')]
    public function testMissingProcessFunctionFailsBeforeStartingWorkers(string $function): void
    {
        file_put_contents($this->root . '/worker-mode', 'crash');
        $this->workers(['mail' => ['processes' => 1]]);
        $master = $this->start([], ['-d', 'disable_functions=' . $function]);
        $this->waitExit($master);
        self::assertStringContainsString('缺少或禁用了函数：' . $function, $this->processLog($master));
        self::assertSame([], $this->events($master, '子进程启动'));
        self::assertFileDoesNotExist($this->root . '/worker-started');
    }

    public static function disabledProcessFunctions(): iterable
    {
        foreach (['proc_open', 'proc_get_status', 'proc_terminate', 'proc_close',
            'pcntl_async_signals', 'pcntl_signal', 'pcntl_signal_get_handler', 'pcntl_alarm'] as $function) yield $function => [$function];
    }

    public function testDuplicateMasterIsRejectedAndLockCanBeReusedAfterStop(): void
    {
        $master = $this->start();
        $this->until(fn (): bool => str_contains($this->processLog($master), '[主进程启动]'));
        $duplicate = $this->start();
        $this->waitExit($duplicate);
        self::assertStringContainsString('已有 queue:supervise 主进程运行', $this->processLog($duplicate));
        $this->stop($master);
        $replacement = $this->start();
        $this->until(fn (): bool => str_contains($this->processLog($replacement), '[主进程启动]'));
        self::assertTrue(proc_get_status($this->processes[$replacement]['handle'])['running']);
    }

    public function testRepeatedCrashesBackOffAndShutdownCancelsRestart(): void
    {
        file_put_contents($this->root . '/worker-mode', 'crash');
        $this->workers(['mail' => ['processes' => 1]]);
        $master = $this->start();
        $this->until(fn (): bool => count($this->events($master, '子进程退出')) >= 3);
        $exits = $this->events($master, '子进程退出');
        self::assertSame([1, 2, 4], array_column(array_slice($exits, 0, 3), 'restart_delay'));
        self::assertSame([17, 17, 17], array_column(array_slice($exits, 0, 3), 'exit_code'));
        $this->stop($master);
        self::assertCount(3, $this->events($master, '子进程启动'));
    }

    public function testCrashingQueueRestartsWithoutInterruptingOtherQueues(): void
    {
        $faultFile = $this->root . '/worker-mode-broken';
        file_put_contents($faultFile, 'crash');
        $this->config['supervisor']['max_processes'] = 3;
        $this->workers(['broken' => ['processes' => 1], 'mail' => ['processes' => 1], 'sync' => ['processes' => 1]]);
        $master = $this->start();
        foreach (['mail', 'sync'] as $queue) $this->push($queue, 'baseline-' . $queue);
        $this->until(fn (): bool => $this->completed('baseline-mail') === 1 && $this->completed('baseline-sync') === 1);
        $masterPid = (int) trim(file_get_contents($this->pidFile()));
        $healthyPids = ['mail' => $this->started('baseline-mail'), 'sync' => $this->started('baseline-sync')];
        $brokenEvents = fn (string $event): array => array_values(array_filter($this->events($master, $event),
            static fn (array $entry): bool => $entry['queue'] === 'broken'));

        // 每次异常退出后继续投递正常队列，验证各轮重启期间仍有业务进展。
        for ($round = 1; $round <= 3; $round++) {
            $this->until(fn (): bool => count($brokenEvents('子进程退出')) >= $round);
            $status = proc_get_status($this->processes[$master]['handle']);
            self::assertTrue($status['running']);
            self::assertSame($masterPid, $status['pid']);
            self::assertSame($masterPid, (int) trim(file_get_contents($this->pidFile())));
            foreach (['mail', 'sync'] as $queue) $this->push($queue, 'round-' . $round . '-' . $queue);
            $this->until(fn (): bool => $this->completed('round-' . $round . '-mail') === 1 &&
                $this->completed('round-' . $round . '-sync') === 1);
            foreach ($healthyPids as $queue => $pid) {
                self::assertSame($pid, $this->started('round-' . $round . '-' . $queue));
            }
        }

        $exits = array_slice($brokenEvents('子进程退出'), 0, 3);
        self::assertSame([17, 17, 17], array_column($exits, 'exit_code'));
        self::assertSame([1, 2, 4], array_column($exits, 'restart_delay'));
        self::assertSame([false, false, false], array_column($exits, 'planned'));
        $crashedPids = array_column($exits, 'child_pid');
        self::assertCount(3, array_unique($crashedPids));
        self::assertSame($crashedPids, array_column(array_slice($brokenEvents('子进程启动'), 0, 3), 'child_pid'));

        // 解除故障后无需人工拉起，下一次自动重启的 Worker 应能正常消费。
        unlink($faultFile);
        $this->push('broken', 'recovered');
        $this->until(fn (): bool => $this->completed('recovered') === 1);
        self::assertNotContains((int) $this->started('recovered'), $crashedPids);
        self::assertCount(4, $brokenEvents('子进程启动'));
        self::assertSame(['broken', 'mail', 'sync'], $this->liveQueues($master));
        self::assertTrue(proc_get_status($this->processes[$master]['handle'])['running']);
        self::assertSame($masterPid, (int) trim(file_get_contents($this->pidFile())));

        foreach ($healthyPids as $queue => $pid) {
            $this->push($queue, 'after-recovery-' . $queue);
            $this->until(fn (): bool => $this->completed('after-recovery-' . $queue) === 1);
            self::assertSame($pid, $this->started('after-recovery-' . $queue));
            self::assertCount(1, array_filter($this->events($master, '子进程启动'),
                static fn (array $entry): bool => $entry['queue'] === $queue));
            self::assertCount(0, array_filter($this->events($master, '子进程退出'),
                static fn (array $entry): bool => $entry['queue'] === $queue));
            self::assertSame(0, $this->queue->size($queue));
        }
        self::assertSame(0, $this->queue->size('broken'));
        $this->assertPeakConcurrency($master, 3);
        $this->stop($master);
        self::assertSame([], $this->liveChildren($master));
        self::assertStringContainsString('[主进程退出]', $this->processLog($master));
    }

    public function testUnresponsiveWorkerIsKilledAfterGracePeriod(): void
    {
        file_put_contents($this->root . '/worker-mode', 'ignore-term');
        $this->config['supervisor']['stop_wait_seconds'] = 1;
        $this->workers(['mail' => ['processes' => 1]]);
        $master = $this->start();
        $this->until(fn (): bool => count($this->liveChildren($master)) === 1);
        // 等待 fixture 安装忽略信号的处理器。
        usleep(200000);
        $this->stop($master);
        self::assertStringContainsString('[子进程强制退出]', $this->processLog($master));
        self::assertSame([], $this->liveChildren($master));
    }

    public function testReloadWithoutRunningMasterFails(): void
    {
        $reload = $this->start(['--reload']);
        $this->waitExit($reload);
        self::assertStringContainsString('主进程未运行', $this->processLog($reload));
        // 即使保留了上次运行的 PID 文件，空闲文件锁仍然表示没有主进程。
        if (!is_dir($this->root . '/runtime')) mkdir($this->root . '/runtime', 0775, true);
        file_put_contents($this->pidFile(), "12345\n");
        $stale = $this->start(['--reload']);
        $this->waitExit($stale);
        self::assertStringContainsString('主进程未运行', $this->processLog($stale));
    }

    public function testInvalidInitialConfigurationDoesNotStartWorkers(): void
    {
        $this->workers(['mail' => ['timeout' => 5]]);
        $master = $this->start();
        $this->waitExit($master);
        self::assertStringContainsString('timeout * 1000 必须小于 retry_after_ms', $this->processLog($master));
        self::assertSame([], $this->events($master, '子进程启动'));
        self::assertFileDoesNotExist($this->pidFile());
    }

    private function workers(array $workers): void
    {
        foreach ($workers as &$options) $options += ['timeout' => 3];
        unset($options);
        $this->config['supervisor']['workers'] = $workers;
        $this->writeConfig();
    }

    private function writeConfig(): void
    {
        $this->writePhp('queue_streams', $this->config);
    }

    private function writePhp(string $name, array $data): void
    {
        $path = $this->root . '/config/' . $name . '.php';
        file_put_contents($path . '.new', '<?php return ' . var_export($data, true) . ';');
        rename($path . '.new', $path);
    }

    private function push(string $queue, string $id, int $wait = 0): void
    {
        $this->queue->push(SupervisedJob::class, ['started' => $this->config['prefix'] . ':started:' . $id,
            'completed' => $this->config['prefix'] . ':completed:' . $id, 'wait_ms' => $wait], $queue, ['max_tries' => 1]);
    }

    private function started(string $id): string|false
    {
        return $this->redis->get($this->config['prefix'] . ':started:' . $id);
    }

    private function completed(string $id): int
    {
        return (int) $this->redis->get($this->config['prefix'] . ':completed:' . $id);
    }

    private function pidFile(): string
    {
        return $this->root . '/runtime/queue-streams-supervisor.pid';
    }

    private function start(array $arguments = [], array $phpOptions = []): int
    {
        $index = count($this->processes);
        $log = $this->root . '/process-' . $index . '.log';
        $handle = proc_open([PHP_BINARY, ...$phpOptions, $this->root . '/think', 'queue:supervise', ...$arguments],
            [0 => ['file', '/dev/null', 'r'], 1 => ['file', $log, 'a'], 2 => ['file', $log, 'a']], $pipes, $this->root);
        self::assertIsResource($handle);
        $this->processes[$index] = ['handle' => $handle, 'log' => $log];
        return $index;
    }

    private function processLog(int $index): string
    {
        return file_get_contents($this->processes[$index]['log']);
    }

    private function events(int $index, string $event): array
    {
        $events = [];
        foreach (explode("\n", $this->processLog($index)) as $line) {
            if (preg_match('/\[' . preg_quote($event, '/') . '\] pid=\d+ (\{.*\})$/', $line, $match)) {
                $events[] = json_decode($match[1], true, 512, JSON_THROW_ON_ERROR);
            }
        }
        return $events;
    }

    private function liveChildren(int $index): array
    {
        $live = [];
        foreach (explode("\n", $this->processLog($index)) as $line) {
            if (preg_match('/\[(子进程启动|子进程退出)\] pid=\d+ (\{.*\})$/', $line, $match)) {
                $event = json_decode($match[2], true, 512, JSON_THROW_ON_ERROR);
                if ($match[1] === '子进程启动') $live[$event['child_pid']] = $event;
                else unset($live[$event['child_pid']]);
            }
        }
        return $live;
    }

    private function liveQueues(int $index): array
    {
        $queues = array_column($this->liveChildren($index), 'queue');
        sort($queues);
        return $queues;
    }

    private function assertPeakConcurrency(int $index, int $maximum): void
    {
        $count = 0;
        foreach (explode("\n", $this->processLog($index)) as $line) {
            if (str_contains($line, '[子进程启动]')) $count++;
            if (str_contains($line, '[子进程退出]')) $count--;
            self::assertLessThanOrEqual($maximum, $count);
        }
    }

    private function until(callable $condition, float $seconds = 10): void
    {
        $deadline = microtime(true) + $seconds;
        do {
            if ($condition()) return;
            usleep(30000);
        } while (microtime(true) < $deadline);
        self::fail('等待进程状态超时：' . implode("\n", array_map(fn (int $index): string => $this->processLog($index), array_keys($this->processes))));
    }

    private function waitExit(int $index): void
    {
        $this->until(fn (): bool => !proc_get_status($this->processes[$index]['handle'])['running']);
    }

    private function stop(int $index): void
    {
        $process = $this->processes[$index]['handle'];
        if (!is_resource($process)) return;
        if (proc_get_status($process)['running']) {
            proc_terminate($process, SIGTERM);
            $this->waitExit($index);
        }
        proc_close($process);
    }
}
