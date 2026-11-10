<?php
declare(strict_types=1);

namespace ThinkQueue\Streams;

use Closure;
use RuntimeException;
use Throwable;

/** 前台运行的单项目主进程；Worker 使用全新 PHP 进程启动，不继承应用连接。 */
final class Supervisor
{
    private array $children = [];
    private array $restarts = [];
    private bool $stopping = false;
    private bool $reloadRequested = false;
    private int $generation = 0;

    public function __construct(
        private string $entryPoint,
        private string $configFile,
        private string $pidFile,
        private Closure $output,
    ) {}

    /** 写入重载请求；使用文件通知，因此不额外依赖 posix 扩展或外部 kill 命令。 */
    public static function requestReload(string $pidFile): void
    {
        $handle = @fopen($pidFile, 'r+e');
        if ($handle === false) throw new RuntimeException('主进程未运行或 PID 文件不可访问。');
        try {
            if (flock($handle, LOCK_EX | LOCK_NB)) {
                flock($handle, LOCK_UN);
                throw new RuntimeException('主进程未运行。');
            }
            if (file_put_contents($pidFile . '.reload', bin2hex(random_bytes(16)), LOCK_EX) === false) {
                throw new RuntimeException('无法写入主进程重载请求。');
            }
        } finally {
            fclose($handle);
        }
    }

    /** 持有项目锁直到所有子进程退出；SIGTERM/SIGINT 停止，SIGHUP 重新加载配置。 */
    public function run(): int
    {
        if (PHP_SAPI !== 'cli' || !extension_loaded('pcntl')) {
            throw new RuntimeException('queue:supervise 要求 Linux/macOS CLI、pcntl 和 proc_open。');
        }
        foreach (['proc_open', 'proc_get_status', 'proc_terminate', 'proc_close',
            'pcntl_async_signals', 'pcntl_signal', 'pcntl_signal_get_handler', 'pcntl_alarm'] as $function) {
            if (!function_exists($function)) throw new RuntimeException('缺少或禁用了函数：' . $function);
        }
        if (!is_file($this->entryPoint)) throw new RuntimeException('应用 think 入口不存在：' . $this->entryPoint);
        $this->stopping = false;
        $this->reloadRequested = false;
        $this->restarts = [];
        $config = SupervisorConfig::load($this->configFile);
        $lock = $this->lock();
        $previousAsync = pcntl_async_signals(true);
        $previousHandlers = [];
        foreach ([SIGTERM, SIGINT, SIGHUP] as $signal) $previousHandlers[$signal] = pcntl_signal_get_handler($signal);
        pcntl_signal(SIGTERM, function (): void { $this->stopping = true; });
        pcntl_signal(SIGINT, function (): void { $this->stopping = true; });
        pcntl_signal(SIGHUP, function (): void { $this->reloadRequested = true; });
        $token = $this->reloadToken();
        try {
            $this->log('主进程启动', ['pid_file' => $this->pidFile, 'workers' => count($config->slots())]);
            while (true) {
                $this->reap($config);
                $nextToken = $this->reloadToken();
                if (!$this->stopping && ($this->reloadRequested || ($nextToken !== '' && $nextToken !== $token))) {
                    $this->reloadRequested = false;
                    $token = $nextToken;
                    try {
                        $nextConfig = SupervisorConfig::load($this->configFile);
                        $config = $nextConfig;
                        $this->generation++;
                        $this->restarts = [];
                        $this->log('配置已重载', ['workers' => count($config->slots()), 'generation' => $this->generation]);
                    } catch (Throwable $exception) {
                        $this->log('配置重载失败', ['exception' => $exception::class, 'error' => $exception->getMessage()]);
                    }
                }
                $this->reconcile($config);
                if ($this->stopping && $this->children === []) break;
                usleep(100000);
            }
            return 0;
        } finally {
            // 发生主循环异常时也先回收 Worker，再释放项目锁。
            $this->stopping = true;
            while ($this->children !== []) {
                $this->reconcile($config);
                usleep(100000);
                $this->reap($config);
            }
            foreach ($previousHandlers as $signal => $handler) pcntl_signal($signal, $handler);
            pcntl_async_signals($previousAsync);
            $this->log('主进程退出');
            flock($lock, LOCK_UN);
            fclose($lock);
            // 保留锁文件，避免 unlink 后出现两个不同 inode 各自持锁的竞态。
        }
    }

    /** 回收已退出的子进程，并为非计划退出安排有上限的指数退避。 */
    private function reap(SupervisorConfig $config): void
    {
        foreach ($this->children as $slot => $child) {
            $status = $child['status'] ?? proc_get_status($child['process']);
            if ($status['running']) continue;
            proc_close($child['process']);
            unset($this->children[$slot]);
            $planned = $child['stopping_at'] !== null;
            $delay = 0;
            if (!$planned && !$this->stopping) {
                $failures = $this->restarts[$slot]['failures'] ?? 0;
                if ($status['exitcode'] === 0 || self::now() - $child['started_at'] >= 30) $failures = 0;
                $delay = $this->scheduleRestart($slot, $failures, $config);
            }
            $this->log('子进程退出', ['queue' => $child['queue'], 'slot' => $slot, 'child_pid' => $child['pid'],
                'exit_code' => $status['exitcode'], 'signal' => $status['termsig'], 'planned' => $planned, 'restart_delay' => $delay]);
        }
    }

    /** 旧代 Worker 先退场；并发上限包含正在退出的 Worker，不在重载时重复扩容。 */
    private function reconcile(SupervisorConfig $config): void
    {
        $slots = $this->stopping ? [] : $config->slots();
        foreach ($this->children as $slot => &$child) {
            if ($this->stopping || $child['generation'] !== $this->generation || !isset($slots[$slot])) {
                if ($child['stopping_at'] === null) {
                    $child['stopping_at'] = self::now();
                    proc_terminate($child['process'], SIGTERM);
                    $this->log('子进程停止请求', ['queue' => $child['queue'], 'child_pid' => $child['pid']]);
                } elseif (!$child['killed'] && self::now() - $child['stopping_at'] >= $child['stop_wait_seconds']) {
                    proc_terminate($child['process'], SIGKILL);
                    $child['killed'] = true;
                    $this->log('子进程强制退出', ['queue' => $child['queue'], 'child_pid' => $child['pid']]);
                }
            }
        }
        unset($child);
        foreach ($slots as $slot => $spec) {
            if ($this->stopping) break;
            if (count($this->children) >= $config->maxProcesses) break;
            // 缩容时，同队列尚未退出的旧槽位也占用目标并发预算。
            $queueCount = count(array_filter($this->children, static fn (array $child): bool => $child['queue'] === $spec['queue']));
            if (isset($this->children[$slot]) || $queueCount >= $spec['options']['processes'] ||
                self::now() < ($this->restarts[$slot]['at'] ?? 0)) continue;
            $this->spawn($slot, $spec, $config);
        }
        foreach (array_keys($this->restarts) as $slot) {
            if (!isset($slots[$slot]) && !isset($this->children[$slot])) unset($this->restarts[$slot]);
        }
    }

    /** 传入参数数组直接执行 PHP；标准输出直连主进程，由宝塔收集，避免日志管道积压。 */
    private function spawn(string $slot, array $spec, SupervisorConfig $config): void
    {
        $command = [PHP_BINARY, $this->entryPoint, 'queue:work', '--queue=' . $spec['queue']];
        foreach ($spec['options'] as $key => $value) {
            if ($key !== 'processes') $command[] = '--' . str_replace('_', '-', $key) . '=' . $value;
        }
        try {
            $process = @proc_open($command, [0 => ['file', '/dev/null', 'r'], 1 => STDOUT, 2 => STDERR], $pipes, dirname($this->entryPoint));
            if ($process === false) throw new RuntimeException('无法启动 Worker 子进程。');
        } catch (Throwable $exception) {
            $delay = $this->scheduleRestart($slot, $this->restarts[$slot]['failures'] ?? 0, $config);
            $this->log('子进程启动失败', ['queue' => $spec['queue'], 'slot' => $slot,
                'exception' => $exception::class, 'error' => $exception->getMessage(), 'restart_delay' => $delay]);
            return;
        }
        $status = proc_get_status($process);
        $this->children[$slot] = ['process' => $process, 'pid' => $status['pid'], 'queue' => $spec['queue'],
            'generation' => $this->generation, 'started_at' => self::now(), 'stopping_at' => null,
            'stop_wait_seconds' => $config->stopWaitSeconds, 'killed' => false,
            // PHP 8.2 的 exitcode 只能读取一次，极快退出时保留第一次状态。
            'status' => $status['running'] ? null : $status];
        $this->log('子进程启动', ['queue' => $spec['queue'], 'slot' => $slot, 'child_pid' => $status['pid']]);
    }

    private function scheduleRestart(string $slot, int $failures, SupervisorConfig $config): int
    {
        $delay = $config->restartDelay;
        for ($i = 0; $i < min($failures, 30) && $delay < $config->restartMaxDelay; $i++) {
            $delay = $delay > intdiv($config->restartMaxDelay, 2) ? $config->restartMaxDelay : $delay * 2;
        }
        $this->restarts[$slot] = ['failures' => min($failures + 1, 30), 'at' => self::now() + $delay];
        return $delay;
    }

    /** 锁使用 close-on-exec，避免 Worker 意外继承主进程锁。 */
    private function lock(): mixed
    {
        $directory = dirname($this->pidFile);
        if (!is_dir($directory) && !@mkdir($directory, 0775, true) && !is_dir($directory)) {
            throw new RuntimeException('无法创建主进程 PID 目录。');
        }
        $handle = @fopen($this->pidFile, 'c+e');
        if ($handle === false) throw new RuntimeException('无法打开主进程 PID 文件。');
        if (!flock($handle, LOCK_EX | LOCK_NB)) {
            fclose($handle);
            throw new RuntimeException('当前项目已有 queue:supervise 主进程运行。');
        }
        if (!ftruncate($handle, 0) || fwrite($handle, (string) getmypid() . "\n") === false || !fflush($handle)) {
            fclose($handle);
            throw new RuntimeException('无法写入主进程 PID 文件。');
        }
        return $handle;
    }

    private function reloadToken(): string
    {
        return (string) (@file_get_contents($this->pidFile . '.reload') ?: '');
    }

    private function log(string $event, array $context = []): void
    {
        ($this->output)('[' . date('c') . '][' . $event . '] pid=' . getmypid() . ' ' .
            json_encode($context, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE | JSON_PARTIAL_OUTPUT_ON_ERROR));
    }

    private static function now(): float
    {
        return hrtime(true) / 1000000000;
    }
}
