<?php
declare(strict_types=1);

namespace ThinkQueue\Streams;

use Closure;
use DateTimeImmutable;
use Psr\Log\LoggerInterface;
use Throwable;
use ThinkQueue\Streams\Exception\QueueFull;

/** 统一的队列运行日志；记录有界业务引用及显式提供的安全错误说明。 */
final class Monitor
{
    private ?Closure $output = null;
    private array $counts = [];
    private array $silentJobs = [];
    private DiagnosticContext $diagnostics;
    private const LABELS = ['started' => '启动', 'published' => '投递确认', 'publish_failed' => '投递未确认',
        'claimed' => '领取', 'succeeded' => '成功', 'released' => '主动重投', 'retried' => '重试',
        'failed' => '最终失败', 'lease_lost' => '处理权丢失', 'timed_out' => '执行超时',
        'error' => '异常', 'stopped' => '退出', 'dispatch' => '邮件调度', 'dispatch_deferred' => '调度推迟'];
    private const FIELDS = ['consumer', 'job_id', 'message_id', 'job', 'attempt', 'max_tries', 'elapsed_ms',
        'retry_number', 'max_retries', 'delay_seconds', 'timeout_seconds', 'exception', 'reason', 'queued', 'deferred', 'skipped',
        'failed', 'claimed', 'succeeded', 'released', 'retried', 'lease_lost', 'timed_out', 'errors',
        'params', 'params_truncated', 'error_code', 'error_message', 'exception_file', 'exception_line',
        'failure_stage', 'smtp_code', 'cause_exception', 'cause_file', 'cause_line'];

    /**
     * 创建运行日志。
     * @param LoggerInterface|null $logger 可选的持久化日志通道
     * @param array<string> $silentJobs 隐藏日常运行日志的处理器，异常日志始终保留
     * @param array<string> $parameterKeys 可记录的参数键或路径
     * @param int $maxParameterBytes 参数 JSON 字节上限
     */
    public function __construct(private ?LoggerInterface $logger = null, array $silentJobs = [],
        array $parameterKeys = DiagnosticContext::DEFAULT_PARAMETER_KEYS, int $maxParameterBytes = 2048)
    {
        $this->diagnostics = new DiagnosticContext($parameterKeys, $maxParameterBytes);
        foreach ($silentJobs as $job) {
            if (!is_string($job) || !Payload::validJob($job)) continue;
            $job = ltrim($job, '\\');
            $this->silentJobs[str_contains($job, '@') ? $job : $job . '@fire'] = true;
        }
    }

    /**
     * 替换控制台输出端，返回旧值以便命令执行结束后恢复。
     * @param Closure|null $output 接收一行文本的输出函数
     * @return Closure|null 原输出函数
     */
    public function setOutput(?Closure $output): ?Closure
    {
        $previous = $this->output;
        $this->output = $output;
        return $previous;
    }

    /**
     * 开始一次消费循环并清零进程统计。
     * @param string $queue 队列名称
     * @param string $consumer 实际消费者名称
     * @return void
     */
    public function start(string $queue, string $consumer): void
    {
        $this->counts = array_fill_keys(['claimed', 'succeeded', 'released', 'retried', 'failed', 'lease_lost', 'timed_out', 'errors'], 0);
        $this->record('started', $queue, ['consumer' => $consumer]);
    }

    /**
     * 输出退出原因及本次循环统计；强制超时时只写控制台，避免信号处理器执行磁盘或网络日志。
     * @param string $queue 队列名称
     * @param string $consumer 消费者名称
     * @param string $reason 退出原因
     * @param bool $persist 是否调用持久化日志
     * @return void
     */
    public function stop(string $queue, string $consumer, string $reason, bool $persist = true): void
    {
        $this->record('stopped', $queue, ['consumer' => $consumer, 'reason' => $reason] + $this->counts, $persist);
    }

    /**
     * 写入带时间、时区、队列和进程编号的安全单行日志；日志故障不影响业务状态转换。
     * @param string $event 预定义运行事件
     * @param string $queue 队列名称
     * @param array<string,mixed> $context 白名单元数据，其他字段自动丢弃
     * @param bool $persist 是否写持久化通道
     * @return void
     */
    public function record(string $event, string $queue, array $context = [], bool $persist = true): void
    {
        if (!isset(self::LABELS[$event])) return;
        $counter = $event === 'error' ? 'errors' : $event;
        if (array_key_exists($counter, $this->counts)) $this->counts[$counter]++;
        // 静默只影响输出；实际领取与处理结果仍进入进程统计。
        if ($this->isSilent($event, $context)) return;
        if (array_key_exists('params', $context)) {
            // 输入始终视作原始参数，即使是字符串也不能绕过筛选策略。
            $context = array_replace($context, $this->diagnostics->parameters($context['params']));
        }
        $label = self::LABELS[$event];
        $attempt = $context['attempt'] ?? null;
        $maxTries = $context['max_tries'] ?? null;
        if ($event === 'retried' && is_int($attempt) && is_int($maxTries) && $attempt >= 1 && $attempt < $maxTries) {
            // 此事件发生在本次失败并已重投之后，attempt 对应即将安排的重试序号。
            // 最大领取次数包含首次执行，所以可用的重试次数比 max_tries 少一次。
            $context['retry_number'] = $attempt;
            $context['max_retries'] = $maxTries - 1;
            $label .= ' 第' . $attempt . '次/最多' . ($maxTries - 1) . '次';
        }
        $line = '[' . (new DateTimeImmutable())->format('Y-m-d H:i:s.vP') . '][' . $label . ']';
        $values = ['queue' => $queue, 'pid' => getmypid()] + array_intersect_key($context, array_flip(self::FIELDS));
        foreach ($values as $key => $value) {
            if (!is_scalar($value)) continue;
            $value = (string) $value;
            if ($key === 'params') { $line .= ' params=' . $value; continue; }
            if (strlen($value) > 2048) $value = substr($value, 0, 2048) . '[TRUNCATED]';
            if ($key === 'job' && !str_contains($value, '@')) $value .= '@fire';
            // 转义空白和控制字符，防止异常队列名或 ID 伪造多行日志。
            $value = preg_match('/[\s\x00-\x1f\x7f<>]/u', $value) !== 0 ?
                json_encode($value, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE) : $value;
            $line .= ' ' . $key . '=' . $value;
        }
        try { ($this->output)?->__invoke($line); } catch (Throwable) { /* 输出失败不改变任务结果。 */ }
        if (!$persist) return;
        $level = in_array($event, ['failed', 'error', 'timed_out'], true) ? 'error' :
            (in_array($event, ['retried', 'lease_lost', 'publish_failed', 'dispatch_deferred'], true) ? 'warning' : 'info');
        try { $this->logger?->log($level, $line); } catch (Throwable) { /* 日志故障不能引发重复投递或确认失败。 */ }
    }

    /**
     * 隐藏指定处理器的例行日志；错误、重试、最终失败、超时和处理权丢失始终可见。
     * @param string $event 运行事件
     * @param array<string,mixed> $context 运行元数据
     * @return bool 是否同时跳过控制台及持久化输出
     */
    private function isSilent(string $event, array $context): bool
    {
        $job = $context['job'] ?? null;
        if (!is_string($job)) return false;
        $job = ltrim($job, '\\');
        if (!isset($this->silentJobs[str_contains($job, '@') ? $job : $job . '@fire'])) return false;
        if (in_array($event, ['published', 'claimed', 'succeeded', 'dispatch'], true)) return true;
        // 调度消息遇到容量限制时会稍后再投；连接异常或 ID 冲突等真实故障继续输出。
        return in_array($event, ['publish_failed', 'dispatch_deferred'], true) &&
            ($context['exception'] ?? null) === QueueFull::class;
    }
}
