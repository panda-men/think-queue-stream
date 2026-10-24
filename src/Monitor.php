<?php
declare(strict_types=1);

namespace ThinkQueue\Streams;

use Closure;
use DateTimeImmutable;
use Psr\Log\LoggerInterface;
use Throwable;

/** 统一的队列运行日志；只接收运行元数据，不记录业务载荷或异常原文。 */
final class Monitor
{
    private ?Closure $output = null;
    private array $counts = [];
    private const LABELS = ['started' => '启动', 'published' => '投递确认', 'publish_failed' => '投递未确认',
        'claimed' => '领取', 'succeeded' => '成功', 'released' => '主动重投', 'retried' => '重试',
        'failed' => '最终失败', 'lease_lost' => '处理权丢失', 'timed_out' => '执行超时',
        'error' => '异常', 'stopped' => '退出', 'dispatch' => '邮件调度', 'dispatch_deferred' => '调度推迟'];
    private const FIELDS = ['consumer', 'job_id', 'message_id', 'job', 'attempt', 'max_tries', 'elapsed_ms',
        'delay_seconds', 'timeout_seconds', 'exception', 'reason', 'queued', 'deferred', 'skipped',
        'failed', 'claimed', 'succeeded', 'released', 'retried', 'lease_lost', 'timed_out', 'errors'];

    /**
     * 创建运行日志。
     * @param LoggerInterface|null $logger 可选的持久化日志通道
     */
    public function __construct(private ?LoggerInterface $logger = null)
    {
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
        $line = '[' . (new DateTimeImmutable())->format('Y-m-d H:i:s.vP') . '][' . self::LABELS[$event] . ']';
        $values = ['queue' => $queue, 'pid' => getmypid()] + array_intersect_key($context, array_flip(self::FIELDS));
        foreach ($values as $key => $value) {
            if (!is_scalar($value)) continue;
            $value = (string) $value;
            if ($key === 'job' && !str_contains($value, '@')) $value .= '@fire';
            // 转义空白和控制字符，防止异常队列名或 ID 伪造多行日志。
            $value = preg_match('/[\s\x00-\x1f\x7f<>]/u', $value) ?
                json_encode($value, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE) : $value;
            $line .= ' ' . $key . '=' . $value;
        }
        try { ($this->output)?->__invoke($line); } catch (Throwable) { /* 输出失败不改变任务结果。 */ }
        if (!$persist) return;
        $level = in_array($event, ['failed', 'error', 'timed_out'], true) ? 'error' :
            (in_array($event, ['retried', 'lease_lost', 'publish_failed', 'dispatch_deferred'], true) ? 'warning' : 'info');
        try { $this->logger?->log($level, $line); } catch (Throwable) { /* 日志故障不能引发重复投递或确认失败。 */ }
    }
}
