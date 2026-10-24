<?php
declare(strict_types=1);

namespace ThinkQueue\Streams;

use InvalidArgumentException;
use Psr\Log\LoggerInterface;
use RuntimeException;
use Throwable;
use ThinkQueue\Streams\Exception\LeaseLost;

final class Worker
{
    private Monitor $monitor;

    /**
     * 创建消费者。
     *
     * @param Queue $queue 队列入口
     * @param LoggerInterface|null $logger 业务异常日志
     * @param Monitor|null $monitor 共享运行日志
     */
    public function __construct(private Queue $queue, ?LoggerInterface $logger = null, ?Monitor $monitor = null)
    {
        $this->monitor = $monitor ?? new Monitor($logger);
    }

    /**
     * 执行一个任务；未调用 delete/release 的任务按重试处理，保持手动确认习惯。
     *
     * @param string|null $queue 队列名称
     * @param int $tries 最大领取次数
     * @param int $timeout 执行超时秒数
     * @param int $delay 重试初始延迟秒数，后续按指数退避，最多一小时
     * @return bool 是否领取到任务
     */
    public function runNextJob(?string $queue = null, int $tries = 3, int $timeout = 60, int $delay = 3): bool
    {
        if ($tries < 1 || $timeout < 1 || $delay < 0 || $delay > 3600) {
            throw new InvalidArgumentException('tries、timeout 必须为正整数，delay 必须在 0 至 3600 秒之间。');
        }
        if ($timeout * 1000 >= $this->queue->transport()->retryAfterMs()) {
            throw new InvalidArgumentException('timeout 必须小于 retry_after_ms。');
        }
        if (!extension_loaded('pcntl')) {
            throw new RuntimeException('第一版 Worker 要求 CLI pcntl 扩展，以强制执行任务超时。');
        }
        $job = $this->queue->pop($queue);
        if ($job === null) {
            return false;
        }
        $queueName = $queue ?? $this->queue->defaultQueue();
        $consumer = $this->queue->transport()->consumerName();
        $maxTries = $job->payload('max_tries') ?? $tries;
        $jobTimeout = $job->payload('timeout') ?? $timeout;
        $context = ['consumer' => $consumer, 'job_id' => $job->getJobId(), 'message_id' => $job->getMessageId(),
            'job' => $job->payload('job'), 'attempt' => $job->attempts(), 'max_tries' => $maxTries,
            'params' => $job->payload('data')];
        $start = hrtime(true);
        $this->monitor->record('claimed', $queueName, $context);
        $previousAlarm = pcntl_signal_get_handler(SIGALRM);
        $previousAsync = pcntl_async_signals(true);
        // 超时只输出元数据并退出，保留 PEL；信号处理器不执行 Redis 或业务数据库操作。
        pcntl_signal(SIGALRM, function () use ($queueName, $consumer, $context, $start, $jobTimeout): void {
            $this->monitor->record('timed_out', $queueName, $context + ['timeout_seconds' => $jobTimeout,
                'elapsed_ms' => (int) ((hrtime(true) - $start) / 1000000)], false);
            $this->monitor->stop($queueName, $consumer, 'timeout', false);
            exit(124);
        });
        pcntl_alarm($jobTimeout);
        $error = null;
        try {
            if ($jobTimeout * 1000 >= $this->queue->transport()->retryAfterMs()) {
                $error = new InvalidArgumentException('任务 timeout 必须小于 retry_after_ms。');
                $job->fail($error);
            } elseif ($job->attempts() > $maxTries) {
                $error = new RuntimeException('任务已超过最大尝试次数。');
                $job->fail($error);
            } else {
                $job->fire();
                if (!$job->isDeletedOrReleased()) {
                    throw new RuntimeException('任务返回时尚未调用 delete() 或 release()。');
                }
            }
        } catch (LeaseLost $exception) {
            $error = $exception;
        } catch (Throwable $exception) {
            $error = $exception;
            if (!$job->isDeletedOrReleased()) {
                if ($job->attempts() >= $maxTries) {
                    $job->fail($exception);
                } else {
                    $backoff = min(3600, $delay * (2 ** min(20, $job->attempts() - 1)));
                    if ($backoff > 0 && $this->queue->transport()->retryJitter()) {
                        $backoff = random_int((int) ceil($backoff / 2), (int) $backoff);
                    }
                    $job->release((int) $backoff);
                }
            }
        } finally {
            pcntl_alarm(0);
            pcntl_signal(SIGALRM, $previousAlarm);
            pcntl_async_signals($previousAsync);
            $context['elapsed_ms'] = (int) ((hrtime(true) - $start) / 1000000);
            if ($error !== null) $context = array_replace($context, DiagnosticContext::exception($error));
            if ($job->hasFailed()) {
                $event = 'failed';
            } elseif ($job->isReleased()) {
                $event = $error === null ? 'released' : 'retried';
                $context['delay_seconds'] = $job->releaseDelay();
            } elseif ($error instanceof LeaseLost) {
                $event = 'lease_lost';
            } elseif ($error !== null || !$job->isDeleted()) {
                $event = 'error';
                $context['reason'] = $job->isDeleted() ? 'exception_after_ack' : 'unsettled';
            } else {
                $event = 'succeeded';
            }
            $this->monitor->record($event, $queueName, $context);
        }
        return true;
    }
}
