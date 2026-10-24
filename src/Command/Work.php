<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Command;

use InvalidArgumentException;
use think\console\Command;
use think\console\Input;
use think\console\input\Option;
use think\console\Output;
use ThinkQueue\Streams\Queue;
use ThinkQueue\Streams\Monitor;
use ThinkQueue\Streams\DiagnosticContext;
use ThinkQueue\Streams\Event\Looping;
use ThinkQueue\Streams\Worker;

final class Work extends Command
{
    /**
     * 定义消费者选项，所有数字在执行边界统一验证和转换。
     *
     * @return void
     */
    protected function configure(): void
    {
        $this->setName('queue:work')->setDescription('消费 Redis Streams 任务')
            ->addOption('queue', null, Option::VALUE_REQUIRED, '队列名称')
            ->addOption('once', null, Option::VALUE_NONE, '只领取一次')
            ->addOption('tries', null, Option::VALUE_REQUIRED, '最大领取次数', 3)
            ->addOption('timeout', null, Option::VALUE_REQUIRED, '任务执行超时秒数', 60)
            ->addOption('delay', null, Option::VALUE_REQUIRED, '重试初始延迟秒数', 3)
            ->addOption('memory', null, Option::VALUE_REQUIRED, '进程内存限制 MB', 128)
            ->addOption('max-jobs', null, Option::VALUE_REQUIRED, '达到处理数量后退出', 1000)
            ->addOption('max-time', null, Option::VALUE_REQUIRED, '进程最长运行秒数', 3600);
    }

    /**
     * 运行可优雅停止的消费循环，使用 Supervisor 或 systemd 在退出后拉起。
     *
     * @param Input $input 命令输入
     * @param Output $output 命令输出
     * @return int 退出码
     */
    protected function execute(Input $input, Output $output): int
    {
        $options = [];
        foreach (['tries', 'timeout', 'delay', 'memory', 'max-jobs', 'max-time'] as $name) {
            $value = filter_var($input->getOption($name), FILTER_VALIDATE_INT);
            if ($value === false || $value < ($name === 'delay' ? 0 : 1)) {
                throw new InvalidArgumentException("--{$name} 必须是有效整数。");
            }
            $options[$name] = $value;
        }
        if (!extension_loaded('pcntl')) {
            throw new \RuntimeException('queue:work 要求 pcntl 扩展。');
        }
        $queue = $this->app->make(Queue::class);
        $monitor = $this->app->make(Monitor::class);
        $worker = new Worker($queue, null, $monitor);
        $queueName = $input->getOption('queue') ?: $queue->defaultQueue();
        $transport = $queue->transport();
        $restart = $transport->restartToken();
        $start = hrtime(true);
        $processed = 0;
        $stop = false;
        $reason = 'exception';
        $previousOutput = $monitor->setOutput(static function (string $line) use ($output): void {
            $output->writeln($line, Output::OUTPUT_RAW);
        });
        $monitor->start($queueName, $transport->consumerName());
        $previousAsync = pcntl_async_signals(true);
        $previousTerm = pcntl_signal_get_handler(SIGTERM);
        $previousInt = pcntl_signal_get_handler(SIGINT);
        $stopHandler = static function () use (&$stop): void { $stop = true; };
        pcntl_signal(SIGTERM, $stopHandler);
        pcntl_signal(SIGINT, $stopHandler);
        try {
            do {
                $this->app->event->trigger(new Looping($queue, $queueName, (bool) $input->getOption('once')));
                if ($worker->runNextJob($queueName, $options['tries'], $options['timeout'], $options['delay'])) {
                    $processed++;
                } elseif ($transport->blockMilliseconds() === 0) {
                    usleep(100000);
                }
                $reason = match (true) {
                    $stop => 'signal',
                    (bool) $input->getOption('once') => 'once',
                    $processed >= $options['max-jobs'] => 'max_jobs',
                    (hrtime(true) - $start) / 1000000000 >= $options['max-time'] => 'max_time',
                    memory_get_usage(true) >= $options['memory'] * 1048576 => 'memory',
                    $transport->restartToken() !== $restart => 'restart',
                    default => '',
                };
                if ($reason !== '') break;
            } while (!$stop);
        } catch (\Throwable $exception) {
            $reason = 'exception';
            $monitor->record('error', $queueName, ['consumer' => $transport->consumerName()] + DiagnosticContext::exception($exception));
            throw $exception;
        } finally {
            pcntl_signal(SIGTERM, $previousTerm);
            pcntl_signal(SIGINT, $previousInt);
            pcntl_async_signals($previousAsync);
            try {
                $transport->closeConsumer();
            } catch (\Throwable $exception) {
                $monitor->record('error', $queueName, ['consumer' => $transport->consumerName(),
                    'reason' => 'consumer_cleanup'] + DiagnosticContext::exception($exception));
            }
            $monitor->stop($queueName, $transport->consumerName(), $reason ?: 'signal');
            $monitor->setOutput($previousOutput);
        }
        return 0;
    }
}
