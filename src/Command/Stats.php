<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Command;

use InvalidArgumentException;
use think\console\Command;
use think\console\Input;
use think\console\input\Option;
use think\console\Output;
use ThinkQueue\Streams\Queue;

final class Stats extends Command
{
    protected function configure(): void
    {
        $this->setName('queue:stats')->setDescription('查看任务、Pending 和消费者心跳快照')
            ->addOption('queue', null, Option::VALUE_REQUIRED, '队列名称')
            ->addOption('json', null, Option::VALUE_NONE, '输出 JSON')
            ->addOption('limit', null, Option::VALUE_REQUIRED, '心跳分页数量，最多 1000', 100)
            ->addOption('offset', null, Option::VALUE_REQUIRED, '心跳分页偏移', 0);
    }

    protected function execute(Input $input, Output $output): int
    {
        $limit = filter_var($input->getOption('limit'), FILTER_VALIDATE_INT);
        $offset = filter_var($input->getOption('offset'), FILTER_VALIDATE_INT);
        if ($limit === false || $offset === false) {
            throw new InvalidArgumentException('limit、offset 必须为整数。');
        }
        $queue = $this->app->make(Queue::class);
        $stats = $queue->transport()->stats($input->getOption('queue') ?: $queue->defaultQueue(), $limit, $offset);
        if ($input->getOption('json')) {
            $output->writeln(json_encode($stats, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE), Output::OUTPUT_RAW);
            return 0;
        }
        foreach (['queue', 'group', 'active', 'waiting', 'pending', 'delayed', 'failed', 'failed_pending', 'quarantined', 'quarantined_pending', 'consumer_count'] as $key) {
            $output->writeln($key . ': ' . $stats[$key], Output::OUTPUT_RAW);
        }
        $output->writeln('消费者心跳与 Pending 详情请使用 --json。');
        return 0;
    }
}
