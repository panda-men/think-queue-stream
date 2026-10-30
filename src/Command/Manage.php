<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Command;

use InvalidArgumentException;
use think\console\Command;
use think\console\Input;
use think\console\input\Argument;
use think\console\input\Option;
use think\console\Output;
use ThinkQueue\Streams\Queue;
use ThinkQueue\Streams\FailedJobStore;

final class Manage extends Command
{
    /**
     * 定义失败任务管理与重启命令。
     *
     * @return void
     */
    protected function configure(): void
    {
        $this->setName('queue:manage')->setDescription('查看和维护失败、隔离记录及消费者')
            ->addArgument('action', Argument::REQUIRED, 'install、migrate-failed、failed、quarantined、retry、forget、forget-quarantine、prune、cleanup-consumer 或 restart')
            ->addArgument('id', Argument::OPTIONAL, '记录 Stream ID 或消费者名称')
            ->addOption('queue', null, Option::VALUE_REQUIRED, '队列名称')
            ->addOption('start', null, Option::VALUE_REQUIRED, '失败记录起始 ID，支持 (ID', '-')
            ->addOption('limit', null, Option::VALUE_REQUIRED, '查询或清理数量，最多 1000', 100)
            ->addOption('kind', null, Option::VALUE_REQUIRED, 'prune 的记录类型：failed/quarantined', 'failed')
            ->addOption('older-than', null, Option::VALUE_REQUIRED, 'prune 的保留秒数，必须显式指定')
            ->addOption('dry-run', null, Option::VALUE_NONE, '只预览 prune 候选，不删除')
            ->addOption('idle-ms', null, Option::VALUE_REQUIRED, 'cleanup-consumer 的最小心跳年龄');
    }

    /**
     * 执行指定管理动作，重放与删除仅针对明确的记录 ID。
     *
     * @param Input $input 命令输入
     * @param Output $output 命令输出
     * @return int 退出码，记录不存在时为 1
     */
    protected function execute(Input $input, Output $output): int
    {
        $action = $input->getArgument('action');
        if (!in_array($action, ['install', 'migrate-failed', 'failed', 'quarantined', 'retry', 'forget', 'forget-quarantine', 'prune', 'cleanup-consumer', 'restart'], true)) {
            throw new InvalidArgumentException('未知管理动作。');
        }
        if ($action === 'install') {
            $this->app->make(FailedJobStore::class)->install();
            $output->writeln('失败记录表已创建。');
            return 0;
        }
        $id = $input->getArgument('id');
        if (in_array($action, ['retry', 'forget', 'forget-quarantine'], true) &&
            (!is_string($id) || !preg_match('/\A\d+-\d+\z/D', $id))) {
            throw new InvalidArgumentException('必须指定有效的失败记录 Stream ID。');
        }
        $queue = $this->app->make(Queue::class);
        $transport = $queue->transport();
        $queueName = $input->getOption('queue') ?: $queue->defaultQueue();
        if ($action === 'restart') {
            $transport->restartToken(true);
            $output->writeln('已更新重启标记；消费者将在当前任务结束后退出。');
            return 0;
        }
        if (in_array($action, ['migrate-failed', 'failed', 'quarantined', 'prune'], true)) {
            $limit = filter_var($input->getOption('limit'), FILTER_VALIDATE_INT);
            if ($limit === false) {
                throw new InvalidArgumentException('limit 必须为整数。');
            }
            if ($action === 'migrate-failed') {
                $migrated = $transport->migrateFailed($queueName, $limit);
                $output->writeln(json_encode(['migrated' => $migrated,
                    'remaining' => $transport->stats($queueName)['failed_pending']], JSON_THROW_ON_ERROR), Output::OUTPUT_RAW);
                return 0;
            }
            if ($action === 'prune') {
                $seconds = filter_var($input->getOption('older-than'), FILTER_VALIDATE_INT);
                if ($seconds === false || $seconds < 1) {
                    throw new InvalidArgumentException('prune 必须显式指定正整数 --older-than 秒数。');
                }
                $dryRun = (bool) $input->getOption('dry-run');
                $ids = $transport->prune($queueName, (string) $input->getOption('kind'), $seconds, $limit, $dryRun);
                $output->writeln(json_encode(['dry_run' => $dryRun, 'count' => count($ids), 'ids' => $ids],
                    JSON_THROW_ON_ERROR), Output::OUTPUT_RAW);
                return 0;
            }
            $records = $action === 'failed' ? $transport->failures($queueName, $limit, (string) $input->getOption('start')) :
                $transport->quarantined($queueName, $limit, (string) $input->getOption('start'));
            foreach ($records as $recordId => $record) {
                $output->writeln(json_encode([$action === 'failed' ? 'failure_id' : 'quarantine_id' => $recordId] + $record,
                    JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE), Output::OUTPUT_RAW);
            }
            return 0;
        }
        if ($action === 'cleanup-consumer') {
            $idle = filter_var($input->getOption('idle-ms'), FILTER_VALIDATE_INT);
            if (!is_string($id) || $idle === false) {
                throw new InvalidArgumentException('必须指定消费者名称和整数 --idle-ms。');
            }
            $removed = $transport->cleanupConsumer($queueName, $id, $idle);
            $output->writeln($removed ? '已清理消费者元数据。' : '未清理：消费者不存在、心跳未过期或持有 Pending。');
            return $removed ? 0 : 1;
        }
        $result = match ($action) {
            'retry' => $transport->retryFailed($queueName, $id),
            'forget-quarantine' => $transport->forgetQuarantined($queueName, $id),
            default => $transport->forgetFailed($queueName, $id),
        };
        $output->writeln($result ? '操作成功。' : '失败记录不存在。');
        return $result ? 0 : 1;
    }
}
