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

final class Manage extends Command
{
    /**
     * 定义失败任务管理与重启命令。
     *
     * @return void
     */
    protected function configure(): void
    {
        $this->setName('queue:manage')->setDescription('查看、重放、删除失败任务，或重启消费者')
            ->addArgument('action', Argument::REQUIRED, 'failed、retry、forget 或 restart')
            ->addArgument('id', Argument::OPTIONAL, '失败记录的 Stream ID')
            ->addOption('queue', null, Option::VALUE_REQUIRED, '队列名称')
            ->addOption('start', null, Option::VALUE_REQUIRED, '失败记录起始 ID，支持 (ID', '-')
            ->addOption('limit', null, Option::VALUE_REQUIRED, '查询数量，最多 1000', 100);
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
        if (!in_array($action, ['failed', 'retry', 'forget', 'restart'], true)) {
            throw new InvalidArgumentException('action 必须为 failed、retry、forget 或 restart。');
        }
        $id = $input->getArgument('id');
        if (in_array($action, ['retry', 'forget'], true) &&
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
        if ($action === 'failed') {
            $limit = filter_var($input->getOption('limit'), FILTER_VALIDATE_INT);
            if ($limit === false) {
                throw new InvalidArgumentException('limit 必须为整数。');
            }
            // JSON_LINES 方便脚本读取，同时避免控制台标签解释消息中的错误文本。
            foreach ($transport->failures($queueName, $limit, (string) $input->getOption('start')) as $failureId => $record) {
                $output->writeln(json_encode(['failure_id' => $failureId] + $record,
                    JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE), Output::OUTPUT_RAW);
            }
            return 0;
        }
        $result = $action === 'retry' ? $transport->retryFailed($queueName, $id) : $transport->forgetFailed($queueName, $id);
        $output->writeln($result ? '操作成功。' : '失败记录不存在。');
        return $result ? 0 : 1;
    }
}
