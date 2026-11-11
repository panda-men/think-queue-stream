<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Command;

use RuntimeException;
use think\console\Command;
use think\console\Input;
use think\console\input\Option;
use think\console\Output;
use ThinkQueue\Streams\Supervisor;

final class Supervise extends Command
{
    public const NAME = 'stream:supervise';

    protected function configure(): void
    {
        $this->setName(self::NAME)->setDescription('统一管理多队列 Worker，支持自动重启与配置重载')
            ->addOption('reload', null, Option::VALUE_NONE, '通知当前项目主进程重新读取配置并更新 Worker');
    }

    protected function execute(Input $input, Output $output): int
    {
        try {
            return $this->supervise($input, $output);
        } catch (RuntimeException $exception) {
            // ThinkPHP 的异常终端渲染也使用 proc_close；直接输出可报告被禁用的进程函数。
            $output->writeln(self::NAME . ': ' . $exception->getMessage(), Output::OUTPUT_RAW);
            return 1;
        }
    }

    private function supervise(Input $input, Output $output): int
    {
        $pidFile = $this->app->getRuntimePath() . 'queue-streams-supervisor.pid';
        if ($input->getOption('reload')) {
            Supervisor::requestReload($pidFile);
            $output->writeln('已发送配置重载请求，执行结果请查看主进程日志。');
            return 0;
        }
        $supervisor = new Supervisor($this->app->getRootPath() . 'think',
            $this->app->getConfigPath() . 'queue_streams.php', $pidFile,
            static function (string $line) use ($output): void { $output->writeln($line, Output::OUTPUT_RAW); });
        return $supervisor->run();
    }
}
