<?php
declare(strict_types=1);

namespace ThinkQueue\Streams;

use ThinkQueue\Streams\Command\Manage;
use ThinkQueue\Streams\Command\Work;
use ThinkQueue\Streams\Command\Stats;

final class Service extends \think\Service
{
    /**
     * 注册队列服务，延迟创建 Redis 连接。
     *
     * @return void
     */
    public function register(): void
    {
        $this->app->bind(Monitor::class, function (): Monitor {
            try {
                $channel = $this->app->config->get('queue_streams.log_channel');
                $logger = $channel ? $this->app->log->channel($channel) : $this->app->log;
            } catch (\Throwable) {
                // 日志通道初始化失败时仍允许消费，命令的标准输出继续提供运行信息。
                $logger = null;
            }
            return new Monitor($logger, (array) $this->app->config->get('queue_streams.log_silent_jobs', []),
                (array) $this->app->config->get('queue_streams.log_parameter_keys', DiagnosticContext::DEFAULT_PARAMETER_KEYS),
                (int) $this->app->config->get('queue_streams.log_parameter_max_bytes', 2048));
        });
        $this->app->bind(Queue::class, function (): Queue {
            $config = $this->app->config->get('queue_streams', []);
            return new Queue(new Transport(RedisConnection::connect($config), $config), $this->app,
                (string) ($config['queue'] ?? 'default'), null, $this->app->make(Monitor::class));
        });
    }

    /**
     * 注册消费者与失败任务管理命令。
     *
     * @return void
     */
    public function boot(): void
    {
        $this->commands([Work::class, Manage::class, Stats::class]);
    }
}
