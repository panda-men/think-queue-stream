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
        $this->app->bind(FailedJobStore::class, function (): FailedJobStore {
            return new FailedJobStore(function (): \PDO {
                $name = $this->app->config->get('queue_streams.failed_connection');
                $source = $this->app->db->connect($name);
                $config = $source->getConfig();
                // 独立连接，不复用业务 PDO，也不参与业务层尚未提交的事务。
                $config['params'][\PDO::ATTR_PERSISTENT] = false;
                $class = $source::class;
                $connection = new $class($config);
                $connection->setDb($this->app->db);
                $connection->query('SELECT 1', [], true);
                return $connection->getPdo();
            }, (string) $this->app->config->get('queue_streams.failed_table', 'failed_jobs'));
        });
        $this->app->bind(Monitor::class, function (): Monitor {
            try {
                $channel = $this->app->config->get('queue_streams.log_channel');
                $logger = $channel ? $this->app->log->channel($channel) : $this->app->log;
            } catch (\Throwable) {
                // 日志通道初始化失败时仍允许消费，命令的标准输出继续提供运行信息。
                $logger = null;
            }
            return new Monitor($logger, (array) $this->app->config->get('queue_streams.log_silent_jobs', []));
        });
        $this->app->bind(Queue::class, function (): Queue {
            $config = $this->app->config->get('queue_streams', []);
            return new Queue(new Transport(RedisConnection::connect($config), $config, $this->app->make(FailedJobStore::class)), $this->app,
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
