<?php
declare(strict_types=1);

namespace ThinkQueue\Streams;

use ThinkQueue\Streams\Command\Manage;
use ThinkQueue\Streams\Command\Work;

final class Service extends \think\Service
{
    /**
     * 注册队列服务，延迟创建 Redis 连接。
     *
     * @return void
     */
    public function register(): void
    {
        $this->app->bind(Queue::class, function (): Queue {
            $config = $this->app->config->get('queue_streams', []);
            return new Queue(new Transport(RedisConnection::connect($config), $config), $this->app,
                (string) ($config['queue'] ?? 'default'));
        });
    }

    /**
     * 注册消费者与失败任务管理命令。
     *
     * @return void
     */
    public function boot(): void
    {
        $this->commands([Work::class, Manage::class]);
    }
}
