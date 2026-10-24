<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Facade;

/**
 * ThinkPHP 静态队列入口。
 *
 * @mixin \ThinkQueue\Streams\Queue
 */
final class Queue extends \think\Facade
{
    /**
     * 获取容器绑定名称。
     *
     * @return string 队列服务类名
     */
    protected static function getFacadeClass(): string
    {
        return \ThinkQueue\Streams\Queue::class;
    }
}
