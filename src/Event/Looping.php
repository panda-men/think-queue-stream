<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Event;

use ThinkQueue\Streams\Queue;

/** 每次领取之前触发；监听器应短时、有界执行，不依赖业务载荷。 */
final class Looping
{
    /**
     * 创建消费循环事件。
     * @param Queue $queue 当前队列入口
     * @param string $queueName 本次消费的队列名称
     * @param bool $once 是否为单次领取模式
     */
    public function __construct(public readonly Queue $queue, public readonly string $queueName,
        public readonly bool $once)
    {
    }
}
