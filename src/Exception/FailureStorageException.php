<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Exception;

/** Redis 已结算为最终失败，但数据库归档尚未确认；消费者必须退出。 */
final class FailureStorageException extends \RuntimeException
{
}
