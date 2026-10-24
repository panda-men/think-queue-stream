<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Exception;

/** 活动任务 ID 已被占用，且不能作为同一任务去重。 */
final class JobIdConflict extends \RuntimeException
{
}
