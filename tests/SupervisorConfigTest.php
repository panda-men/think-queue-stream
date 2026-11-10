<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Tests;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ThinkQueue\Streams\SupervisorConfig;

final class SupervisorConfigTest extends TestCase
{
    public function testMultipleQueuesAndPausedQueueShareConcurrencyBudget(): void
    {
        $config = SupervisorConfig::fromArray(['supervisor' => ['max_processes' => 3, 'workers' => [
            'mail' => ['processes' => '2', 'delay' => 0], 'sync' => ['processes' => 1], 'paused' => ['processes' => 0],
        ]]]);
        self::assertSame(['mail:0', 'mail:1', 'sync:0'], array_keys($config->slots()));
        self::assertSame(60, $config->workers['mail']['timeout']);
        self::assertSame(0, $config->workers['mail']['delay']);
        self::assertSame([], SupervisorConfig::fromArray([])->slots());
    }

    #[DataProvider('invalidConfigurations')]
    public function testInvalidConfigurationIsRejected(array $config): void
    {
        $this->expectException(InvalidArgumentException::class);
        SupervisorConfig::fromArray($config);
    }

    public static function invalidConfigurations(): iterable
    {
        yield 'type' => [['supervisor' => 'invalid']];
        yield 'workers type' => [['supervisor' => ['workers' => false]]];
        yield 'null supervisor' => [['supervisor' => null]];
        yield 'null workers' => [['supervisor' => ['workers' => null]]];
        yield 'null budget' => [['supervisor' => ['max_processes' => null]]];
        yield 'unknown supervisor option' => [['supervisor' => ['max_process' => 2]]];
        yield 'invalid budget' => [['supervisor' => ['max_processes' => 0]]];
        yield 'invalid grace' => [['supervisor' => ['stop_wait_seconds' => 0]]];
        yield 'invalid backoff' => [['supervisor' => ['restart_delay' => 10, 'restart_max_delay' => 5]]];
        foreach ([
            'negative processes' => ['processes' => -1], 'boolean' => ['processes' => true],
            'float' => ['processes' => 1.5], 'unknown worker option' => ['max-jobs' => 1],
            'zero tries' => ['tries' => 0], 'delay too large' => ['delay' => 3601],
            'timeout at lease' => ['timeout' => 90], 'timeout overflow' => ['timeout' => PHP_INT_MAX],
            'over budget' => ['processes' => 33],
        ] as $name => $options) yield $name => [['supervisor' => ['workers' => ['mail' => $options]]]];
        yield 'queue name' => [['supervisor' => ['workers' => ['mail,sync' => []]]]];
        yield 'missing options' => [['supervisor' => ['workers' => ['mail' => 1]]]];
        yield 'total budget' => [['supervisor' => ['max_processes' => 3, 'workers' => ['a' => ['processes' => 2], 'b' => ['processes' => 2]]]]];
    }
}
