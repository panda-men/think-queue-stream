<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Benchmark;

require dirname(__DIR__) . '/vendor/autoload.php';

use Redis;
use think\App;
use ThinkQueue\Streams\Job;
use ThinkQueue\Streams\Queue;
use ThinkQueue\Streams\RedisConnection;
use ThinkQueue\Streams\Transport;
use ThinkQueue\Streams\Worker;

final class BenchmarkJob
{
    public static array $waits = [];

    public function __construct(private Redis $redis) {}

    public function fire(Job $job, array $data): void
    {
        self::$waits[] = (hrtime(true) - $data['enqueued_at_ns']) / 1000000;
        $this->redis->hIncrBy($data['counter'], $job->getJobId(), 1);
        $job->delete();
    }
}

$options = getopt('', ['jobs:', 'payload-bytes:']);
$jobs = filter_var($options['jobs'] ?? 1000, FILTER_VALIDATE_INT);
$bytes = filter_var($options['payload-bytes'] ?? 256, FILTER_VALIDATE_INT);
if ($jobs === false || $jobs < 1 || $jobs > 100000 || $bytes === false || $bytes < 0 || $bytes > 65536) {
    fwrite(STDERR, "jobs 必须为 1 至 100000，payload-bytes 必须为 0 至 65536。\n");
    exit(1);
}
$config = ['host' => getenv('REDIS_HOST') ?: '127.0.0.1', 'port' => (int) (getenv('REDIS_PORT') ?: 6379),
    'password' => getenv('REDIS_PASSWORD') === false ? null : getenv('REDIS_PASSWORD'),
    'prefix' => 'benchmark-' . bin2hex(random_bytes(8)), 'block_ms' => 0];
$redis = RedisConnection::connect($config);
$app = new App();
$app->instance(Redis::class, $redis);
$queue = new Queue(new Transport($redis, $config), $app);
$counter = $config['prefix'] . ':executions';
try {
    $start = hrtime(true);
    for ($i = 0; $i < $jobs; $i++) {
        $queue->push(BenchmarkJob::class, ['counter' => $counter, 'body' => str_repeat('x', $bytes),
            'enqueued_at_ns' => hrtime(true)]);
    }
    $produceMs = (hrtime(true) - $start) / 1000000;
    $worker = new Worker($queue);
    $start = hrtime(true);
    for ($i = 0; $i < $jobs; $i++) {
        if (!$worker->runNextJob()) {
            throw new \RuntimeException('基准任务未能全部领取。');
        }
    }
    $consumeMs = (hrtime(true) - $start) / 1000000;
    $executions = $redis->hVals($counter);
    if (count($executions) !== $jobs || count(array_filter($executions, fn ($n) => (int) $n !== 1)) > 0 || $queue->size() !== 0) {
        throw new \RuntimeException('基准任务有缺失、重复执行或未完成。');
    }
    sort(BenchmarkJob::$waits);
    echo json_encode(['php' => PHP_VERSION, 'redis' => $redis->info('server')['redis_version'], 'jobs' => $jobs,
        'payload_bytes' => $bytes, 'produce_jobs_per_second' => round($jobs * 1000 / max(0.001, $produceMs), 2),
        'consume_jobs_per_second' => round($jobs * 1000 / max(0.001, $consumeMs), 2),
        'queue_wait_ms_p50' => round(BenchmarkJob::$waits[(int) floor(($jobs - 1) * 0.5)], 2),
        'queue_wait_ms_p95' => round(BenchmarkJob::$waits[(int) floor(($jobs - 1) * 0.95)], 2),
        'peak_memory_bytes' => memory_get_peak_usage(true), 'unique_completed' => count($executions)],
        JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT) . "\n";
} finally {
    $queue->transport()->closeConsumer();
    $redis->del(...[...$queue->transport()->keys('default'), $counter]);
}
