<?php
declare(strict_types=1);

require dirname(__DIR__, 2) . '/vendor/autoload.php';

use think\App;
use ThinkQueue\Streams\Queue;
use ThinkQueue\Streams\RedisConnection;
use ThinkQueue\Streams\Transport;
use ThinkQueue\Streams\Worker;
use ThinkQueue\Streams\Service;
use ThinkQueue\Streams\Facade\Queue as QueueFacade;
use ThinkQueue\Streams\Tests\Fixtures\ThrowingJob;

if ($argv[1] === 'proxy') {
    $server = stream_socket_server('tcp://127.0.0.1:0');
    echo substr(strrchr(stream_socket_get_name($server, false), ':'), 1) . "\n";
    flush();
    $client = stream_socket_accept($server, 5);
    $upstream = stream_socket_client('tcp://' . (getenv('REDIS_HOST') ?: '127.0.0.1') . ':' .
        (getenv('REDIS_PORT') ?: 6379));
    while (true) {
        $read = [$client, $upstream];
        $write = $except = null;
        if (stream_select($read, $write, $except, 5) === 0) {
            break;
        }
        foreach ($read as $socket) {
            $bytes = fread($socket, 8192);
            if ($bytes === '' && feof($socket)) {
                break 2;
            }
            fwrite($socket === $client ? $upstream : $client, $bytes);
        }
    }
    exit(0);
}

$config = ['host' => getenv('REDIS_HOST') ?: '127.0.0.1', 'port' => (int) (getenv('REDIS_PORT') ?: 6379),
    'prefix' => $argv[2], 'block_ms' => 0, 'retry_after_ms' => 2000, 'claim_interval_ms' => 1];
$redis = RedisConnection::connect($config);
$app = new App();
$app->instance(Redis::class, $redis);
$queue = new Queue(new Transport($redis, $config), $app);
if ($argv[1] === 'claim') {
    $job = $queue->pop();
    echo $job?->getJobId() . "\n";
    flush();
    sleep(10);
} elseif ($argv[1] === 'timeout') {
    (new Worker($queue))->runNextJob(null, 1, 1, 0);
} elseif ($argv[1] === 'drain') {
    $redis->incr($config['prefix'] . ':ready');
    while ($redis->get($config['prefix'] . ':start') !== '1') {
        usleep(1000);
    }
    $worker = new Worker($queue);
    while ($worker->runNextJob(null, 3, 1, 0)) {
        $redis->hIncrBy($config['prefix'] . ':consumers', (string) getmypid(), 1);
        usleep(1000);
    }
    $queue->transport()->closeConsumer();
} elseif ($argv[1] === 'commands') {
    $app->config->set(['default' => 'file', 'stores' => ['file' => ['type' => 'File',
        'path' => sys_get_temp_dir() . '/' . $config['prefix'] . '/cache/']]], 'cache');
    $app->config->set(['default' => 'file', 'channels' => ['file' => ['type' => 'File',
        'path' => sys_get_temp_dir() . '/' . $config['prefix'] . '/log/']]], 'log');
    $app->config->set($config, 'queue_streams');
    $service = new Service($app);
    $service->register();
    $service->boot();
    QueueFacade::push(ThrowingJob::class, ['counter' => $config['prefix'] . ':counter']);
    $console = $app->console;
    $workOutput = $console->call('queue:work', ['--once', '--delay=0', '--timeout=1', '--tries=1'])->fetch();
    $record = json_decode(trim($console->call('queue:manage', ['failed', '--limit=1'])->fetch()), true, 512, JSON_THROW_ON_ERROR);
    $console->call('queue:manage', ['retry', $record['failure_id']]);
    $console->call('queue:manage', ['restart']);
    echo json_encode(['work_output' => $workOutput, 'record' => $record,
        'failures_after_retry' => count(QueueFacade::transport()->failures('default')),
        'restart_token' => QueueFacade::transport()->restartToken()], JSON_THROW_ON_ERROR);
}
