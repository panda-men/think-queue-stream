<?php
declare(strict_types=1);

require dirname(__DIR__, 2) . '/vendor/autoload.php';

use think\App;
use ThinkQueue\Streams\Tests\Fixtures\FailureDatabase;
use ThinkQueue\Streams\Queue;
use ThinkQueue\Streams\RedisConnection;
use ThinkQueue\Streams\Transport;
use ThinkQueue\Streams\Worker;
use ThinkQueue\Streams\Service;
use ThinkQueue\Streams\Facade\Queue as QueueFacade;
use ThinkQueue\Streams\Tests\Fixtures\ThrowingJob;
use ThinkQueue\Streams\Tests\Fixtures\SuccessJob;
use ThinkQueue\Streams\Exception\QueueFull;
use ThinkQueue\Streams\Exception\JobIdConflict;

/** 读取一条 RESP2 帧，同时保留可转发的原始字节。 */
function readResp($socket): array
{
    $line = fgets($socket);
    if ($line === false) {
        throw new RuntimeException('RESP connection closed.');
    }
    $raw = $line;
    $value = substr($line, 1, -2);
    if ($line[0] === '*') {
        $items = [];
        for ($i = 0; $i < (int) $value; $i++) {
            [$bytes, $item] = readResp($socket);
            $raw .= $bytes;
            $items[] = $item;
        }
        return [$raw, $items];
    }
    if ($line[0] === '$' && (int) $value >= 0) {
        $bytes = '';
        while (strlen($bytes) < (int) $value + 2) {
            $part = fread($socket, (int) $value + 2 - strlen($bytes));
            if ($part === false || $part === '') {
                throw new RuntimeException('Incomplete RESP bulk string.');
            }
            $bytes .= $part;
        }
        return [$raw . $bytes, substr($bytes, 0, -2)];
    }
    return [$raw, $value];
}

if (in_array($argv[1], ['auth-zero', 'stall-auth', 'stall-select', 'drop-ack-response', 'drop-enqueue-response', 'drop-replay-response'], true)) {
    $server = stream_socket_server('tcp://127.0.0.1:0');
    echo substr(strrchr(stream_socket_get_name($server, false), ':'), 1) . "\n";
    flush();
    $client = stream_socket_accept($server, 5);
    $authenticated = false;
    $upstream = in_array($argv[1], ['drop-ack-response', 'drop-enqueue-response', 'drop-replay-response'], true) ? stream_socket_client('tcp://' .
        (getenv('REDIS_HOST') ?: '127.0.0.1') . ':' . (getenv('REDIS_PORT') ?: 6379)) : null;
    $dropScript = match ($argv[1]) { 'drop-enqueue-response' => 'enqueue', 'drop-replay-response' => 'replay', default => 'ack' };
    $dropSha = sha1(file_get_contents(dirname(__DIR__, 2) . '/src/Lua/common.lua') . "\n" .
        file_get_contents(dirname(__DIR__, 2) . '/src/Lua/' . $dropScript . '.lua'));
    while (!feof($client)) {
        if (($peek = fread($client, 1)) === '') {
            break;
        }
        // 首字节用于检测关闭；读取剩余数组头与各参数。
        $head = $peek . fgets($client);
        $raw = $head;
        $args = [];
        for ($i = 0; $i < (int) substr($head, 1); $i++) {
            [$bytes, $value] = readResp($client);
            $raw .= $bytes;
            $args[] = $value;
        }
        $command = strtoupper($args[0]);
        if (($argv[1] === 'stall-auth' && $command === 'AUTH') ||
            ($argv[1] === 'stall-select' && $command === 'SELECT')) {
            sleep(5);
            break;
        }
        if ($upstream !== null) {
            fwrite($upstream, $raw);
            [$reply] = readResp($upstream);
            if ($command === 'EVALSHA' && $args[1] === $dropSha && $reply[0] !== '-') {
                echo $dropScript . "-applied\n";
                flush();
                break;
            }
        } elseif ($command === 'AUTH') {
            $authenticated = $args[1] === '0';
            $reply = $authenticated ? "+OK\r\n" : "-WRONGPASS invalid password\r\n";
        } elseif (!$authenticated && $argv[1] === 'auth-zero') {
            $reply = "-NOAUTH Authentication required\r\n";
        } elseif ($command === 'INFO') {
            $info = "# Server\r\nredis_version:6.2.0\r\n";
            $reply = '$' . strlen($info) . "\r\n" . $info . "\r\n";
        } else {
            $reply = "+OK\r\n";
        }
        fwrite($client, $reply);
    }
    fclose($client);
    if ($upstream !== null) {
        fclose($upstream);
    }
    exit(0);
}

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
if ($argv[1] === 'produce') {
    $config['max_active_jobs'] = 7;
}
$redis = RedisConnection::connect($config);
$app = new App();
$app->config->set(FailureDatabase::configuration($config['prefix']), 'database');
$app->instance(Redis::class, $redis);
$queue = new Queue(new Transport($redis, $config, FailureDatabase::store($config['prefix'])), $app);
if (in_array($argv[1], ['retry-failure', 'migrate-failures'], true)) {
    $redis->incr($config['prefix'] . ':ready');
    while ($redis->get($config['prefix'] . ':start') !== '1') usleep(1000);
    $result = $argv[1] === 'retry-failure' ? $queue->transport()->retryFailed('default', $argv[3]) :
        $queue->transport()->migrateFailed('default', 1000);
    echo json_encode($result, JSON_THROW_ON_ERROR);
} elseif ($argv[1] === 'claim') {
    $job = $queue->pop();
    echo $job?->getJobId() . "\n";
    flush();
    sleep(10);
} elseif ($argv[1] === 'logged-timeout') {
    $monitor = new \ThinkQueue\Streams\Monitor();
    $monitor->setOutput(static function (string $line): void { echo $line . "\n"; });
    $monitor->start('default', $queue->transport()->consumerName());
    (new Worker($queue, null, $monitor))->runNextJob(null, 1, 1, 0);
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
} elseif ($argv[1] === 'produce') {
    $redis->incr($config['prefix'] . ':ready');
    while ($redis->get($config['prefix'] . ':start') !== '1') {
        usleep(1000);
    }
    for ($i = 0; $i < 10; $i++) {
        try {
            $queue->push(SuccessJob::class);
            $redis->incr($config['prefix'] . ':accepted');
        } catch (QueueFull) {
            $redis->incr($config['prefix'] . ':rejected');
        }
    }
} elseif (in_array($argv[1], ['produce-ids', 'produce-collisions', 'produce-duplicates', 'produce-id-conflicts'], true)) {
    if ($argv[1] === 'produce-collisions') {
        $generated = 0;
        $queue = new Queue(new Transport($redis, $config, FailureDatabase::store($config['prefix'])), $app, 'default', static function () use (&$generated): string {
            return $generated++ === 0 ? str_repeat('0', 32) : bin2hex(random_bytes(16));
        });
    }
    $redis->incr($config['prefix'] . ':ready');
    $deadline = microtime(true) + 5;
    while ($redis->get($config['prefix'] . ':start') !== '1') {
        if (microtime(true) >= $deadline) {
            throw new RuntimeException('并发生产者等待超时。');
        }
        usleep(1000);
    }
    for ($i = 0; $i < 10; $i++) {
        $options = in_array($argv[1], ['produce-duplicates', 'produce-id-conflicts'], true) ? ['id' => 'shared-task'] : [];
        $data = match ($argv[1]) {
            'produce-duplicates' => ['report_id' => 10001],
            'produce-id-conflicts' => ['producer' => getmypid()],
            default => ['producer' => getmypid(), 'index' => $i],
        };
        try {
            $id = $queue->push(SuccessJob::class, $data, null, $options);
            $redis->hIncrBy($config['prefix'] . ':returned-ids', $id, 1);
            $redis->incr($config['prefix'] . ':accepted');
        } catch (JobIdConflict) {
            $redis->incr($config['prefix'] . ':rejected');
        }
    }
} elseif ($argv[1] === 'wake') {
    usleep(150000);
    $queue->push(SuccessJob::class);
} elseif ($argv[1] === 'monitor-commands') {
    if (($argv[3] ?? '') === 'silent') {
        $config['log_silent_jobs'] = [\ThinkQueue\Streams\Tests\Fixtures\LifecycleJob::class];
    }
    $app->config->set(['default' => 'file', 'stores' => ['file' => ['type' => 'File',
        'path' => sys_get_temp_dir() . '/' . $config['prefix'] . '/cache/']]], 'cache');
    $app->config->set(['default' => 'file', 'channels' => ['file' => ['type' => 'File',
        'path' => sys_get_temp_dir() . '/' . $config['prefix'] . '/log/']]], 'log');
    $app->config->set($config, 'queue_streams');
    $service = new Service($app);
    $service->register();
    $service->boot();
    $app->instance(Queue::class, $queue);
    $monitor = $app->make(\ThinkQueue\Streams\Monitor::class);
    $queue = new Queue(new Transport($redis, $config, FailureDatabase::store($config['prefix'])), $app, 'default', null, $monitor);
    $app->instance(Queue::class, $queue);
    $restored = '';
    $monitor->setOutput(static function (string $line) use (&$restored): void { $restored = $line; });
    $events = [];
    $app->event->listen(\ThinkQueue\Streams\Event\Looping::class,
        static function ($event) use (&$events): void {
            $events[] = $event->once;
            if (!$event->once) $event->queue->push(\ThinkQueue\Streams\Tests\Fixtures\LifecycleJob::class);
        });
    $work = $app->console->call('queue:work', ['--max-jobs=1', '--timeout=1'])->fetch();
    $once = $app->console->call('queue:work', ['--once', '--timeout=1'])->fetch();
    $id = QueueFacade::push(\ThinkQueue\Streams\Tests\Fixtures\LifecycleJob::class);
    echo json_encode(compact('work', 'once', 'events', 'id', 'restored'), JSON_THROW_ON_ERROR);
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
    $console->call('queue:manage', ['install']);
    $workOutput = $console->call('queue:work', ['--once', '--delay=0', '--timeout=1', '--tries=1'])->fetch();
    $record = json_decode(trim($console->call('queue:manage', ['failed', '--limit=1'])->fetch()), true, 512, JSON_THROW_ON_ERROR);
    $console->call('queue:manage', ['retry', $record['failure_id']]);
    $console->call('queue:manage', ['restart']);
    $stats = json_decode($console->call('queue:stats', ['--json'])->fetch(), true, 512, JSON_THROW_ON_ERROR);
    $preview = json_decode($console->call('queue:manage', ['prune', '--older-than=86400', '--dry-run'])->fetch(), true, 512, JSON_THROW_ON_ERROR);
    $bad = QueueFacade::push(ThrowingJob::class, [], 'invalid');
    $transport = QueueFacade::transport();
    $redis->hSet($transport->keys('invalid')[2], $bad, 'invalid-json');
    QueueFacade::pop('invalid');
    $quarantined = json_decode(trim($console->call('queue:manage', ['quarantined', '--queue=invalid'])->fetch()), true, 512, JSON_THROW_ON_ERROR);
    $consumer = $transport->stats('invalid')['consumers'][0]['name'];
    $redis->zAdd($transport->keys('invalid')[7], (time() - 1000) * 1000, $consumer);
    $cleanup = $console->call('queue:manage', ['cleanup-consumer', $consumer, '--queue=invalid', '--idle-ms=300000'])->fetch();
    $console->call('queue:manage', ['forget-quarantine', $quarantined['quarantine_id'], '--queue=invalid']);
    $redis->xAdd($transport->keys('history')[5], '1-0', ['error' => 'old']);
    $migration = json_decode($console->call('queue:manage', ['migrate-failed', '--queue=history', '--limit=1'])->fetch(), true, 512, JSON_THROW_ON_ERROR);
    $pruned = json_decode($console->call('queue:manage', ['prune', '--queue=history', '--older-than=86400'])->fetch(), true, 512, JSON_THROW_ON_ERROR);
    echo json_encode(['work_output' => $workOutput, 'record' => $record,
        'failures_after_retry' => count(QueueFacade::transport()->failures('default')),
        'restart_token' => QueueFacade::transport()->restartToken(), 'stats' => $stats, 'prune' => $preview,
        'quarantined' => $quarantined, 'quarantined_after_forget' => count($transport->quarantined('invalid')),
        'cleanup' => $cleanup, 'pruned' => $pruned, 'migration' => $migration], JSON_THROW_ON_ERROR);
}
