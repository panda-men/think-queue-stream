<?php
declare(strict_types=1);

require dirname(__DIR__, 2) . '/vendor/autoload.php';

use think\App;
use ThinkQueue\Streams\RedisConnection;
use ThinkQueue\Streams\Service;

// 由临时项目的 think 入口包含，使用真实 Console、Service 和 queue:work。
$root = dirname($_SERVER['SCRIPT_FILENAME']);
if (($argv[1] ?? '') === 'queue:work') {
    $modeFile = $root . '/worker-mode';
    // 队列级故障注入只影响目标队列，其他队列仍启动真实 queue:work。
    foreach ($argv as $argument) {
        if (str_starts_with($argument, '--queue=')) {
            $queueModeFile = $root . '/worker-mode-' . substr($argument, strlen('--queue='));
            if (is_file($queueModeFile)) $modeFile = $queueModeFile;
            break;
        }
    }
    $mode = is_file($modeFile) ? trim(file_get_contents($modeFile)) : '';
    if ($mode === 'crash') {
        file_put_contents($root . '/worker-started', (string) getmypid());
        exit(17);
    }
    if ($mode === 'ignore-term') {
        pcntl_async_signals(true);
        pcntl_signal(SIGTERM, SIG_IGN);
        while (true) usleep(100000);
    }
}
$app = new App($root);
$app->initialize();
$service = new Service($app);
$service->register();
$service->boot();
if (($argv[1] ?? '') === 'queue:work') {
    $app->instance(Redis::class, RedisConnection::connect($app->config->get('queue_streams')));
    if (($mode ?? '') === 'stop-before-claim') {
        $app->event->listen(\ThinkQueue\Streams\Event\Looping::class, static function () use ($root): void {
            file_put_contents($root . '/loop-entered', '1');
            $until = hrtime(true) + 800000000;
            while (hrtime(true) < $until) usleep(10000);
        });
    }
}
$app->console->run();
