# think-queue-stream

适用于 ThinkPHP 8 的 Redis Streams 队列。支持立即和延迟投递、失败重试、任务接管，以及一个主进程托管多个队列。最终失败和隔离消息写入数据库。

v0.7 的命令统一使用 `stream:`，可以与使用 `queue:` 的 `topthink/think-queue` 在同一项目中运行。版本记录见 [CHANGELOG](CHANGELOG.md)。

## 快速开始

### 安装和配置

需要 PHP 8.2+、ThinkPHP 8、Redis Server 6.2+、phpredis 6.0+，以及 MySQL/MariaDB 或 SQLite 对应的 PDO 驱动。运行 Worker 的 Linux/macOS CLI 还需启用 `pcntl`。

在 ThinkPHP 项目根目录执行：

```bash
composer require 'panda-men/think-queue-stream:^0.7'
php think service:discover
php think vendor:publish
```

在项目的 `config/queue_streams.php` 中配置 Redis 的 `host`、`port`、`password` 和 `database`。`prefix` 用于区分项目和环境，同一项目的生产者、Worker 和管理命令使用同一套配置。

`failed_connection` 默认为 `null`，使用应用的默认数据库连接。`failed_table` 默认为 `failed_jobs`，需要表前缀时请填写完整表名。首次部署时创建失败表：

```bash
php think stream:manage install
```

完整配置和默认值见 [配置模板](config/queue_streams.php)。未使用服务发现的项目可在应用服务列表中注册 `ThinkQueue\Streams\Service`，并将模板复制到应用的 `config/` 目录。

### 写一个 Job

以下示例放在 `app/job/ExportReport.php`。示例处理器会写入一条日志，可以将该行替换为实际导出逻辑。

```php
<?php

namespace app\job;

use think\facade\Log;
use ThinkQueue\Streams\Job;

final class ExportReport
{
    public function fire(Job $job, array $data): void
    {
        Log::info('收到报表导出任务', ['report_id' => $data['report_id']]);
        $job->delete();
    }
}
```

业务成功后调用 `$job->delete()` 确认任务。业务失败时抛出异常，Worker 按尝试次数和退避策略重试。需要主动延后时调用 `$job->release(30)` 并结束本次处理，单位为秒。一个 Job 有多个处理方法时，投递时可以指定 `类名@方法名`。

### 投递和消费

在业务代码中调用 `Queue::push()` 或 `Queue::later()`，两者均返回队列任务 ID。

```php
use app\job\ExportReport;
use ThinkQueue\Streams\Facade\Queue;

// 立即投递
$jobId = Queue::push(ExportReport::class, ['report_id' => 10001], 'exports');

// 另一条任务在 60 秒后到期
Queue::later(60, ExportReport::class, ['report_id' => 10002], 'exports');
```

Worker 的 `--queue` 要和投递时的队列名一致。`--once` 只尝试领取一次，适合本地验证。

```bash
php think stream:work --queue=exports --once
php think stream:stats --queue=exports --json
```

持续消费时可运行 `php think stream:work --queue=exports --tries=3 --timeout=60`，由进程管理器在 Worker 退出后拉起。需要集中管理多个队列时，使用下面的主进程。

## 常驻消费和宝塔部署

在项目的 `config/queue_streams.php` 中配置 `supervisor.workers`：

```php
'supervisor' => [
    'workers' => [
        'exports' => ['processes' => 2, 'tries' => 3, 'timeout' => 60],
        'mail' => ['processes' => 1, 'tries' => 3, 'timeout' => 30],
    ],
    'stop_wait_seconds' => 120,
],
```

队列名作为 `workers` 的键。模板默认 `workers=[]`，需要先填入实际队列。`processes=0` 可暂停指定队列。`stream:supervise` 在前台拉起各队列的 Worker，并在子进程退出后补齐。

宝塔、Supervisor 或 systemd 只需守护一个主进程。改用主进程托管前，先停止原有的独立 Worker。以下是 Supervisor 示例，路径和运行用户按实际部署填写：

```ini
[program:queue-streams]
directory=/www/wwwroot/your-project
command=/www/server/php/84/bin/php /www/wwwroot/your-project/think stream:supervise
user=www
numprocs=1
autostart=true
autorestart=true
stopsignal=TERM
stopwaitsecs=150
stopasgroup=true
killasgroup=true
redirect_stderr=true
stdout_logfile=/www/wwwroot/your-project/runtime/queue-supervise.log
stdout_logfile_maxbytes=20MB
stdout_logfile_backups=5
```

运行用户需要能写入 `runtime/`，CLI 需允许 `proc_open` 等进程函数。`stop_wait_seconds` 应大于最长任务超时与一次阻塞读取耗时之和，进程管理器的停止等待时间应再留出余量。

修改队列配置后，从项目根目录执行 `php think stream:supervise --reload`。主进程会校验配置，并在旧 Worker 完成当前任务后替换它们。重载结果看主进程日志。使用配置缓存时先更新缓存，修改 `.env` 或启动入口时重启主进程。

## 重试、任务 ID 和日志

```php
$jobId = Queue::push(ExportReport::class, ['report_id' => 10003], 'exports', [
    'id' => 'report-10003-v1',
    'max_tries' => 3,
    'timeout' => 30,
]);
```

- `max_tries` 包含首次领取、主动重投和崩溃后接管。单次硬超时 `timeout` 的单位为秒，必须满足 `timeout * 1000 < retry_after_ms`。
- 默认每次投递生成新 ID。指定 `id` 后，同一队列内相同内容和执行选项的活动任务会复用该 ID。内容或选项冲突时抛出 `JobIdConflict`。业务需要再次独立执行时使用新的任务 ID。
- 任务可能在业务完成后、确认前因进程中断而再次执行。业务侧应按业务编号实现幂等。数据库提交与 Redis 投递需要联动时，可用事务发件箱补投。
- 日志会记录完整任务参数。任务参数尽量只传业务 ID，凭据和业务正文从业务存储读取。`log_channel` 可指定独立日志通道，`log_silent_jobs` 可隐藏指定处理器的例行日志。

部署时使用持久化的队列 Redis，避免队列键被缓存淘汰、手动清理或截断。用 `max_active_jobs` 和 `max_payload_bytes` 控制积压和消息大小。

## 查看积压和处理失败

| 用途 | 命令 |
| --- | --- |
| 查看等待、处理中、延迟任务和 Worker 心跳 | `php think stream:stats --queue=exports --json` |
| 查看最终失败记录 | `php think stream:manage failed --queue=exports --limit=20` |
| 重放指定失败任务 | `php think stream:manage retry 1234567890000-0 --queue=exports` |
| 查看隔离的坏消息 | `php think stream:manage quarantined --queue=exports --limit=20` |
| 预览超过 7 天的失败记录 | `php think stream:manage prune --queue=exports --older-than=604800 --limit=100 --dry-run` |
| 通知同一 Redis 前缀下的 Worker 完成当前任务后退出 | `php think stream:manage restart` |

重放时用查询结果中的 `failure_id` 替换示例 ID。隔离消息需先排查生产者，再投递修正后的任务。确认清理范围后，去掉 `--dry-run` 才会删除记录。

失败历史保存在数据库，`failed_pending` 表示 Redis 中尚待归档的记录。写库未确认时，Worker 会退出，恢复后优先补写暂存。执行 `restart` 后由主进程或外部进程管理器重新拉起 Worker。更多选项可运行 `php think stream:manage --help` 或 `php think stream:work --help` 查看。

## 升级和迁移

### 从 think-queue-stream v0.6 升级

1. 停止已有的 Streams 主进程或独立 Worker。
2. 执行 `composer require 'panda-men/think-queue-stream:^0.7'`。
3. 把部署脚本、进程管理器和应用代码中的 Streams 命令改为 `stream:`，再启动主进程或 Worker。

现有队列名、Redis 前缀和失败表可直接沿用。更早版本也应先停消费，再按 [CHANGELOG](CHANGELOG.md) 核对失败表结构。使用 v0.7 命令时，旧表通过 `stream:manage upgrade` 升级，Redis 中的旧失败记录可按队列执行 `stream:manage migrate-failed --queue=exports`，重复到 `remaining=0`。

### 与 topthink/think-queue 共存

两个包分别使用 `config/queue.php` 和 `config/queue_streams.php`。如果两边都将失败历史存入数据库，为 Streams 配置独立的 `failed_table`。

按业务分批切换。先为 Streams 新建使用 `ThinkQueue\Streams\Job` 的处理器并启动 Worker，再将这一批生产者切到 `ThinkQueue\Streams\Facade\Queue`。保留旧处理器和旧 Worker，等旧队列的等待、延迟和处理中任务清空后再停。两套队列的数据格式独立，旧积压和失败历史由旧包处理。

## 开发验证

在本包仓库中准备独立 Redis，并启用 `pdo_sqlite`，执行：

```bash
composer install
composer lint
REDIS_HOST=127.0.0.1 REDIS_PORT=6379 composer test
```

也可以运行 `sh scripts/test-docker.sh 8.4 7.4`。PHP、Redis 和 MySQL 的测试组合见 [CI 配置](.github/workflows/ci.yml)。

## 许可证

Apache-2.0。API 设计参考 topthink/think-queue，详见 [NOTICE](NOTICE)。
