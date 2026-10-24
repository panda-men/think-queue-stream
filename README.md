# think-queue-stream

面向 **PHP 8.2+、ThinkPHP 8** 的 Redis Streams 任务队列。沿用 think-queue 的
`push/later`、`fire`、`delete/release` 使用习惯，重新实现传输和消费者。

当前为第一版开发实现，尚未发布稳定版本或完成生产环境验收。

## 环境要求

- PHP `^8.2`，ThinkPHP `^8.0`。
- phpredis 扩展 `>=6.0`，Redis Server `>=6.2`。
- 消费者要求 Linux/macOS CLI 与 `pcntl`；HTTP 生产者不需要 `pcntl`。
- 第一版支持单 Redis 实例或稳定的 Redis 服务地址；不支持 `RedisCluster`、多消费组广播。

## 安装与接入

包名：`panda-men/think-queue-stream`。在发布到 Packagist 前，可在项目的
`composer.json` 添加 VCS 仓库，并在实际推送分支后安装该开发分支：

```json
{
    "repositories": [
        {"type": "vcs", "url": "https://github.com/panda-men/think-queue-stream.git"}
    ]
}
```

ThinkPHP 的服务发现会注册 `ThinkQueue\Streams\Service` 并发布
`config/queue_streams.php`。已有项目可执行 `php think service:discover` 和
`php think vendor:publish`。如果未使用服务发现，在项目服务列表中注册上述 Service。

当前开发分支安装命令：

```bash
composer require panda-men/think-queue-stream:dev-codex/redis-streams-v1
```

配置文件不包含凭据默认值，请从部署环境读取 Redis 密码。队列使用自己的专用连接，
不复用带序列化或客户端 key 前缀的缓存连接。

```php
use ThinkQueue\Streams\Facade\Queue;
use app\job\ExportReport;

$id = Queue::push(ExportReport::class, ['report_id' => 10001], 'exports');
Queue::later(60, ExportReport::class, ['report_id' => 10002], 'exports');

// 可选：活动任务 ID 去重。已完成任务不会永久保留去重记录。
Queue::push(ExportReport::class, ['report_id' => 10003], 'exports', [
    'id' => 'report-10003-v1',
    'max_tries' => 5,
    'timeout' => 30,
]);
```

任务仅保存版本化 JSON 数据，不支持序列化整个 PHP 对象。不要把 ORM 模型、资源或
凭据对象放入消息；业务操作通过 ID 在 Service 中重新读取。

```php
namespace app\job;

use app\common\service\ReportService;
use ThinkQueue\Streams\Job;

final class ExportReport
{
    public function __construct(private ReportService $reports) {}

    public function fire(Job $job, array $data): void
    {
        $this->reports->export($data['report_id']);
        $job->delete();
    }

    public function failed(array $data, \Throwable $exception): void
    {
        // 最终失败记录已保存。此回调是尽力通知，不能依赖其恰好执行一次。
    }
}
```

`Job::release($seconds)` 用于主动重投。`attempts()` 从 1 开始，重投、崩溃后接管均计数。
任务返回时未调用 `delete()` 或 `release()`，Worker 会将其作为异常重试。
业务参数和任务类来自可信生产者，Redis 写权限必须限制为受信任的应用。

## 消费与管理

```bash
php think queue:work --queue=exports --tries=3 --timeout=60 --delay=3
php think queue:work --queue=exports --once --delay=0

php think queue:manage failed --queue=exports --limit=100
php think queue:manage retry 1234567890000-0 --queue=exports
php think queue:manage forget 1234567890000-0 --queue=exports
php think queue:manage restart
```

失败记录查询为 JSON Lines，`failure_id` 是重放或删除命令所需的 Stream ID，
`job_id` 是跨重投保持不变的任务 ID。分页可传 `--start='(上次最后的failure_id'`。

Worker 默认处理 1000 个任务、运行 3600 秒或使用 128 MB 内存后退出，可分别用
`--max-jobs`、`--max-time`、`--memory` 配置。使用 Supervisor/systemd 自动拉起；
SIGTERM/SIGINT 与 restart 标记会让消费者在当前任务完成后退出。Redis 连接异常会使
消费者退出，由进程管理器重新启动，不盲目重发结果不明的写命令。

`--delay` 是重试初始秒数，异常重试采用指数退避，最长 3600 秒。
`--tries` 表示最大领取次数；强制退出的任务也消耗一次领取次数，达到上限后的恢复领取
仅负责转入失败队列，不再运行处理器。主动重投同样受此限制。

执行 `failed()` 回调前先保存失败记录。如果处理器尚未启动就因超限失败，无法调用尚未
创建的处理器回调；管理命令中的失败记录是最终状态的查询来源。

## 交付语义与边界

- 按至少一次投递设计。业务成功而 ACK 前进程退出时仍会重复处理，业务幂等必须自行实现。
- 数据库提交与消息入队不是同一事务。要求二者一致的业务应实现事务发件箱等机制。
- 即时任务使用 `XREADGROUP`，Pending 任务通过 `XAUTOCLAIM` 接管；每个消费者名称唯一。
- 延迟任务与重试任务使用 ZSet；消费者在每轮领取前有界迁移到期任务，不循环重投未来消息。
- 延迟精度受 `block_ms`、Worker 负载影响；没有消费者运行时不会迁移，但任务仍保留在 ZSet。
- ACK、重投、失败迁移都会校验 PEL 当前消费者和本次租约 token，阻止旧 Worker 错误确认。
- Lua 对相关状态变更提供互斥执行，写入前检查键类型；Lua 本身不提供运行时错误回滚。
- Redis 服务端持久化、内存淘汰与复制故障仍影响数据持久性；不要把队列与可随意淘汰的缓存混用。
- 活动任务完成后即删除消息体、尝试次数和租约。失败消息保留到显式删除或重放，无默认过期。
- 不要对 ready Stream 进行外部 `XTRIM/MAXLEN`、改消费组或手动删除待处理消息。
- 第一版不提供自动续租。`timeout * 1000` 必须严格小于 `retry_after_ms`；长任务应拆分或提高二者。
- Worker 超时直接以 124 退出并保留 PEL，由其他消费者在超时后恢复。业务外部操作仍须有自身超时。
- 每次创建新的任务类实例；长期持有的应用单例服务仍应自行避免跨任务残留业务状态。
- 正常退出清理空消费者元数据；崩溃进程留下的空消费者元数据需要运维定期检查。

## 与 topthink/think-queue 的差异

这是采用独立命名空间的新实现，**不是完全兼容的原包替换**。使用时将 Facade 和 Job 的
导入改为 `ThinkQueue\Streams\Facade\Queue`、`ThinkQueue\Streams\Job`。
原包的 `CallQueuedHandler`、PHP 对象序列化、Database/Sync 驱动、原有 Redis List 数据和
失败表不会自动迁移。不要在同一应用中同时注册两个包的 `queue:work` 命令。

只实现一个消费组的任务队列，不承诺严格执行顺序、恰好一次业务执行或广播语义。

## 测试

Redis 不可用时测试直接失败，不会跳过集成验证：

```bash
composer install
composer validate --strict
composer lint
REDIS_HOST=127.0.0.1 REDIS_PORT=6379 composer test
```

Docker 隔离验证（不使用现有 Redis，不暴露端口）：

```bash
sh scripts/test-docker.sh 8.2
sh scripts/test-docker.sh 8.4
```

GitHub Actions 配置 PHP 8.2/8.3/8.4 × Redis 6.2/7.4。测试覆盖立即/延迟投递、
重投计数、最终失败及回放、多 PHP 进程竞争、SIGKILL 恢复、任务超时、旧消费者处理权、
确认前断连、Lua 键类型校验和 ThinkPHP 命令集成。

CI 通过不等同于生产故障演练。Redis 主从切换、OOM、网络在响应返回前断开、长期高负载
以及实际业务幂等仍需部署环境验收。

## 许可证

Apache-2.0。API 设计参考 topthink/think-queue，来源说明见 [NOTICE](NOTICE)。
