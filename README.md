# think-queue-stream

面向 **PHP 8.2+、ThinkPHP 8** 的 Redis Streams 任务队列。沿用 think-queue 的
`push/later`、`fire`、`delete/release` 使用习惯，重新实现传输和消费者。

当前版本为 **v0.2.1**，版本记录和升级说明见 [CHANGELOG](CHANGELOG.md)，发布说明见
[GitHub Releases](https://github.com/panda-men/think-queue-stream/releases)。具体业务仍需完成部署环境验收。

## 环境要求

- PHP `^8.2`，ThinkPHP `^8.0`。
- phpredis 扩展 `>=6.0`，Redis Server `>=6.2`。
- 消费者要求 Linux/macOS CLI 与 `pcntl`；HTTP 生产者不需要 `pcntl`。
- 第一版支持单 Redis 实例或稳定的 Redis 服务地址；不支持 `RedisCluster`、多消费组广播。

## 运行日志与循环事件

`queue:work` 默认显示启动、领取、成功、主动重投、重试、最终失败、处理权丢失和退出日志。每行包含毫秒时间及时区、队列名称、PID；消费日志还包含实际消费者名称、任务 ID、Stream 消息 ID、类名@方法、尝试次数和耗时。重投日志包含实际延迟秒数，退出日志包含原因和本进程统计。

日志不包含业务载荷、异常原文或堆栈，只保留异常类型。`succeeded` 表示处理器正常返回且已确认消息，不能代替业务数据库中的完成状态。`published`（投递确认）既可能新建任务，也可能复用相同自定义活动任务 ID；投递确认中的 `delay_seconds` 是请求值，不会改写已存在任务的延迟。`publish_failed`（投递未确认）不能用于判断 Redis 一定没有写入，网络异常仍应沿用稳定 ID 补偿。日志端故障不影响消息状态转换。

默认通过应用日志持久化，可在 `queue_streams.php` 配置 `log_channel` 为独立通道名；常驻进程建议为通道开启 `realtime_write`。强制任务超时时仅向标准输出写超时与退出统计，并以 124 退出，保留 PEL 供接管；应让进程管理器收集标准输出。`SIGKILL` 无法输出退出日志。`--once`、数量、时间、内存限制、重启标记和停止信号分别记录退出原因。没有任务时不持续输出空闲日志。

业务可监听 `ThinkQueue\Streams\Event\Looping`，每次领取前执行短时、有界的维护或补投；可读取 `$event->queue`、`$event->queueName` 和 `$event->once`。监听器异常会记录类型并让命令退出，由进程管理器重启。对于单次领取模式，应由业务根据 `once` 决定是否跳过补投，避免改变一次领取的含义。不要在监听器中进行无界扫描或长期阻塞。循环事件由当前消费进程串行执行，长任务会推迟下一次触发。

## 安装与接入

包名：`panda-men/think-queue-stream`。安装 v0.2 系列：

```bash
composer require panda-men/think-queue-stream:^0.2
```

如果所用 Composer 仓库尚未收录该包，可在项目的 `composer.json` 添加 VCS 仓库，
然后执行相同的安装命令，Composer 会从 Git 标签解析版本：

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

配置文件不包含凭据默认值，请从部署环境读取 Redis 密码。队列使用自己的专用连接，
不复用带序列化或客户端 key 前缀的缓存连接。

```php
use ThinkQueue\Streams\Facade\Queue;
use app\job\ExportReport;

// 立即投递：处理器类、业务参数、队列名；返回稳定的队列任务 ID。
// 这里只传报表 ID，由消费者中的 ReportService 根据 ID 读取业务数据。
$jobId = Queue::push(ExportReport::class, ['report_id' => 10001], 'exports');
// 延迟投递：第一个参数 60 的单位是秒，即 60 秒后才允许执行。
Queue::later(60, ExportReport::class, ['report_id' => 10002], 'exports');

// 可选：活动任务 ID 去重。已完成任务不会永久保留去重记录。
Queue::push(ExportReport::class, ['report_id' => 10003], 'exports', [
    // 自定义任务 ID；相同 ID 的活动任务只有内容和执行选项一致时才去重。
    'id' => 'report-10003-v1',
    // 最多领取 5 次，包含首次执行；不是首次执行之外再重试 5 次。
    'max_tries' => 5,
    // 单次执行硬超时，单位：秒；30 * 1000 必须小于 retry_after_ms。
    'timeout' => 30,
]);
```

### 任务 ID、并发与重复投递

`push()` 和 `later()` 返回字符串任务 ID（`job_id`），表示 Redis 已接受任务，或已存在
可以去重的同一活动任务，不表示业务已经执行成功。无需追踪任务时可以不接收返回值。
`report_id` 用于消费者查询业务报表，`job_id` 用于关联队列消息体、延迟记录、尝试次数和日志。
消费者通过 `$job->getJobId()` 读取相同 ID；重投、超时接管和失败重放不会更换它。
`$job->getMessageId()` 是本次投递的 Redis Stream ID，重投时可以变化。

**业务主动重新执行应创建新任务；同一任务内部重试才沿用原 ID。** 默认每次调用
`push()` 或 `later()` 都生成新 ID，即使任务类和业务参数完全相同也会分别入队。
只有显式传入 `options['id']` 才启用自定义 ID 去重，不会按业务 ID 自动合并任务：

```php
// 同一份报表的两次独立导出，两条队列任务，返回不同的 job_id。
$firstJobId = Queue::push(ExportReport::class, ['report_id' => 10001], 'exports');
$secondJobId = Queue::push(ExportReport::class, ['report_id' => 10001], 'exports');
```

需要每次都执行时，将业务 ID 放在 `$data` 中，让队列生成任务 ID；系统已有 GUID 或雪花 ID
时也可以为每次独立执行生成新值传入。任务处理中调用 `$job->release()` 属于原任务重试，
继续使用原 `job_id`。

默认 ID 是 **32 位十六进制随机字符串**，使用完整的 `bin2hex(random_bytes(16))`，
保留 128 位随机数，不拼接时间戳。例如：

```text
e4b7842c3f734f93a54c36f9c8d7a201
```

并发入队依靠随机数和 Redis 原子查重处理冲突；此格式不承诺严格执行顺序。
任务创建时间保存在 `created_at` 字段中。已有合法任务 ID 仍可消费、重投及重放，
无需迁移 Redis 数据。

查重和创建在同一个 Lua 脚本中执行，多个生产者不会同时创建两个相同 ID 的活动任务：

| 入队场景 | 处理规则 |
| --- | --- |
| 自动生成 ID 尚未被占用 | 创建新任务并返回 ID。 |
| 自动生成 ID 已被活动任务占用 | 不作为去重成功；重新生成后尝试，最多生成 5 次，仍冲突则抛出 `Exception\JobIdConflict`。 |
| 自定义 ID 已存在，任务类、业务参数、`max_tries` 和 `timeout` 一致 | 返回已有 ID，保留原任务内容、尝试次数和延迟安排。创建时间及 JSON 对象键顺序不参与内容差异判断；数组顺序和 JSON 值类型需要一致。 |
| 自定义 ID 已存在，但上述内容或选项不同，或原消息损坏 | 抛出 `Exception\JobIdConflict`，保留原任务，不覆盖、不静默忽略新任务。 |
| Redis 连接失败、超时或写入响应丢失 | 直接传播异常，不换 ID 自动重发结果不明的请求。 |

自定义 ID 仍支持 1 至 128 位字母、数字、下划线和连字符；雪花 ID 请转换为字符串传入
`['id' => $taskId]`。直接构造 `Queue` 时可选传入第四个参数 `Closure` 作为 ID 生成器；
它只用于未指定自定义 ID 的请求，输出同样需要通过格式校验，并遵循最多 5 次的冲突处理。

去重范围是同一 Redis 前缀下的同一队列，且只覆盖等待、延迟或处理中任务。任务完成、
最终失败或被隔离后不永久保留去重占位；再次提交相同 ID 可以创建新任务。
任务 ID 不能替代业务幂等，消费者仍可能因确认前崩溃等原因重复执行业务。

任务仅保存版本化 JSON 数据，不支持序列化整个 PHP 对象。不要把 ORM 模型、资源或
凭据对象放入消息；业务操作通过 ID 在 Service 中重新读取。

默认限制完整消息 JSON 为 1 MiB，`max_payload_bytes=0` 可关闭；`max_active_jobs=0`
表示不限制活跃任务数量。活跃任务包含等待、延迟和处理中任务，已失败或隔离的记录不计入。
可在 `queue_streams.php` 配置：

```php
// 每个队列最多保留 10000 个活跃任务，防止积压无限增长。
'max_active_jobs' => 10000,
// 单条完整消息最多 1 MiB，包含业务参数和消息元数据。
'max_payload_bytes' => 1048576,
// 异常重试增加随机抖动，避免大量任务同时重试。
'retry_jitter' => true,
```

容量检查和入队在同一个 Lua 内完成；满队列抛出 `Exception\QueueFull`，超大消息抛出
`Exception\PayloadTooLarge`，调用方可按业务决定稍后提交或拒绝请求。相同自定义活动任务 ID
先检查内容是否一致，一致时去重优先于容量和字节限制，不一致时抛出 `Exception\JobIdConflict`。
已有任务 `release()` 不受容量限制；失败记录重放会重新检查容量和字节限制，
被拒绝时保留失败记录。此去重只覆盖任务活跃期间，不能替代业务幂等。
同一队列的生产者、消费者和重放命令应使用一致的限制配置；降低字节上限后，超限旧消息
也会在消费时转入隔离记录。

下面的处理器放在 `app/job/ExportReport.php`。`ReportService` 是由业务项目实现的服务，
负责按报表 ID 查询数据并完成导出；队列包通过 ThinkPHP 容器注入它。

```php
namespace app\job;

use app\common\service\ReportService;
use ThinkQueue\Streams\Job;

final class ExportReport
{
    /**
     * 注入业务服务，每次执行任务时创建新的处理器实例。
     * @param ReportService $reports 报表导出服务
     */
    public function __construct(private ReportService $reports) {}

    /**
     * 执行报表导出；业务成功后再确认队列消息。
     * @param Job $job 当前队列任务，可读取任务 ID、领取次数或主动重投
     * @param array{report_id:int} $data 入队时传入的业务参数
     * @return void
     * @throws \Throwable 导出失败时直接上抛，由 Worker 按最大尝试次数和退避策略处理
     */
    public function fire(Job $job, array $data): void
    {
        // 根据业务 ID 读取数据；导出操作应按业务规则实现幂等，允许任务重复执行。
        $this->reports->export($data['report_id']);
        // delete() 表示确认本次任务完成并清理队列消息，不会删除业务报表。
        // 如果业务没有成功，不要提前确认，否则消费者无法再恢复该任务。
        $job->delete();
    }

    /**
     * 最终失败后的尽力回调，此时 Redis 中的失败记录已保存。
     * @param array{report_id:int} $data 原始业务参数
     * @param \Throwable $exception 导致任务最终失败的异常
     * @return void
     */
    public function failed(array $data, \Throwable $exception): void
    {
        // 可在此补充业务失败记录或告警；处理时同样需要幂等，不能依赖回调恰好执行一次。
        // 若任务在处理器创建前已因超限等原因失败，本回调不会执行；以队列失败记录为准。
    }
}
```

`Job::release($seconds)` 用于主动重投。`attempts()` 从 1 开始，重投、崩溃后接管均计数。
任务返回时未调用 `delete()` 或 `release()`，Worker 会将其作为异常重试。
业务参数和任务类来自可信生产者，Redis 写权限必须限制为受信任的应用。

## 配置项中文说明

完整模板见 [config/queue_streams.php](config/queue_streams.php)，每个配置项均有中文注释。
下表为**队列包默认值**，业务项目可以在发布后的配置文件中覆盖；已有配置文件请按需合并，
以实际项目配置为准。下表的“毫秒”与 Worker 命令的“秒”不要混用。

| 配置项 | 默认值 | 中文说明与限制 |
| --- | --- | --- |
| `host` | `127.0.0.1` | Redis 服务器地址。队列使用自己的专用连接。 |
| `port` | `6379` | Redis TCP 端口。 |
| `password` | `null` | Redis 认证密码，`null` 表示不认证；部署时通过环境变量读取真实密码。 |
| `database` | `0` | Redis 逻辑数据库编号，生产者、消费者及管理命令必须一致。 |
| `connect_timeout` | `3.0` | 连接超时，单位为**秒**；允许小数，必须是有限正数，不能设为 0。 |
| `read_timeout` | `5.0` | 命令读取超时，单位为**秒**；必须是有限正数，且 `read_timeout * 1000 > block_ms`。 |
| `prefix` | `think-queue` | Redis 键名前缀，用于隔离项目和环境；仅允许字母、数字、冒号、下划线及连字符。 |
| `queue` | `default` | 未显式指定队列时使用的队列名，允许通过入队参数或 `--queue` 选择其他队列。 |
| `retry_after_ms` | `90000` | 待确认消息空闲多久后允许被接管，单位为**毫秒**。必须满足 `timeout * 1000 < retry_after_ms`；不是异常重试延迟，也不会自动续租。 |
| `block_ms` | `1000` | 空闲阻塞读取的最长等待，单位为**毫秒**，范围 0 至 5000；0 表示不阻塞，CLI 空闲时仍会短暂休眠。 |
| `claim_interval_ms` | `1000` | 检查超时待确认消息的间隔，单位为**毫秒**，必须大于 0；接管仍需满足 `retry_after_ms`。 |
| `promotion_batch` | `100` | 单次把到期延迟任务搬入就绪 Stream 的数量上限，必须大于 0。 |
| `promotion_interval_ms` | `1000` | 检查到期延迟任务的间隔，单位为**毫秒**，必须大于 0；实际延迟精度受消费者运行和负载影响。 |
| `heartbeat_interval_ms` | `10000` | 消费循环更新心跳的间隔，单位为**毫秒**，必须大于 0；长任务期间可能不更新，不能只看心跳判断进程是否存活。 |
| `restart_interval_ms` | `1000` | 检查重启标记的间隔，单位为**毫秒**，必须大于 0；发现标记后等待当前任务结束再退出。 |
| `max_active_jobs` | `0` | **每个队列**的活跃任务上限，0 表示不限；包含等待、延迟和处理中任务，不包含失败与隔离历史。 |
| `max_payload_bytes` | `1048576` | 单条完整 JSON 消息的字节上限，默认 1 MiB，0 表示不限；包含任务类、业务参数和元数据。 |
| `retry_jitter` | `true` | 异常重试是否增加随机抖动；false 为固定指数退避，主动 `release()` 不应用此抖动。 |

例如 `timeout=60` 表示 60 秒，`retry_after_ms=90000` 表示 90 秒；前者必须严格小于后者。
配置中的维护间隔不能保证任务准点执行；没有消费者运行时，到期延迟任务不会被搬运。
`max_active_jobs` 和 `max_payload_bytes` 分别限制数量和单条大小，不是整个 Redis 的内存上限；
失败和隔离历史还需按保留期单独清理。

## 消费与管理

```bash
# 持续消费 exports 队列；最多领取 3 次，单次硬超时 60 秒，异常重试从 3 秒开始退避。
php think queue:work --queue=exports --tries=3 --timeout=60 --delay=3
# 只进行一次领取尝试，队列为空也退出；异常重投不增加延迟，本进程仍只领取一次。
php think queue:work --queue=exports --once --delay=0

# 查看最多 100 条最终失败记录，输出包含 failure_id 和原始任务参数。
php think queue:manage failed --queue=exports --limit=100
# 重放指定失败记录：把示例 ID 替换为查询得到的 failure_id，不是业务 ID 或 job_id。
php think queue:manage retry 1234567890000-0 --queue=exports
# 删除指定失败记录；删除后不能再通过这条记录重放任务。
php think queue:manage forget 1234567890000-0 --queue=exports
# 更新当前 prefix 下所有队列消费者共享的重启标记；不会自动启动新的进程。
php think queue:manage restart

# 输出任务计数、待确认任务和消费者心跳的 JSON 快照。
php think queue:stats --queue=exports --json
# 查看心跳列表的下一页；分页不会改变队列消息，offset 是列表偏移量。
php think queue:stats --queue=exports --json --limit=100 --offset=100
# 查看被隔离的坏消息，修复生产者后再发布合法任务。
php think queue:manage quarantined --queue=exports --limit=100
# 删除指定隔离记录；使用查询返回的 quarantine_id。
php think queue:manage forget-quarantine 1234567890000-0 --queue=exports

# 预览 7 天前的失败记录，每次最多 100 条；604800 的单位为秒，预览不会删除数据。
php think queue:manage prune --queue=exports --kind=failed \
  --older-than=604800 --limit=100 --dry-run
# 预览 30 天前的隔离记录；确认候选后去掉 --dry-run 才会实际删除。
php think queue:manage prune --queue=exports --kind=quarantined \
  --older-than=2592000 --limit=100 --dry-run
# 清理心跳至少 5 分钟未更新且没有 Pending 的消费者元数据；300000 的单位为毫秒。
# 将名称替换为 stats 输出的真实消费者名称；不会强制结束进程或删除待执行任务。
php think queue:manage cleanup-consumer '统计返回的消费者名称' --queue=exports --idle-ms=300000
```

### 消费者参数

| 参数 | 默认值 | 中文说明 |
| --- | --- | --- |
| `--queue` | 配置中的 `queue` | 本进程消费的队列名，应与生产者发布时指定的名称一致。 |
| `--once` | 未启用 | 只完成一次领取尝试后退出；队列为空时也退出，不会排空整个队列。 |
| `--tries` | `3` | 默认最大领取次数，包含首次、主动重投和崩溃后的接管；任务的 `max_tries` 优先。 |
| `--timeout` | `60` | 默认单次硬超时，单位为秒，任务的 `timeout` 优先；必须严格小于 `retry_after_ms / 1000`。 |
| `--delay` | `3` | 异常重试初始退避，单位为秒，允许 0 至 3600；0 表示不增加退避等待。 |
| `--memory` | `128` | 进程内存退出阈值，按 MiB 换算，必须为正整数。 |
| `--max-jobs` | `1000` | 本进程领取处理的次数上限，包含失败任务，达到后退出，必须为正整数。 |
| `--max-time` | `3600` | 本进程最长运行时间，单位为秒，必须为正整数。 |

内存、累计任务数和进程运行时间阈值在任务之间检查；当前任务的硬超时由 `timeout` 控制。
消费者退出后需由进程管理器自动拉起，`queue:manage restart` 本身不负责拉起进程。

### 管理与统计参数

| 参数 | 使用位置 | 中文说明 |
| --- | --- | --- |
| `--queue` | `queue:manage`、`queue:stats` | 操作指定队列，省略时使用默认队列；`restart` 使用共享前缀，不按此参数限制单个队列。 |
| `--limit` | 查询、清理、统计 | 单次记录数或心跳分页条数，默认 100，范围 1 至 1000；不是待执行任务总数上限。 |
| `--start` | `failed`、`quarantined` | 从指定 Stream ID 开始查询，默认 `-` 表示最早记录；用 `(ID` 排除上一页末条记录。 |
| `--kind` | `prune` | 清理类型：`failed` 为失败记录，`quarantined` 为隔离记录，默认 `failed`。 |
| `--older-than` | `prune` | 按记录 Stream ID 的创建时间筛选早于指定年龄的记录，单位为秒，必须显式提供正整数。 |
| `--dry-run` | `prune` | 仅返回候选记录 ID，不执行删除；省略此参数会实际删除候选记录。 |
| `--idle-ms` | `cleanup-consumer` | 最小心跳年龄，单位为毫秒，必须显式指定且不小于 `max(retry_after_ms, heartbeat_interval_ms * 2)`。 |
| `--json` | `queue:stats` | 输出完整 JSON，包含心跳明细和首条 Pending 信息；不带此参数只输出主要计数。 |
| `--offset` | `queue:stats` | 心跳列表分页偏移，默认 0，必须为非负整数；下一页可使用返回的 `next_offset`。 |

失败记录查询为 JSON Lines，`failure_id` 是重放或删除命令所需的 Stream ID，
`job_id` 是跨重投保持不变的任务 ID。重放成功后移除原失败记录，将同一 `job_id` 重新入队，
领取次数从 0 重新计数；容量或字节检查不通过时保留失败记录。
例如上一页最后一个 `failure_id` 为 `1234567890000-0`，下一页传 `--start='(1234567890000-0'`；
开头的 `(` 表示不包含该条记录，参数末尾无需补右括号。

隔离记录使用独立 Stream，包含 `quarantine_id`、`job_id`、原始 `payload`、`raw_entry`、
错误原因和来源。消费端会隔离非法 JSON、不支持的版本、字段类型错误、任务 ID 不匹配、
缺少消息体及超大消息；延迟搬运会隔离缺少消息体的记录并继续搬运正常任务。
每次领取最多处理 100 条坏记录，避免无界循环。隔离记录不直接重放，应先检查并修复
生产者，再用合法参数重新发布任务。CLI 展示无法编码的字节时会替换字符，Redis 中保留原始字节。

`queue:stats` 提供同一次 Lua 执行内的计数快照：`active`、`waiting`、`pending`、`delayed`、
`failed`、`quarantined`、`first_pending`、`consumers` 等。`waiting` 是本包单组、确认后删除
约定下的 `ready_entries - pending`，不依赖 Redis 7 的 lag；`first_pending` 按消息 ID 排序，
其 idle 不是整个 PEL 的最大 idle。`counts_consistent` 仅检查活跃消息数与 ready/delayed 数量关系。
心跳按最近时间分页，`next_offset` 为下一页偏移；页间可能变化，心跳年龄也不等同于进程存活证明。

| 统计字段 | 中文含义 |
| --- | --- |
| `queue` / `group` | 队列名称 / 消费组名称。 |
| `sampled_at_ms` | Redis 服务端采样时间，单位为毫秒时间戳。 |
| `active` | 活跃任务数，包含等待、延迟和处理中任务。 |
| `ready_entries` | 就绪 Stream 中的记录数，包含未确认的 Pending。 |
| `waiting` | 尚未领取的就绪任务数，按 `ready_entries - pending` 计算。 |
| `pending` | 已被领取、尚未确认的任务数。 |
| `delayed` | 延迟集合中的任务数，包含等待到期的主动重投或异常重试。 |
| `failed` / `quarantined` | 最终失败记录数 / 隔离记录数，需要显式维护。 |
| `consumer_count` / `heartbeat_count` | Redis 消费组中的消费者数 / 本包登记的心跳记录数，两者可能不同。 |
| `consumers` | 当前页的消费者名称、最后心跳时间和心跳年龄；字段末尾 `_ms` 均以毫秒计。 |
| `first_pending` | 消息 ID 最小的一条 Pending，包含消费者、空闲毫秒数和 Redis 记录的投递次数；不是最大空闲时间。 |
| `next_delayed_at_ms` | 最早一条延迟任务的到期时间，单位为毫秒时间戳；没有延迟任务时为 null。 |
| `next_offset` | 下一页心跳偏移，为 null 时没有下一页。 |
| `counts_consistent` | `active == ready_entries + delayed` 的数量一致性检查，不代表业务成功或 Redis 持久性。 |

失败和隔离记录默认永久保留，不自动删除。`prune` 按记录 Stream ID 的创建时间筛选，
要求显式给出正整数 `--older-than` 秒数；`--limit` 最大 1000。可用部署平台定时调用该命令。
`cleanup-consumer` 只处理本包登记的指定消费者，在同一个 Lua 中检查心跳过期和无 Pending；
`idle-ms` 不得小于 `retry_after_ms` 与两倍 `heartbeat_interval_ms` 中的较大值。

Worker 默认处理 1000 个任务、运行 3600 秒或使用 128 MB 内存后退出，可分别用
`--max-jobs`、`--max-time`、`--memory` 配置。使用 Supervisor/systemd 自动拉起；
SIGTERM/SIGINT 与 restart 标记会让消费者在当前任务完成后退出。Redis 连接异常会使
消费者退出，由进程管理器重新启动，不盲目重发结果不明的写命令。

`--delay` 是重试初始秒数，异常重试采用指数退避，最长 3600 秒。
默认开启 equal jitter：从指数退避上限的一半（向上取整）至该上限随机选择整数秒；
`retry_jitter=false` 恢复固定指数退避，零延迟保持为零。
`--tries` 表示最大领取次数；强制退出的任务也消耗一次领取次数，达到上限后的恢复领取
仅负责转入失败队列，不再运行处理器。主动重投同样受此限制。

执行 `failed()` 回调前先保存失败记录。如果处理器尚未启动就因超限失败，无法调用尚未
创建的处理器回调；管理命令中的失败记录是最终状态的查询来源。

## 交付语义与边界

- 按至少一次投递设计。业务成功而 ACK 前进程退出时仍会重复处理，业务幂等必须自行实现。
- 数据库提交与消息入队不是同一事务。要求二者一致的业务应实现事务发件箱等机制。
- 即时任务使用 `XREADGROUP`，Pending 任务通过 `XAUTOCLAIM` 接管；每个消费者名称唯一。
- 延迟任务与重试任务使用 ZSet；消费者按维护间隔有界迁移到期任务，不循环重投未来消息。
- 延迟精度受 `block_ms`、Worker 负载影响；没有消费者运行时不会迁移，但任务仍保留在 ZSet。
- ACK、重投、失败迁移都会校验 PEL 当前消费者和本次租约 token，阻止旧 Worker 错误确认。
- Lua 对相关状态变更提供互斥执行，写入前检查键类型；Lua 本身不提供运行时错误回滚。
- Redis 服务端持久化、内存淘汰与复制故障仍影响数据持久性；不要把队列与可随意淘汰的缓存混用。
- 活动任务完成后即删除消息体、尝试次数和租约。失败和隔离消息保留到显式维护，无默认过期。
- 不要对 ready Stream 进行外部 `XTRIM/MAXLEN`、改消费组或手动删除待处理消息。
- 第一版不提供自动续租。`timeout * 1000` 必须严格小于 `retry_after_ms`；长任务应拆分或提高二者。
- Worker 超时直接以 124 退出并保留 PEL，由其他消费者在超时后恢复。业务外部操作仍须有自身超时。
- 每次创建新的任务类实例；长期持有的应用单例服务仍应自行避免跨任务残留业务状态。
- 正常退出原子清理空消费者元数据；崩溃遗留记录可通过 stats 和 cleanup-consumer 检查处理。

维护配置均使用毫秒：`promotion_interval_ms=1000`、`claim_interval_ms=1000`、
`heartbeat_interval_ms=10000`、`restart_interval_ms=1000`。即时消息通过阻塞读取唤醒，
不追加固定空闲休眠；`block_ms=0` 的非阻塞 CLI 保留短暂休眠。读取会在下一次维护期限前
返回；到期搬运也会考虑已知的下一条延迟任务。重启标记最长在下一个检查间隔及当前任务结束后生效。
Lua 使用 `EVALSHA`，只有明确的 `NOSCRIPT` 才加载并重发；连接超时不触发写操作重试。
`connect_timeout`、`read_timeout` 必须为有限正秒数；读取超时在认证前生效，并必须大于 `block_ms`。

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
sh scripts/test-docker.sh 8.2 6.2
sh scripts/test-docker.sh 8.4 7.4
```

GitHub Actions 配置 PHP 8.2/8.3/8.4 × Redis 6.2/7.4。测试覆盖立即/延迟投递、
重投计数、最终失败及回放、多 PHP 进程竞争、SIGKILL 恢复、任务超时、旧消费者处理权、
确认前断连、ACK 已执行但响应丢失、Lua 键类型校验和 ThinkPHP 命令集成。
还覆盖坏消息与缺失消息体隔离、并发生产者容量限制、失败重放背压、统计与心跳维护、
清理预览与数量上限、脚本缓存失效、密码字符串 `0` 和认证/SELECT 读取超时。
任务 ID 测试覆盖 32 位随机格式、强制碰撞与重试上限、并发生产者查重、内容冲突、
旧 ID 兼容，以及入队已执行但响应丢失时不自动换 ID 重发。

可在独立 Redis 上执行可重复的单进程诊断基准：

```bash
REDIS_HOST=127.0.0.1 php scripts/benchmark.php --jobs=1000 --payload-bytes=256
```

脚本先批量入队再消费，输出生产/消费速率、排队等待 p50/p95 和 PHP 峰值内存，并校验
任务没有缺失或重复。每次使用随机前缀，最后只清理自身数据。排队等待包含批量投递时间，
内存也包含测量开销；该结果不能代表多进程生产环境吞吐或实时到达的端到端延迟。

CI 通过不等同于生产故障演练。Redis 主从切换、OOM、网络在响应返回前断开、长期高负载
以及实际业务幂等仍需部署环境验收。

## 许可证

Apache-2.0。API 设计参考 topthink/think-queue，来源说明见 [NOTICE](NOTICE)。
