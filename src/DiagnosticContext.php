<?php
declare(strict_types=1);

namespace ThinkQueue\Streams;

use Throwable;
use ThinkQueue\Streams\Contracts\ProvidesFailureContext;

/** 有界的业务引用摘要和安全异常上下文，不序列化任意对象或堆栈参数。 */
final class DiagnosticContext
{
    public const DEFAULT_PARAMETER_KEYS = ['id', '*_id', '*_ids', 'business_type', 'type', 'limit'];
    private array $parameterKeys;
    private int $maxBytes;

    /**
     * 创建诊断上下文策略，敏感字段的屏蔽优先于白名单。
     * @param array<string> $parameterKeys 允许记录的参数键或点号路径，支持星号
     * @param int $maxBytes 参数 JSON 字节上限，限制在 128 至 8192 字节
     */
    public function __construct(array $parameterKeys = self::DEFAULT_PARAMETER_KEYS, int $maxBytes = 2048)
    {
        $this->parameterKeys = array_slice(array_values(array_filter($parameterKeys,
            static fn ($key): bool => is_string($key) && strlen($key) <= 64)), 0, 64);
        $this->maxBytes = max(128, min(8192, $maxBytes));
    }

    /**
     * 生成可定位数据的 JSON 摘要，限制节点数、层数、字段数和字符串长度。
     * @param mixed $data 任务参数
     * @return array<string,mixed> params 及可选的 params_truncated 标记
     */
    public function parameters(mixed $data): array
    {
        $budget = 64;
        $truncated = false;
        $filtered = $this->filter($data, '', false, 0, $budget, $truncated);
        $json = self::encode($filtered);
        while (strlen($json) > $this->maxBytes) {
            $truncated = true;
            if (is_array($filtered) && $filtered !== []) array_pop($filtered);
            else $filtered = '[TRUNCATED]';
            $json = self::encode($filtered);
        }
        return ['params' => $json] + ($truncated ? ['params_truncated' => 1] : []);
    }

    /**
     * 提取异常位置和业务显式提供的安全说明；不自动输出任意第三方异常原文。
     * @param Throwable $exception 当前异常
     * @return array<string,mixed> 可用于日志的异常上下文
     */
    public static function exception(Throwable $exception): array
    {
        $context = ['exception' => $exception::class, 'error_code' => 'UNHANDLED_EXCEPTION',
            'error_message' => '任务执行异常，请根据异常位置和业务参数排查',
            'exception_file' => $exception->getFile(), 'exception_line' => $exception->getLine()];
        if ($exception instanceof ProvidesFailureContext) {
            try {
                $safe = $exception->queueFailureContext();
                foreach (['error_code', 'error_message', 'failure_stage', 'smtp_code', 'cause_exception', 'cause_file', 'cause_line'] as $key) {
                    if (isset($safe[$key]) && is_scalar($safe[$key])) $context[$key] = $safe[$key];
                }
            } catch (Throwable) { /* 诊断提供器失败不能改变任务状态。 */ }
        }
        return $context;
    }

    /**
     * 递归筛选白名单引用，敏感字段及未开放的叶子值固定隐藏。
     * @param mixed $value 当前参数
     * @param string $path 参数路径
     * @param bool $allowed 父级是否允许记录
     * @param int $depth 当前深度
     * @param int $budget 剩余节点预算
     * @param bool $truncated 是否截断
     * @return mixed 脱敏后的参数
     */
    private function filter(mixed $value, string $path, bool $allowed, int $depth, int &$budget, bool &$truncated): mixed
    {
        if (--$budget < 0 || $depth > 4) { $truncated = true; return '[TRUNCATED]'; }
        if (!is_array($value)) {
            if (!$allowed || (!is_scalar($value) && $value !== null)) return '[REDACTED]';
            if (is_float($value) && !is_finite($value)) return '[NON_FINITE]';
            if (is_string($value) && strlen($value) > 128) { $truncated = true; return substr($value, 0, 128) . '[TRUNCATED]'; }
            return $value;
        }
        $result = [];
        foreach ($value as $key => $child) {
            if (count($result) >= 20 || $budget <= 0) { $truncated = true; break; }
            $name = (string) $key;
            $childPath = $path === '' ? $name : $path . '.' . $name;
            $outputKey = is_int($key) || preg_match('/^[A-Za-z_][A-Za-z0-9_.-]{0,63}$/D', $name) ? $key : '[key]';
            $normalized = strtolower((string) preg_replace('/[^a-z0-9]/i', '', $name));
            if (in_array($normalized, ['auth', 'headers', 'header', 'key', 'pass', 'passphrase', 'signature', 'sign'], true) ||
                preg_match('/password|passwd|pwd|token|secret|credential|authorization|cookie|session|apikey|privatekey|body|content|html|attachment|recipient|email|phone|mobile|idcard|passport/', $normalized)) {
                $result[$outputKey] = '[REDACTED]';
                continue;
            }
            $childAllowed = is_int($key) && $allowed;
            foreach ($this->parameterKeys as $pattern) {
                if (fnmatch($pattern, $name) || fnmatch($pattern, $childPath)) { $childAllowed = true; break; }
            }
            $result[$outputKey] = $this->filter($child, $childPath, $childAllowed, $depth + 1, $budget, $truncated);
        }
        return $result;
    }

    /**
     * 编码为单行 JSON，非法 UTF-8 用替代字符表示。
     * @param mixed $value 已筛选的数据
     * @return string JSON 文本
     */
    private static function encode(mixed $value): string
    {
        return json_encode($value, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE) ?: 'null';
    }
}
