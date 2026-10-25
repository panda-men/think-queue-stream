<?php
declare(strict_types=1);

namespace ThinkQueue\Streams;

use Throwable;
use ThinkQueue\Streams\Contracts\ProvidesFailureContext;

/** 完整的任务参数 JSON 和业务显式提供的异常上下文。 */
final class DiagnosticContext
{
    /**
     * 完整编码任务参数，不按字段筛选或截断内容。
     * @param mixed $data 任务参数
     * @return array<string,string> params 参数 JSON
     */
    public function parameters(mixed $data): array
    {
        return ['params' => self::encode($data)];
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
     * 编码为单行 JSON；不可编码的值使用 JSON 部分输出，编码器异常不影响任务结算。
     * @param mixed $value 原始任务参数
     * @return string JSON 文本
     */
    private static function encode(mixed $value): string
    {
        try {
            // 使用 JSON 编码器允许的最大深度，不额外设置日志层数限制。
            $json = json_encode($value, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES |
                JSON_INVALID_UTF8_SUBSTITUTE | JSON_PARTIAL_OUTPUT_ON_ERROR, 2147483647);
            return $json === false ? 'null' : $json;
        } catch (Throwable) {
            return 'null';
        }
    }
}
