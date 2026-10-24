<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Tests\Fixtures;

use ThinkQueue\Streams\Contracts\ProvidesFailureContext;

final class DiagnosticFailure extends \RuntimeException implements ProvidesFailureContext
{
    /**
     * 提供可公开的业务错误，不暴露底层认证对话。
     * @return array<string,mixed> 安全诊断字段
     */
    public function queueFailureContext(): array
    {
        return ['error_code' => 'REPORT_NOT_FOUND', 'error_message' => '报表不存在，请核对报表编号'];
    }
}
