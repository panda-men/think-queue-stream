<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Contracts;

/** 由业务显式提供可公开的错误说明，禁止返回密码、正文、SQL 或原始协议对话。 */
interface ProvidesFailureContext
{
    /**
     * 返回已有的安全错误码、中文说明及可选诊断字段；不得执行数据库或网络访问。
     * @return array<string,mixed> error_code、error_message、failure_stage、smtp_code 或 cause_* 元数据
     */
    public function queueFailureContext(): array;
}
