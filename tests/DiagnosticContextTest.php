<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Tests;

use PHPUnit\Framework\TestCase;
use ThinkQueue\Streams\DiagnosticContext;
use ThinkQueue\Streams\Tests\Fixtures\DiagnosticFailure;

final class DiagnosticContextTest extends TestCase
{
    /**
     * 所有字段完整记录，包括原敏感字段、嵌套参数及任意字段名。
     * @return void
     */
    public function testAllParametersAreRecordedWithoutRedaction(): void
    {
        $parameters = ['mail_task_id' => 10004, 'order_ids' => [10001, 10002],
            'nested' => ['support_contract_order_id' => 500, 'access_token' => 'TOKEN_SECRET'],
            'password_id' => 'PASSWORD_SECRET', 'recipient' => 'receiver@example.test', 'body' => 'BODY_SECRET',
            'auth' => 'AUTH_VALUE', 'headers' => ['Authorization' => 'Bearer TOKEN'], 'key' => 'KEY_VALUE',
            'signature' => 'SIGNATURE_VALUE', 'API_KEY' => 'API_VALUE', 'email' => 'user@example.test',
            'order_no' => 'ORD-1', '中文字段' => '完整保留', str_repeat('long_key_', 12) => 'LONG_KEY_VALUE'];
        $result = (new DiagnosticContext())->parameters($parameters);
        self::assertSame($parameters, json_decode($result['params'], true, 512, JSON_THROW_ON_ERROR));
        self::assertSame(['params'], array_keys($result));
    }

    /**
     * 长字符串、深层、宽数组及大量节点完整输出，不再截断。
     * @return void
     */
    public function testLargeAndNestedParametersAreNotTruncated(): void
    {
        $nested = ['value' => 'DEEPEST_VALUE'];
        for ($depth = 0; $depth < 600; $depth++) $nested = ['nested' => $nested];
        $parameters = ['body' => str_repeat("完整正文\n", 2000), 'nested' => $nested,
            'items' => array_fill(0, 100, ['id' => 1, 'name' => '完整节点']),
            'fields' => array_combine(range(1, 100), array_fill(0, 100, '完整字段'))];
        $result = (new DiagnosticContext())->parameters($parameters);
        self::assertSame($parameters, json_decode($result['params'], true, 1024, JSON_THROW_ON_ERROR));
        self::assertGreaterThan(8192, strlen($result['params']));
        self::assertSame(['params'], array_keys($result));
        self::assertStringNotContainsString('[TRUNCATED]', $result['params']);
        self::assertStringNotContainsString("\n", $result['params']);
    }

    /**
     * 标量参数保持原值，非法 UTF-8 和编码器故障不影响任务日志。
     * @return void
     */
    public function testScalarParametersAndEncodingFailures(): void
    {
        $diagnostics = new DiagnosticContext();
        foreach ([0, false, null, '完整字符串', ["invalid" => "x\xff\n"]] as $value) {
            $result = $diagnostics->parameters($value);
            self::assertSame(json_encode($value, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE), $result['params']);
        }
        $broken = new class implements \JsonSerializable {
            /** 模拟参数 JSON 编码失败。 */
            public function jsonSerialize(): mixed { throw new \RuntimeException('编码失败'); }
        };
        self::assertSame(['params' => 'null'], $diagnostics->parameters($broken));
        $recursive = ['id' => 10001];
        $recursive['self'] = &$recursive;
        $result = $diagnostics->parameters($recursive);
        self::assertSame(['id' => 10001, 'self' => null], json_decode($result['params'], true, 512, JSON_THROW_ON_ERROR));
    }

    /**
     * 只输出显式提供的安全错误说明，普通异常保留准确位置而不输出原文。
     * @return void
     */
    public function testSafeExceptionsAndUnknownExceptionLocation(): void
    {
        $error = new DiagnosticFailure('内部细节不应自动输出');
        $context = DiagnosticContext::exception($error);
        self::assertSame('REPORT_NOT_FOUND', $context['error_code']);
        self::assertSame('报表不存在，请核对报表编号', $context['error_message']);
        self::assertSame($error->getFile(), $context['exception_file']);
        self::assertSame($error->getLine(), $context['exception_line']);
        $unknown = DiagnosticContext::exception(new \RuntimeException('SMTP_PASSWORD_SECRET'));
        self::assertSame('UNHANDLED_EXCEPTION', $unknown['error_code']);
        self::assertStringNotContainsString('SMTP_PASSWORD_SECRET', json_encode($unknown));
    }
}
