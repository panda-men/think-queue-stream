<?php
declare(strict_types=1);

namespace ThinkQueue\Streams\Tests;

use PHPUnit\Framework\TestCase;
use ThinkQueue\Streams\DiagnosticContext;
use ThinkQueue\Streams\Tests\Fixtures\DiagnosticFailure;

final class DiagnosticContextTest extends TestCase
{
    /**
     * 嵌套业务引用可见，敏感字段和未开放字段隐藏。
     * @return void
     */
    public function testReferenceParametersAndSensitiveFields(): void
    {
        $result = (new DiagnosticContext())->parameters(['mail_task_id' => 10004, 'order_ids' => [10001, 10002],
            'nested' => ['support_contract_order_id' => 500, 'access_token' => 'TOKEN_SECRET'],
            'password_id' => 'PASSWORD_SECRET', 'recipient' => 'receiver@example.test', 'body' => 'BODY_SECRET',
            'unapproved' => 'PRIVATE_VALUE']);
        $data = json_decode($result['params'], true, 512, JSON_THROW_ON_ERROR);
        self::assertSame(10004, $data['mail_task_id']);
        self::assertSame([10001, 10002], $data['order_ids']);
        self::assertSame(500, $data['nested']['support_contract_order_id']);
        foreach (['TOKEN_SECRET', 'PASSWORD_SECRET', 'receiver@example.test', 'BODY_SECRET', 'PRIVATE_VALUE'] as $secret) {
            self::assertStringNotContainsString($secret, $result['params']);
        }
        $all = (new DiagnosticContext(['*']))->parameters(['password' => 'SECRET', 'body' => 'BODY', 'tracking_no' => 'TRACK-1']);
        self::assertStringContainsString('TRACK-1', $all['params']);
        self::assertStringNotContainsString('SECRET', $all['params']);
        self::assertStringNotContainsString('BODY', $all['params']);
    }

    /**
     * 深层、大量、非法 UTF-8 及对象参数保持有界且返回合法单行 JSON。
     * @return void
     */
    public function testBoundedParametersAndObjectsAreNotSerialized(): void
    {
        $object = new class {
            /** 禁止日志触发对象序列化。 */
            public function __serialize(): array { throw new \RuntimeException('不应执行'); }
        };
        $data = ['mail_task_id' => 10004, 'name' => str_repeat('中文', 1000), 'object' => $object,
            'nested' => ['a' => ['b' => ['c' => ['d' => ['id' => 100]]]]], 'invalid_id' => "x\xff\n"];
        $result = (new DiagnosticContext(['*'], 128))->parameters($data);
        self::assertLessThanOrEqual(128, strlen($result['params']));
        self::assertSame(1, $result['params_truncated']);
        self::assertIsArray(json_decode($result['params'], true, 512, JSON_THROW_ON_ERROR));
        self::assertStringNotContainsString("\n", $result['params']);
        $result = (new DiagnosticContext())->parameters(['ids' => array_fill(0, 10000, ['id' => 1])]);
        self::assertSame(1, $result['params_truncated']);
        self::assertLessThanOrEqual(2048, strlen($result['params']));
        $result = (new DiagnosticContext())->parameters(['report_id' => 10001, 'invalid_id' => NAN]);
        $data = json_decode($result['params'], true, 512, JSON_THROW_ON_ERROR);
        self::assertSame(10001, $data['report_id']);
        self::assertSame('[NON_FINITE]', $data['invalid_id']);
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
