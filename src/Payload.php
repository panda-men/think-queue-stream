<?php
declare(strict_types=1);

namespace ThinkQueue\Streams;

use UnexpectedValueException;

final class Payload
{
    /** 解码并校验协议；不实例化来自消息的 PHP 对象。 */
    public static function decode(string $json, string $id): array
    {
        $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        if (!is_array($data) || ($data['version'] ?? null) !== 1) {
            throw new UnexpectedValueException('消息协议版本不受支持。');
        }
        if (!is_string($data['id'] ?? null) || $data['id'] !== $id || !self::validId($id)) {
            throw new UnexpectedValueException('消息任务 ID 不合法或与 Stream 不一致。');
        }
        if (!is_string($data['job'] ?? null) || !self::validJob($data['job']) ||
            !array_key_exists('data', $data) || !is_int($data['created_at'] ?? null) || $data['created_at'] < 0) {
            throw new UnexpectedValueException('消息缺少合法的 job、data 或 created_at。');
        }
        foreach (['max_tries', 'timeout'] as $name) {
            if (!array_key_exists($name, $data) ||
                ($data[$name] !== null && (!is_int($data[$name]) || $data[$name] < 1))) {
                throw new UnexpectedValueException("消息 {$name} 必须为正整数或 null。");
            }
        }
        return $data;
    }

    public static function validId(string $id): bool
    {
        return preg_match('/\A[a-zA-Z0-9_-]{1,128}\z/D', $id) === 1;
    }

    public static function validJob(string $job): bool
    {
        return preg_match('/\A\\\\?[a-zA-Z_][a-zA-Z0-9_\\\\]*(?:@[a-zA-Z][a-zA-Z0-9_]*)?\z/D', $job) === 1;
    }

    /** 比较任务内容与执行选项；忽略创建时间，JSON 对象键序不影响去重。 */
    public static function sameTask(string $stored, string $incoming): bool
    {
        try {
            $storedPayload = json_decode($stored, false, 512, JSON_THROW_ON_ERROR);
            $incomingPayload = json_decode($incoming, false, 512, JSON_THROW_ON_ERROR);
            if (!$storedPayload instanceof \stdClass || !$incomingPayload instanceof \stdClass ||
                !isset($incomingPayload->id) || !is_string($incomingPayload->id)) {
                return false;
            }
            self::decode($stored, $incomingPayload->id);
            $left = $right = [];
            foreach (['version', 'id', 'job', 'data', 'max_tries', 'timeout'] as $field) {
                if (!property_exists($storedPayload, $field) || !property_exists($incomingPayload, $field)) {
                    return false;
                }
                $left[$field] = self::canonicalize($storedPayload->$field);
                $right[$field] = self::canonicalize($incomingPayload->$field);
            }
            return json_encode($left, JSON_THROW_ON_ERROR) === json_encode($right, JSON_THROW_ON_ERROR);
        } catch (\JsonException|UnexpectedValueException) {
            // 损坏的旧消息不能被当作成功去重的新任务。
            return false;
        }
    }

    private static function canonicalize(mixed $value): mixed
    {
        if ($value instanceof \stdClass) {
            $properties = get_object_vars($value);
            ksort($properties, SORT_STRING);
            $result = new \stdClass();
            foreach ($properties as $key => $item) {
                $result->$key = self::canonicalize($item);
            }
            return $result;
        }
        return is_array($value) ? array_map(self::canonicalize(...), $value) : $value;
    }
}
