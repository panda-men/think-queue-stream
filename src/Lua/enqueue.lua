local id, payload, delay = ARGV[1], ARGV[2], tonumber(ARGV[3])
-- 查重与创建在同一次脚本中完成；返回原消息快照，由调用方区分碰撞和主动去重。
local existing = redis.call('HGET', KEYS[3], id)
if existing then return {'exists', existing} end
local error = admission(payload, ARGV[4], ARGV[5])
if error then return redis.error_reply(error) end
redis.call('HSET', KEYS[3], id, payload)
redis.call('HSET', KEYS[4], id, 0)
if delay > 0 then
    redis.call('ZADD', KEYS[2], now_ms() + delay, id)
else
    redis.call('XADD', KEYS[1], '*', 'job_id', id)
end
return {'created', id}
