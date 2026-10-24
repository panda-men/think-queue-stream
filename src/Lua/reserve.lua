local group, message, consumer, token, id = ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5]
local pending = redis.call('XPENDING', KEYS[1], group, message, message, 1)
if #pending ~= 1 or pending[1][2] ~= consumer then return nil end
local payload = redis.call('HGET', KEYS[3], id)
if not payload then return redis.error_reply('QUEUE_MISSING_PAYLOAD ' .. id) end
if redis.call('HGET', KEYS[5], message) ~= token then
    redis.call('HINCRBY', KEYS[4], id, 1)
    redis.call('HSET', KEYS[5], message, token)
end
return {payload, redis.call('HGET', KEYS[4], id)}
