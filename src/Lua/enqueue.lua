local id, payload, delay = ARGV[1], ARGV[2], tonumber(ARGV[3])
if redis.call('HEXISTS', KEYS[3], id) == 1 then return id end
redis.call('HSET', KEYS[3], id, payload)
redis.call('HSET', KEYS[4], id, 0)
if delay > 0 then
    redis.call('ZADD', KEYS[2], now_ms() + delay, id)
else
    redis.call('XADD', KEYS[1], '*', 'job_id', id)
end
return id
