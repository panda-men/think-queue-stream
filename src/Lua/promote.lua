local ids = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now_ms(), 'LIMIT', 0, ARGV[1])
for _, id in ipairs(ids) do
    if redis.call('HEXISTS', KEYS[3], id) == 0 then
        return redis.error_reply('QUEUE_MISSING_PAYLOAD ' .. id)
    end
end
for _, id in ipairs(ids) do
    redis.call('XADD', KEYS[1], '*', 'job_id', id)
    redis.call('ZREM', KEYS[2], id)
end
return #ids
