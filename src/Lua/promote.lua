local now = now_ms()
local moved = 0
local ids = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, ARGV[1])
for _, id in ipairs(ids) do
    if redis.call('HEXISTS', KEYS[3], id) == 0 then
        redis.call('XADD', KEYS[7], '*', 'job_id', id, 'payload', '', 'raw_entry', '{}',
            'source', 'delayed', 'message_id', '', 'error', 'QUEUE_MISSING_PAYLOAD',
            'quarantined_at_ms', tostring(now))
        redis.call('HDEL', KEYS[4], id)
    else
        redis.call('XADD', KEYS[1], '*', 'job_id', id)
        moved = moved + 1
    end
    redis.call('ZREM', KEYS[2], id)
end
local next = redis.call('ZRANGE', KEYS[2], 0, 0, 'WITHSCORES')
return {moved, next[2] or '', now}
