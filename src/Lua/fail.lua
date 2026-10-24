local group, message, consumer, token, id = ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5]
if not owns(group, message, consumer, token) then return nil end
local payload = redis.call('HGET', KEYS[3], id)
if not payload then return redis.error_reply('QUEUE_MISSING_PAYLOAD ' .. id) end
local failure = redis.call('XADD', KEYS[6], '*', 'job_id', id, 'payload', payload,
    'attempts', redis.call('HGET', KEYS[4], id), 'error', ARGV[6], 'failed_at_ms', tostring(now_ms()))
finish(group, message)
redis.call('HDEL', KEYS[3], id)
redis.call('HDEL', KEYS[4], id)
return failure
