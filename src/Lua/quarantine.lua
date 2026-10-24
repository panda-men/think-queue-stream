local group, message, consumer, token, id = ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5]
local pending = redis.call('XPENDING', KEYS[1], group, message, message, 1)
if #pending ~= 1 or pending[1][2] ~= consumer then return 0 end
if token ~= '' and not owns(group, message, consumer, token) then return 0 end
redis.call('XADD', KEYS[7], '*', 'job_id', id,
    'payload', redis.call('HGET', KEYS[3], id) or '', 'raw_entry', ARGV[7],
    'source', 'ready', 'message_id', message, 'error', ARGV[6],
    'quarantined_at_ms', tostring(now_ms()))
finish(group, message)
if id ~= '' then
    redis.call('HDEL', KEYS[3], id)
    redis.call('HDEL', KEYS[4], id)
end
return 1
