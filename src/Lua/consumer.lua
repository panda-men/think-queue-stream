local action, consumer = ARGV[1], ARGV[2]
local now = now_ms()
if action == 'heartbeat' then
    redis.call('ZADD', KEYS[8], now, consumer)
    return 1
end
if action == 'cleanup' then
    local last = redis.call('ZSCORE', KEYS[8], consumer)
    if not last or now - tonumber(last) < tonumber(ARGV[3]) then return 0 end
end
local pending = redis.pcall('XPENDING', KEYS[1], 'workers', '-', '+', 1, consumer)
if not pending.err and #pending > 0 then return 0 end
if pending.err and not string.find(pending.err, 'NOGROUP', 1, true) then return redis.error_reply(pending.err) end
if not pending.err then redis.call('XGROUP', 'DELCONSUMER', KEYS[1], 'workers', consumer) end
return redis.call('ZREM', KEYS[8], consumer)
