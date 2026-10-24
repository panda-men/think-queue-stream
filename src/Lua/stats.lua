local now = now_ms()
local pending, consumers = 0, 0
if redis.call('EXISTS', KEYS[1]) == 1 then
    for _, info in ipairs(redis.call('XINFO', 'GROUPS', KEYS[1])) do
        local group = {}
        for i = 1, #info, 2 do group[info[i]] = info[i + 1] end
        if group.name == 'workers' then pending = group.pending; consumers = group.consumers end
    end
end
local first = {}
if pending > 0 then first = redis.call('XPENDING', KEYS[1], 'workers', '-', '+', 1) end
local recent = redis.call('ZREVRANGE', KEYS[8], ARGV[2], tonumber(ARGV[2]) + tonumber(ARGV[1]) - 1, 'WITHSCORES')
local next = redis.call('ZRANGE', KEYS[2], 0, 0, 'WITHSCORES')
return {now, redis.call('HLEN', KEYS[3]), redis.call('XLEN', KEYS[1]), pending,
    redis.call('ZCARD', KEYS[2]), redis.call('XLEN', KEYS[6]), redis.call('XLEN', KEYS[7]),
    consumers, first, recent, redis.call('ZCARD', KEYS[8]), next[2] or ''}
