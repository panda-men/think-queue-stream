-- 提前检查键类型；Lua 发生运行时错误不会回滚已经完成的写入。
local expected = {'stream', 'zset', 'hash', 'hash', 'hash', 'stream', 'stream', 'zset', 'hash'}
for i, kind in ipairs(expected) do
    local actual = redis.call('TYPE', KEYS[i]).ok
    if actual ~= 'none' and actual ~= kind then
        return redis.error_reply('QUEUE_WRONGTYPE ' .. KEYS[i])
    end
end
local function admission(payload, maximum, bytes)
    if tonumber(bytes) > 0 and string.len(payload) > tonumber(bytes) then
        return 'QUEUE_PAYLOAD_TOO_LARGE'
    end
    if tonumber(maximum) > 0 and redis.call('HLEN', KEYS[3]) >= tonumber(maximum) then
        return 'QUEUE_FULL'
    end
end
local function now_ms()
    local t = redis.call('TIME')
    return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end
local function owns(group, message, consumer, token)
    local pending = redis.call('XPENDING', KEYS[1], group, message, message, 1)
    return #pending == 1 and pending[1][2] == consumer
        and redis.call('HGET', KEYS[5], message) == token
end
local function finish(group, message)
    redis.call('XACK', KEYS[1], group, message)
    redis.call('XDEL', KEYS[1], message)
    redis.call('HDEL', KEYS[5], message)
end
