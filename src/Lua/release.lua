local group, message, consumer, token, id, delay = ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], tonumber(ARGV[6])
if not owns(group, message, consumer, token) then return 0 end
if delay > 0 then
    redis.call('ZADD', KEYS[2], now_ms() + delay, id)
else
    redis.call('XADD', KEYS[1], '*', 'job_id', id)
end
finish(group, message)
return 1
