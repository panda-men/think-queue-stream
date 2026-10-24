local entries = redis.call('XRANGE', KEYS[6], ARGV[1], ARGV[1], 'COUNT', 1)
if #entries == 0 then return nil end
local data = {}
for i = 1, #entries[1][2], 2 do data[entries[1][2][i]] = entries[1][2][i + 1] end
if redis.call('HEXISTS', KEYS[3], data.job_id) == 1 then
    return redis.error_reply('QUEUE_JOB_ALREADY_ACTIVE ' .. data.job_id)
end
redis.call('HSET', KEYS[3], data.job_id, data.payload)
redis.call('HSET', KEYS[4], data.job_id, 0)
redis.call('XADD', KEYS[1], '*', 'job_id', data.job_id)
redis.call('XDEL', KEYS[6], ARGV[1])
return data.job_id
