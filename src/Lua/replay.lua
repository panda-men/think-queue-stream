local failure, id, payload = ARGV[1], ARGV[2], ARGV[3]
-- 数据库事务可能在 Redis 发布成功后回滚或断线；回执保留到数据库确认删除。
local replayed = redis.call('HGET', KEYS[9], failure)
if replayed then return replayed end
if redis.call('HEXISTS', KEYS[3], id) == 1 then
    return redis.error_reply('QUEUE_JOB_ALREADY_ACTIVE ' .. id)
end
local error = admission(payload, ARGV[4], ARGV[5])
if error then return redis.error_reply(error) end
redis.call('HSET', KEYS[3], id, payload)
redis.call('HSET', KEYS[4], id, 0)
redis.call('XADD', KEYS[1], '*', 'job_id', id)
redis.call('HSET', KEYS[9], failure, id)
return id
