local group, message, consumer, token, id = ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5]
if not owns(group, message, consumer, token) then return 0 end
finish(group, message)
redis.call('HDEL', KEYS[3], id)
redis.call('HDEL', KEYS[4], id)
return 1
