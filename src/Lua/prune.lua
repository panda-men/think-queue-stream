local key = KEYS[tonumber(ARGV[1])]
local before = now_ms() - tonumber(ARGV[2]) * 1000
if before <= 0 then return {} end
local entries = redis.call('XRANGE', key, '-', '(' .. string.format('%.0f', before) .. '-0', 'COUNT', ARGV[3])
local ids = {}
for _, entry in ipairs(entries) do ids[#ids + 1] = entry[1] end
if ARGV[4] ~= '1' and #ids > 0 then redis.call('XDEL', key, unpack(ids)) end
return ids
