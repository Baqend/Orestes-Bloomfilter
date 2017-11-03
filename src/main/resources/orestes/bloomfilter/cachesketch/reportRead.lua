local newScore = tonumber(ARGV[1])
local element = ARGV[2]
local score = redis.call('zscore', KEYS[1], element)
if score == false or newScore > tonumber(score) then
    redis.call('zadd', KEYS[1], newScore, element)
end
