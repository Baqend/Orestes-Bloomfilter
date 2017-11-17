local QUEUE_KEY = KEYS[1]
local COUNTS_KEY = KEYS[2]
local BITS_KEY = KEYS[3]
local now = ARGV[1]

-- Get expired elements from Redis
local expiredElements = redis.call("ZRANGEBYSCORE", QUEUE_KEY, 0, now)

local counts = {} -- Collect all counts
for _, msgPack in ipairs(expiredElements) do

    -- Remove element from the queue
    redis.call("ZREM", QUEUE_KEY, msgPack)

    -- Unpack the message pack, retrieve values
    local entry = cmsgpack.unpack(msgPack)
    local positions = entry.positions

    -- Decrement all counts and collect results in table
    for _, position in ipairs(positions) do
        counts[position] = redis.call("HINCRBY", COUNTS_KEY, position, -1)
    end
end

local minCount

-- Reset all positions in binary BF which have to be reset
for position, count in pairs(counts) do
    if minCount == nil or minCount > count then
        minCount = count
    end
    -- If count is not positive, clear the bit at the given position
    if count <= 0 then
        redis.call("SETBIT", BITS_KEY, position, 0)
    end
end

return minCount
