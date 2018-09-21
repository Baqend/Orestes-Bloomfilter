local QUEUE_KEY = KEYS[1]
local COUNTS_KEY = KEYS[2]
local BITS_KEY = KEYS[3]
local NOW = ARGV[1]

-- Find a good limit
local limit = 100

-- Get expired elements from Redis
local expiredElements = redis.call("ZRANGEBYSCORE", QUEUE_KEY, 0, NOW, "WITHSCORES", "LIMIT", 0, limit)
local length = #expiredElements
if length < 1 then return 0 end

local maxScore
local lastScore
for i = length - 1, 1, -2 do
    local msgPack = expiredElements[i]

    -- Retrieve max score and zrem scores
    if maxScore == nil then
        local score = tonumber(expiredElements[i + 1])
        if lastScore == nil or lastScore == score then
            lastScore = score
            redis.call("ZREM", QUEUE_KEY, msgPack)
        else
            maxScore = score
        end
    end

    -- Unpack the message pack, retrieve values
    local entry = cmsgpack.unpack(msgPack)

    -- Process positions
    for _, position in ipairs(entry.positions) do
        -- Encode the key
        local key = string.char(
            bit.rshift(position, 24),
            bit.band(bit.rshift(position, 16), 0xFF),
            bit.band(bit.rshift(position, 8), 0xFF),
            bit.band(position, 0xFF)
        )

        -- Decrement count
        local count = redis.call("HINCRBY", COUNTS_KEY, key, -1)

        -- If count is not positive, clear the bit at the given position
        if count <= 0 then
            redis.call("HDEL", COUNTS_KEY, key)
            redis.call("SETBIT", BITS_KEY, position, 0)
        end
    end
end

-- Remove elements from the queue
if maxScore ~= nil then redis.call("ZREMRANGEBYSCORE", QUEUE_KEY, 0, maxScore) end

return length / 2
