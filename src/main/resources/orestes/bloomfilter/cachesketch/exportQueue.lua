local QUEUE_KEY = KEYS[1]
local COUNTS_KEY = KEYS[2]
local BITS_KEY = KEYS[3]
local now = ARGV[1]
local LIMIT = 16384

--[[
-- Returns a slice of a table.
--]]
local function slice(table, fromIndex, toIndex)
    local pos, new = 1, {}
    if toIndex > #table then
        toIndex = #table
    end

    for i = fromIndex, toIndex do
        new[pos] = table[i]
        pos = pos + 1
    end

    return new
end

--[[
-- Encodes an integer key as byte array.
--]]
local function encodeKey(int)
    local result = {}
    for i = 0, 3 do
        result[4 - i] = string.char(bit.band(bit.rshift(int, i * 8), 0xFF))
    end
    return table.concat(result)
end

--[[
-- Encodes many integers as byte arrays.
--]]
local function encodeKeys(ints)
    local result = {}
    for i, value in ipairs(ints) do
        result[i] = encodeKey(value)
    end
    return result
end

-- Get expired elements from Redis
local expiredElements = redis.call("ZRANGEBYSCORE", QUEUE_KEY, 0, now, "WITHSCORES", "LIMIT", 0, LIMIT)
local length = #expiredElements
if length < 1 then
    return
end

local maxScore = 0
local allPositions = {}
for i = 1, #expiredElements - 1, 2 do
    local msgPack = expiredElements[i]
    local score = tonumber(expiredElements[i + 1])
    if score > maxScore then maxScore = score end

    -- Unpack the message pack, retrieve values
    local entry = cmsgpack.unpack(msgPack)

    -- Add to all positions
    for _, position in ipairs(entry.positions) do
        table.insert(allPositions, position)
    end
end

-- Remove elements from the queue
redis.call("ZREMRANGEBYSCORE", QUEUE_KEY, 0, maxScore)

-- Batch 20 elements
local BATCH = 20
for j = 1, #allPositions, BATCH do
    local positionSlice = slice(allPositions, j, j + BATCH)
    local keys = encodeKeys(positionSlice)

    -- Collect all counts
    local counts = redis.call("HMGET", COUNTS_KEY, unpack(keys))

    -- Decrement all counts
    local hmset = {}
    local setbits = {}
    for i, foundValue in ipairs(counts) do
        local count = tonumber(foundValue)
        redis.log(redis.LOG_WARNING, "count(" .. positionSlice[i] .. ") -> " .. count)

        -- Insert key first
        table.insert(hmset, keys[i])

        -- Insert new count second
        counts[i] = count - 1
        table.insert(hmset, counts[i])

        -- Collect bits to clear
        setbits[positionSlice[i]] = count <= 1
    end
    redis.call("HMSET", COUNTS_KEY, unpack(hmset))

    -- Reset all positions in binary BF which have to be reset
    for position, shouldClear in pairs(setbits) do
        -- If count is not positive, clear the bit at the given position
        if shouldClear then
            redis.call("SETBIT", BITS_KEY, position, 0)
        end
    end
end
