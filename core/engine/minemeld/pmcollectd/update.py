UPDATE = b"""
-- this should be used to implement a Poor Man's collectd using Redis
-- and remove some deps (and deps of deps)

local steps = 1440 -- number of CDPs per row
local packed_cdp_size = struct.size("Ll")
local max_cdps_size = (steps - 1) * packed_cdp_size

-- keys
local key_current_timestamp_hi = "ctimestamp_hi"
local key_current_bin = "cbin"
local key_current_value = "cvalue"
local key_cdps = "cdps"

local function initalize_cdp (base_cdp_key, current_timestamp_hi, current_bin, current_value)
    -- initialize
    redis.call("hmset",
        base_cdp_key,
        key_current_timestamp_hi, current_timestamp_hi,
        key_current_bin, current_bin,
        key_current_value, current_value,
        key_cdps, ""
    )
end

local function update_cdp (base_cdp_key, interval, timestamp_hi, timestamp_lo, value)
    local current_params = redis.call('hmget',
        base_cdp_key,
        key_current_timestamp_hi,
        key_current_bin,
        key_current_value
    )
    if current_params[1] ~= tostring(timestamp_hi) then
        -- welcome 2038 - we jumped into a new 32-bit time segment, reset!
        current_params[1] = nil
        current_params[2] = nil
        current_params[3] = nil
    end

    local new_bin = math.floor(timestamp_lo/interval)*interval
    if current_params[2] == nil or current_params[3] == nil then
        initalize_cdp(base_cdp_key, timestamp_hi, new_bin, value)
        return true
    end

    local current_bin = tonumber(current_params[2])

    if current_bin == nil then
        initalize_cdp(base_cdp_key, timestamp_hi, new_bin, value)
        return true
    end

    if new_bin < current_bin then
        -- update from the past, ignore
        return false
    end

    if new_bin > current_bin then
        -- we are in a new bin, save the old one and set the new one
        local encoded_current = struct.pack('Ll', current_bin, tonumber(current_params[3]))
        local current_cdps = redis.call('hget',
            base_cdp_key,
            key_cdps
        )
        local new_cdps = current_cdps..encoded_current
        if string.len(new_cdps) > max_cdps_size then
            new_cdps = string.sub(new_cdps, packed_cdp_size + 1, -1)
        end
        redis.call('hmset',
            base_cdp_key,
            key_current_bin, new_bin,
            key_current_value, value,
            key_cdps, new_cdps
        )
        return true
    end

    if tonumber(value) > tonumber(current_params[3]) then
        -- new max in the current bin
        redis.call('hset',
            base_cdp_key,
            key_current_value, value
        )
        return true
    end

    return true
end

-- main function, timestamp is considered the current timestamp
local function update (key_name, timestamp_hi, timestamp_lo, value)
    local base_keyname = "pmcollectd:"..ARGV[1]

    if not update_cdp(base_keyname.."\\00cdp:day",60,timestamp_hi,timestamp_lo,value) then -- day
        return redis.status_reply("Ignored")
    end

    update_cdp(base_keyname.."\\00cdp:week",420,timestamp_hi,timestamp_lo,value) -- week
    update_cdp(base_keyname.."\\00cdp:month",1860,timestamp_hi,timestamp_lo,value) -- month

    return redis.status_reply("OK")
end

return update(ARGV[1],ARGV[2],ARGV[3],ARGV[4])
"""
