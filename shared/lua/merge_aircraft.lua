-- merge_aircraft.lua
--
-- Merges aircraft:mictronics:{icao_hex} and aircraft:registry:{icao_hex} into a
-- single JSON document and returns it as a string.
--
-- ARGV[1] : icao_hex (e.g. "A8AE7F")
--
-- Returns nil if no mictronics record exists for the given icao_hex.
-- Registry fields win over mictronics fields on any overlap (same semantics as the
-- old deep-merge-on-write pattern, but performed server-side at read time).
--
-- Called by the processor via EVALSHA so the merge is a single round-trip.

local icao_hex = ARGV[1]

local function deep_merge(base, update)
    for k, v in pairs(update) do
        if type(v) == 'table' and type(base[k]) == 'table' then
            deep_merge(base[k], v)
        else
            base[k] = v
        end
    end
end

local simple_raw = redis.call('JSON.GET', 'aircraft:mictronics:' .. icao_hex)
if not simple_raw then
    return nil
end

local result = cjson.decode(simple_raw)

local detail_raw = redis.call('JSON.GET', 'aircraft:registry:' .. icao_hex)
if detail_raw then
    deep_merge(result, cjson.decode(detail_raw))
end

return cjson.encode(result)
