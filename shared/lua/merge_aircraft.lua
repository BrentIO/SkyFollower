-- merge_aircraft.lua
--
-- Merges aircraft:mictronics:{icao_hex} and aircraft:registry:{icao_hex} into a
-- single JSON document and returns it as a string.
--
-- ARGV[1] : icao_hex (e.g. "A8AE7F")
--
-- Returns nil only if both keys are absent for the given icao_hex.
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

local mictronics_raw = redis.call('JSON.GET', 'aircraft:mictronics:' .. icao_hex)
local registry_raw   = redis.call('JSON.GET', 'aircraft:registry:'   .. icao_hex)

if not mictronics_raw and not registry_raw then
    return nil
end

local result = mictronics_raw and cjson.decode(mictronics_raw) or {}
if registry_raw then
    deep_merge(result, cjson.decode(registry_raw))
end

return cjson.encode(result)
