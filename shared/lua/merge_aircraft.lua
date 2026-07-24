-- merge_aircraft.lua
--
-- Merges aircraft:mictronics:{icao_hex}, aircraft:registry:{icao_hex}, and
-- aircraft:livery:{icao_hex} into a single JSON document and returns it as a
-- string.
--
-- ARGV[1] : icao_hex (e.g. "A8AE7F")
--
-- Returns nil only if all three keys are absent for the given icao_hex.
-- Fields win in this priority order, lowest to highest: mictronics,
-- registry, livery (same semantics as the old deep-merge-on-write pattern,
-- but performed server-side at read time). In practice registry and livery
-- never write overlapping fields — livery only ever contributes
-- special_livery/livery_name — but the ordering is preserved so the
-- three-source stacking (mictronics -> registry -> livery) holds even if
-- that changes.
--
-- If the merged result has no aircraft.manufacturer_model (only ever written by
-- mictronics), it is synthesized from aircraft.manufacturer / aircraft.model —
-- whichever civil registry runner supplied them — so TTS still has something to
-- announce for hexes Mictronics hasn't picked up. This is computed on the
-- merged result only, never written back to either Redis key, so it self-heals
-- the moment Mictronics later picks up the hex and can never shadow or degrade
-- a real Mictronics value.
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

local function is_absent(v)
    return v == nil or v == cjson.null
end

local function trim(s)
    return (s:gsub('^%s+', ''):gsub('%s+$', ''))
end

local function apply_manufacturer_model_fallback(result)
    if result.aircraft and is_absent(result.aircraft.manufacturer_model) then
        local parts = {}
        for _, field in ipairs({'manufacturer', 'model'}) do
            local v = result.aircraft[field]
            if not is_absent(v) then
                local trimmed = trim(v)
                if trimmed ~= '' then
                    table.insert(parts, trimmed)
                end
            end
        end
        if #parts > 0 then
            result.aircraft.manufacturer_model = table.concat(parts, ' ')
        end
    end
end

local mictronics_raw = redis.call('JSON.GET', 'aircraft:mictronics:' .. icao_hex)
local registry_raw   = redis.call('JSON.GET', 'aircraft:registry:'   .. icao_hex)
local livery_raw     = redis.call('JSON.GET', 'aircraft:livery:'     .. icao_hex)

if not mictronics_raw and not registry_raw and not livery_raw then
    return nil
end

local result = mictronics_raw and cjson.decode(mictronics_raw) or {}
if registry_raw then
    deep_merge(result, cjson.decode(registry_raw))
end
if livery_raw then
    deep_merge(result, cjson.decode(livery_raw))
end

apply_manufacturer_model_fallback(result)

return cjson.encode(result)
