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
-- special_livery — but the ordering is preserved so the three-source
-- stacking (mictronics -> registry -> livery) holds even if that changes.
--
-- If the merged result has no aircraft.manufacturer_model (only ever written by
-- mictronics), it is synthesized from aircraft.manufacturer / aircraft.model —
-- whichever civil registry runner supplied them — so TTS still has something to
-- announce for hexes Mictronics hasn't picked up. This is computed on the
-- merged result only, never written back to either Redis key, so it self-heals
-- the moment Mictronics later picks up the hex and can never shadow or degrade
-- a real Mictronics value.
--
-- Each of the three keys carries its own scalar `source` field naming the
-- runner that wrote it (e.g. "mictronics", "us-faa"). A plain deep_merge
-- would let later keys silently clobber earlier ones, hiding the fact that
-- an aircraft's enrichment came from more than one runner. Instead the
-- result never carries a bare `source` — every present key's `source` is
-- collected into `data_sources`, an array in mictronics -> registry ->
-- livery order, distinct from #492's unrelated per-flight
-- `receiver_sources` field (1090/978/MLAT).
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

local function collect_data_sources(mictronics_doc, registry_doc, livery_doc)
    -- Three explicit checks rather than ipairs() over a {a, b, c} table:
    -- ipairs stops at the first nil element, so if mictronics_doc were nil
    -- (Mictronics hasn't picked up this hex yet) it would silently skip
    -- registry_doc/livery_doc too, even though they're present.
    --
    -- Must run BEFORE deep_merge: `result` below starts out as the *same
    -- table* as mictronics_doc (not a copy), so deep_merge(result,
    -- registry_doc) mutates mictronics_doc.source in place. Reading .source
    -- off all three docs here, before any merging happens, is what keeps
    -- them from aliasing into the same value.
    local data_sources = {}
    if mictronics_doc and not is_absent(mictronics_doc.source) then
        table.insert(data_sources, mictronics_doc.source)
    end
    if registry_doc and not is_absent(registry_doc.source) then
        table.insert(data_sources, registry_doc.source)
    end
    if livery_doc and not is_absent(livery_doc.source) then
        table.insert(data_sources, livery_doc.source)
    end
    return data_sources
end

local mictronics_raw = redis.call('JSON.GET', 'aircraft:mictronics:' .. icao_hex)
local registry_raw   = redis.call('JSON.GET', 'aircraft:registry:'   .. icao_hex)
local livery_raw     = redis.call('JSON.GET', 'aircraft:livery:'     .. icao_hex)

if not mictronics_raw and not registry_raw and not livery_raw then
    return nil
end

local mictronics_doc = mictronics_raw and cjson.decode(mictronics_raw) or nil
local registry_doc   = registry_raw   and cjson.decode(registry_raw)   or nil
local livery_doc     = livery_raw     and cjson.decode(livery_raw)     or nil

local data_sources = collect_data_sources(mictronics_doc, registry_doc, livery_doc)

local result = mictronics_doc or {}
if registry_doc then
    deep_merge(result, registry_doc)
end
if livery_doc then
    deep_merge(result, livery_doc)
end

result.source = nil
if #data_sources > 0 then
    result.data_sources = data_sources
end

apply_manufacturer_model_fallback(result)

return cjson.encode(result)
