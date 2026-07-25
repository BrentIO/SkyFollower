-- route_airports.lua
--
-- Resolves a flight ident to its route's full airport records, one round
-- trip instead of a GET route:{ident} followed by N separate
-- JSON.GET airport:{code} calls from the caller.
--
-- ARGV[1] : ident (e.g. "AAL3273")
--
-- route:{ident} is a plain Redis string (not JSON) written by the
-- standing-data runner, e.g. "KMIA-KJFK-KMIA" — an ordered, dash-delimited
-- list of ICAO airport codes with duplicates preserved for round trips.
--
-- Returns cjson.encode of an array of decoded airport:{code} records, in
-- route order, duplicates preserved (e.g. KJFK-KMIA-KJFK yields the same
-- KJFK record at both elements 0 and 2).
--
-- Returns cjson.encode of an empty array, rather than nil, in two cases:
--   * route:{ident} is absent (no route known for this ident)
--   * any code in the route has no matching airport:{code} record — a
--     partial route can't reliably tell the caller the origin or
--     destination, so it is treated as no result rather than a
--     result with gaps
-- Callers always get a JSON array back, never nil, so they don't need a
-- separate nil-check branch alongside the empty-array check.
--
-- Called by whichever component needs a resolved route (e.g. UI backend
-- lookup, notification enrichment) via EVALSHA.

local ident = ARGV[1]

local EMPTY_ARRAY = '[]'

local function split(s, delimiter)
    local parts = {}
    for part in (s .. delimiter):gmatch('(.-)' .. delimiter) do
        table.insert(parts, part)
    end
    return parts
end

local route_raw = redis.call('GET', 'route:' .. string.upper(ident))

if not route_raw then
    return EMPTY_ARRAY
end

local codes = split(route_raw, '-')

local airports = {}
for _, code in ipairs(codes) do
    local airport_raw = redis.call('JSON.GET', 'airport:' .. string.upper(code))
    if not airport_raw then
        -- One missing leg invalidates the whole route — see header comment.
        return EMPTY_ARRAY
    end
    table.insert(airports, cjson.decode(airport_raw))
end

-- cjson.encode({}) serializes an empty Lua table as the JSON object "{}",
-- not "[]" — it can't tell an empty array from an empty object — so the
-- empty case is always returned as the EMPTY_ARRAY literal above rather
-- than through cjson.encode. Non-empty tables with contiguous integer keys
-- (built via table.insert here) do encode correctly as JSON arrays.
return cjson.encode(airports)
