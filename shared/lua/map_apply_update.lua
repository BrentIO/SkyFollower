-- map_apply_update.lua
--
-- Collapses map/state_store.py's FlightStateStore.apply_update into a
-- single round trip: the out-of-order check, the merge HSET, all three TTL
-- refreshes (flight:detail:{icao_hex}'s EXPIRE, flight:live:{icao_hex}'s
-- SET...EX, and flight:visible:{icao_hex}'s SET...EX), and -- for
-- `position` packets only -- the trail RPUSH, then returns the full merged
-- current-state so the caller never needs a separate HGETALL to build the
-- WebSocket event payload.
--
-- ARGV[1] : icao_hex
-- ARGV[2] : msg_type ("position" or "metadata")
-- ARGV[3] : timestamp, a Lua-parseable number (message-processor's
--           received_at, or metadata's round-tripped equivalent -- see
--           map/main.py's _extract_timestamp)
-- ARGV[4] : field names to merge, cjson-encoded array of strings
-- ARGV[5] : field values, cjson-encoded array of strings, parallel to
--           ARGV[4] by index -- each element already the exact JSON text
--           map/state_store.py's apply_update() would have json.dumps()'d
--           for that field. Never decoded then re-encoded by this script:
--           a value already sitting in the hash and a value written here
--           are spliced back together as raw text, so none of cjson's
--           empty-array/empty-object ambiguity (see route_airports.lua's
--           header comment) ever enters the round trip.
-- ARGV[6] : stale_seconds (flight:live:{icao_hex} TTL)
-- ARGV[7] : evict_seconds (flight:detail:{icao_hex} / flight:trail:{icao_hex} TTL)
-- ARGV[8] : hide_seconds (flight:visible:{icao_hex} TTL)
--
-- Returns nil if the packet is dropped as out-of-order; otherwise a JSON
-- object string matching map/state_store.py's get_flight() shape exactly
-- (internal bookkeeping field stripped).
--
-- Equal timestamps are accepted, not dropped, and compared with a small
-- epsilon rather than a bare `<` -- see FlightStateStore.apply_update's
-- docstring for why: a `position` packet and a same-tick `metadata`
-- packet for one source ADS-B message legitimately share a timestamp, and
-- metadata's timestamp is a round-tripped ISO-8601 string that can come
-- back a hair below the original float.
--
-- Called by map/state_store.py via EVALSHA.

local icao_hex = ARGV[1]
local msg_type = ARGV[2]
local timestamp = tonumber(ARGV[3])
local field_names = cjson.decode(ARGV[4])
local field_values = cjson.decode(ARGV[5])
local stale_seconds = tonumber(ARGV[6])
local evict_seconds = tonumber(ARGV[7])
local hide_seconds = tonumber(ARGV[8])

local TIMESTAMP_EPSILON_SECONDS = 0.001
local LAST_APPLIED_TIMESTAMP_FIELD = '_last_applied_timestamp'

local detail_key = 'flight:detail:' .. icao_hex
local live_key = 'flight:live:' .. icao_hex
local visible_key = 'flight:visible:' .. icao_hex
local trail_key = 'flight:trail:' .. icao_hex

local last_raw = redis.call('HGET', detail_key, LAST_APPLIED_TIMESTAMP_FIELD)
if last_raw then
    local last_timestamp = tonumber(last_raw)
    if last_timestamp and timestamp < last_timestamp - TIMESTAMP_EPSILON_SECONDS then
        return nil
    end
end

local hset_args = {}
for i, name in ipairs(field_names) do
    table.insert(hset_args, name)
    table.insert(hset_args, field_values[i])
end
table.insert(hset_args, 'icao_hex')
table.insert(hset_args, cjson.encode(icao_hex))
table.insert(hset_args, LAST_APPLIED_TIMESTAMP_FIELD)
table.insert(hset_args, cjson.encode(timestamp))

redis.call('HSET', detail_key, unpack(hset_args))
redis.call('EXPIRE', detail_key, evict_seconds)
redis.call('SET', live_key, '1', 'EX', stale_seconds)
redis.call('SET', visible_key, '1', 'EX', hide_seconds)

-- Re-reading the hash after the write (rather than folding the
-- just-applied fields into a Lua-side copy of the previous state) is what
-- makes the returned payload trivially correct: it reflects Redis's own
-- merged truth, not a hand-maintained shadow of it.
local raw = redis.call('HGETALL', detail_key)
local parts = {}
local latitude_value = nil
local longitude_value = nil
local altitude_value = nil
for i = 1, #raw, 2 do
    local field = raw[i]
    local value = raw[i + 1]
    if field == 'lat' then
        latitude_value = value
    elseif field == 'lon' then
        longitude_value = value
    elseif field == 'alt' then
        altitude_value = value
    end
    if field ~= LAST_APPLIED_TIMESTAMP_FIELD then
        table.insert(parts, cjson.encode(field) .. ':' .. value)
    end
end
local merged_json = '{' .. table.concat(parts, ',') .. '}'

-- Trail accumulation: every accepted `position` packet appends the
-- *merged* (not just this packet's) lat/lon/altitude snapshot, once a
-- latitude/longitude are actually known -- an aircraft whose only traffic
-- so far is velocity/heading-only position packets has nothing meaningful
-- to plot yet. altitude defaults to JSON null (not omitted) so every trail
-- point has the same shape regardless of whether altitude is known yet.
if msg_type == 'position' and latitude_value and longitude_value then
    local point = '{"lat":' .. latitude_value ..
        ',"lon":' .. longitude_value ..
        ',"alt":' .. (altitude_value or 'null') .. '}'
    redis.call('RPUSH', trail_key, point)
    redis.call('EXPIRE', trail_key, evict_seconds)
end

return merged_json
