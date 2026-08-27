-- incr_period_counter.lua
--
-- Atomic increment-with-expiry-on-creation-only for a period counter (e.g.
-- an hour or today bucket that must reset itself at a real UTC boundary).
-- Generic on purpose -- shared by the receiver's per-connection message
-- counts and the message processor's registration/aircraft-type miss
-- counts, not owned by either.
--
-- ARGV[1] : key
-- ARGV[2] : increment amount (integer)
-- ARGV[3] : absolute Unix expiry (EXPIREAT), applied only if this call is
--           the one that creates the key -- i.e., the key didn't already
--           exist at the moment INCRBY ran. An already-existing key keeps
--           whatever expiry it was given when it was first created, so a
--           mid-period flush never pushes the real boundary out further.
--
-- Returns the key's new value.
--
-- Called by EVALSHA so the exists-check + increment + conditional expire
-- is a single round-trip and can't race with a concurrent caller.

local key = ARGV[1]
local amount = tonumber(ARGV[2])
local expires_at = tonumber(ARGV[3])

local existed = redis.call('EXISTS', key) == 1
local new_value = redis.call('INCRBY', key, amount)

if not existed then
    redis.call('EXPIREAT', key, expires_at)
end

return new_value
