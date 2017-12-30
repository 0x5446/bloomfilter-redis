local conf = {
	gap = 300, -- 300s interval between filter
	win = 1800, -- 1 hour sliding window size
	m = 2000000000, -- 2 billion
	k = 16
}

-- functions BOF
local function getBKDRHashSeed(n)
    local r = 0 
    if 0 == n then
        return 31
    end 
    local j = n + 1 
    for i = 0, j, 1 do
        if i % 2 ~= 0 then
            r = r * 10 + 3 
        else
            r = r * 10 + 1 
        end 
    end 
    return r
end

local function BKDRHash(str, seed)
	local hash = 0
	local len = string.len(str)
	for i = 1, len, 1 do
		hash = hash * seed + string.byte(str, i)
	end
	return bit.band(hash, 0x7FFFFFFF)
end

local function filt(key, e)
	local rt = 0
	for i = 1, conf.k, 1 do
		local seed = getBKDRHashSeed(i-1)
		local hash = BKDRHash(e, seed)
		local offset = hash % conf.m
		rt = rt + redis.pcall('SETBIT', key, offset, 1)
	end
	return rt == conf.k
end

local function remove(k)
	redis.pcall('DEL', k)
end
-- functions EOF

local e = tostring(ARGV[1])
local ts = tonumber(ARGV[2])
local diff = ts - conf.win
local oldest = diff - diff % conf.gap
local obsolete = oldest - conf.gap

local rt = filt(oldest, e)
local key = oldest + conf.gap

while key <= ts
do
	filt(key, e)
	key = key + conf.gap
end

remove(obsolete)

return rt
--# vim: set ts=4 sw=4 cindent nu :
