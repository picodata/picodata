local fun = require('fun')
local json = require('json')
local net = require('net.box')

-- I believe that we won't maintain clusters over 1k replicasets
-- in the nearest future
local SELECT_LIMIT = 1000

local VERSION_PLACEHOLDER = '??.??'
local DEFAULT_FUTURE_TIMEOUT = 60

local INSTANCE_GRADE = {
    ONLINE = 'Online',
    OffLINE = 'Offline',
}

local function table_len(table)
    local i = 0
    for _ in pairs(table) do
        i = i + 1
    end

    return i
end

local function get_instances()
    local tuples = box.space._pico_instance:select({}, { limit = SELECT_LIMIT })
    return fun.iter(tuples)
        :map(function(tuple) return tuple:tomap({ names_only = true }) end)
        :totable()
end

local function get_replicasets()
    local tuples = box.space._pico_replicaset:select({}, { limit = SELECT_LIMIT })
    return fun.iter(tuples)
        :map(function(tuple)
            local replicaset = tuple:tomap({ names_only = true })
            return replicaset.replicaset_uuid, replicaset
        end)
        :tomap()
end

local function get_peer_addresses()
    local tuples = box.space._pico_peer_address:select({}, { limit = SELECT_LIMIT })
    return fun.iter(tuples)
        :map(function(tuple)
            local peer_address = tuple:tomap({ names_only = true })
            return peer_address.raft_id, peer_address
        end)
        :tomap()
end

--[[ the expected response:
export type TierType = {
    id: string;
    plugins: string[];
    replicasetCount: number;
    instanceCount: number;
    rf: number;
    canVoter: boolean;
    replicasets: ReplicasetType[];
};
]]
local function get_tiers()
    local tuples = box.space._pico_tier:select({}, { limit = SELECT_LIMIT })
    return fun.iter(tuples)
        :map(function(tuple)
            local tier = tuple:tomap({ names_only = true })
            tier.can_vote = tier.can_vote or true
            tier.replicasets = {}
            tier.plugins = {}
            tier.replicasetCount = 0
            tier.instanceCount = 0
            tier.rf = tier.replication_factor
            tier.replication_factor = nil
            return tier.name, tier
        end)
        :tomap()
end

-- returns leader's box.slab.info
local function get_memory_info()
    local replicasets = vshard.router.routeall()

    -- we only need to watch for leader's capacity
    local leaders = fun.iter(replicasets)
        :reduce(function(acc, uuid, replicaset)
            acc[uuid] = replicaset.master
            return acc
        end,{})

    local slab_info_futures = fun.iter(leaders)
        :reduce(function(acc, uuid, instance)
            local future = instance.conn:call('box.slab.info', {}, { is_async = true })
            acc[uuid] = future
            return acc
        end, {})

    local results = fun.iter(slab_info_futures)
        :reduce(function(acc, uuid, future)
            acc[uuid] = future:wait_result(DEFAULT_FUTURE_TIMEOUT)[1]
            return acc
        end, {})

    return results
end

local function is_leader(instance, replicasets)
    return instance.instance_id == replicasets[instance.replicaset_uuid].current_master_id
end

local function get_address(instance, peer_addresses)
    return peer_addresses[instance.raft_id].address
end

--[[
    export interface InstanceType {
    name: string;
    targetGrade: string;
    currentGrade: string;
    failureDomain: Record<string, string>;
    version: string;
    isLeader: boolean;
    binaryAddress: string;
    httpAddress: string;
}
]]
local function create_instance(instance, cluster_state)
    local instance_dto = {
        name = instance.instance_id,
        targetGrade = instance.target_grade[1],
        currentGrade = instance.current_grade[1],
        failureDomain = instance.failure_domain,
        version = VERSION_PLACEHOLDER,
        isLeader = is_leader(instance, cluster_state.replicasets),
        binaryAddress = get_address(instance, cluster_state.peer_addresses),
    }

    local c = net.connect(instance_dto.binaryAddress, { fetch_schema = false })
    local result = c:eval([[
        return {
            httpd = pico and pico.httpd and {
                host = pico.httpd.host,
                port = pico.httpd.port
            },
            version = pico.PICODATA_VERSION
        }
    ]])

    if result then
        local httpAddress = result.httpd
        if httpAddress then
            instance_dto.httpAddress = string.format('%s:%d', httpAddress.host, httpAddress.port)
        end
        instance_dto.version = result.version
    end

    return instance_dto
end

--[[
export interface ReplicasetType {
    id: string;
    instanceCount: number;
    instances: InstanceType[];
    version: string;
    grade: string;
    capacity: number;
}
]]
local function update_replicaset_memory_data(replicaset, instance, instance_memory_info)
    replicaset.grade = instance.current_grade[1]
    -- frontend expects number
    replicaset.capacityUsage = instance_memory_info.quota_used / instance_memory_info.quota_size * 100
    replicaset.memory = {
        used = instance_memory_info.quota_used,
        usable = instance_memory_info.quota_size,
    }
end

local function create_replicaset(instance, cluster_state, instance_memory_info)
    local instance_dto = create_instance(instance, cluster_state)
    local replicaset = {
        id = instance.replicaset_id,
        uuid = instance.replicaset_uuid,
        instanceCount = 1,
        instances = { instance_dto },
        version = instance_dto.version,
        tier = instance.tier
    }

    if is_leader(instance, cluster_state.replicasets) then
        update_replicaset_memory_data(replicaset, instance, instance_memory_info)
    end

    return replicaset
end

local function list_tiers()
    local cluster_state = {
        replicasets = get_replicasets(),
        instances = get_instances(),
        memory_info = get_memory_info(),
        tiers = get_tiers(),
        peer_addresses = get_peer_addresses()
    }


    local replicasets = (fun.iter(cluster_state.instances)
        :reduce(
            function(acc, instance)
                local instance_memory_info = cluster_state.memory_info[instance.replicaset_uuid]
                if acc[instance.replicaset_uuid] == nil then
                    acc[instance.replicaset_uuid] = create_replicaset(
                        instance,
                        cluster_state,
                        instance_memory_info)
                else
                    local replicaset = acc[instance.replicaset_uuid]
                    table.insert(replicaset.instances, create_instance(instance, cluster_state))
                    replicaset.instanceCount = replicaset.instanceCount + 1
                    if is_leader(instance, cluster_state.replicasets) then
                        update_replicaset_memory_data(replicaset, instance, instance_memory_info)
                    end
                end
                return acc
            end,
            {}))

    local tiers = fun.iter(replicasets)
        :reduce(
            function(acc, _, replicaset)
                local tier = acc[replicaset.tier]
                tier.instanceCount = tier.instanceCount + replicaset.instanceCount
                tier.replicasetCount = tier.replicasetCount + 1
                table.insert(tier.replicasets, replicaset)
                replicaset.tier = nil
                return acc
            end,
            cluster_state.tiers
        )

    return {
        status = 200,
        body = json.encode(fun.iter(tiers)
            :map(function (_, tier) return tier end)
            :totable()),
        headers = {
            ['content-type'] = 'application/json' }
        }
end

-- the expected response:
-- --------------------------------------------------
-- export interface ClusterInfoType {
--     capacityUsage: string;
--     memory: {
--      used: string;
--      usable: string;
--     };
--     replicasetsCount: number;
--     instancesCurrentGradeOnline: number;
--     instancesCurrentGradeOffline: number;
--     currentInstaceVersion: string;
-- }
-- --------------------------------------------------
local function cluster_info()
    local replicasets = fun.iter(get_replicasets()):map(function(_, x) return x end):totable()
    local replicasetsCount = table_len(replicasets)

    local instances = get_instances()

    local instancesOnlineIter, instancesOfflineIter = fun.iter(instances)
        :partition(function(instance) return instance.current_grade[1] == INSTANCE_GRADE.ONLINE end)

    local memory_info = get_memory_info()
    -- '#' operator does not work for map-like tables
    local memory_info_count = fun.iter(memory_info):length()
    local avg_capacity = fun.iter(memory_info)
        :map(function(_, slab_info)
            -- i believe that calculating quota_used_ratio one more time is
            -- easier than parsing it from '54.23%' string
            return slab_info.quota_used / slab_info.quota_size
        end)
        :sum() / memory_info_count * 100

    local total_used = fun.iter(memory_info)
        :map(function(_, slab_info) return slab_info.quota_used end)
        :sum()
    local total_size = fun.iter(memory_info)
        :map(function(_, slab_info) return slab_info.quota_size end)
        :sum()

    local resp = {
        capacityUsage = avg_capacity,
        memory = {
            used = total_used,
            usable = total_size,
        },
        replicasetsCount = replicasetsCount,
        instancesCurrentGradeOnline = instancesOnlineIter:length(),
        instancesCurrentGradeOffline = instancesOfflineIter:length(),
        currentInstaceVersion = pico.PICODATA_VERSION,
    }

    return {
        status = 200,
        body = json.encode(resp),
        headers = {
            ['content-type'] = 'application/json' }
        }
end

local host, port = ...;
local httpd = require('http.server').new(host, port);
httpd:route({ method = 'GET', path = 'api/v1/tiers' }, list_tiers)
httpd:route({ method = 'GET', path = 'api/v1/cluster' }, cluster_info)
httpd:start();
_G.pico.httpd = httpd
