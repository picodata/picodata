local fun = require('fun')
local json = require('json')

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
        :map(function(tuple) return tuple:tomap({ names_only = true }) end)
        :totable()
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
    local replicaset = fun.iter(replicasets)
        :filter(function(replicaset) return replicaset.replicaset_uuid == instance.replicaset_uuid end)
        :totable()[1]

    return instance.instance_id == replicaset.master_id
end

local function cast_instance(instance, is_leader)
    return {
        name = instance.instance_id,
        targetGrade = instance.target_grade[1],
        currentGrade = instance.current_grade[1],
        failureDomain = instance.failure_domain,
        version = VERSION_PLACEHOLDER,
        isLeader = is_leader,
    }
end

-- the expected response:
-- --------------------------------------------------
-- export interface ReplicasetType {
--     id: string;
--     instanceCount: number;
--     instances: InstanceType[];
--     version: string;
--     grade: string;
--     capacity: string;
-- }

-- export interface InstanceType {
--     name: string;
--     targetGrade: string;
--     currentGrade: string;
--     failureDomain: string;
--     version: string;
--     isLeader: boolean;
--   }
-- --------------------------------------------------
local function list_replicasets()
    local replicasets = get_replicasets()

    local instances = get_instances()

    local memory_info = get_memory_info()
    local replicaset_table = fun.iter(instances)
        :reduce(function(acc, instance)
            local is_leader = is_leader(instance, replicasets)
            local instance_memory_info = memory_info[instance.replicaset_uuid]
            if acc[instance.replicaset_uuid] == nil then
                acc[instance.replicaset_uuid] = {
                    id = instance.replicaset_id,
                    uuid = instance.replicaset_uuid,
                    instanceCount = 1,
                    instances = { cast_instance(instance, is_leader) },
                    version = VERSION_PLACEHOLDER,
                    grade = is_leader and instance.current_grade[1],
                    -- frontend expects number
                    capacity =
                        is_leader
                        and instance_memory_info.quota_used / instance_memory_info.quota_size * 100,
                }
            else
                local replicaset = acc[instance.replicaset_uuid]
                table.insert(replicaset.instances, cast_instance(instance))
                replicaset.instanceCount = replicaset.instanceCount + 1
                if is_leader then
                    replicaset.grade = instance.current_grade[1]
                    replicaset.capacity = instance_memory_info.quota_used / instance_memory_info.quota_size * 100
                end
            end
            return acc
        end, {})

    local replicasets = fun.iter(replicaset_table)
        :map(function(_, replicaset) return replicaset end)
        :totable()

    return {
        status = 200,
        body = json.encode(replicasets),
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
    local replicasets = get_replicasets()
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
httpd:route({ method = 'GET', path = 'api/v1/replicaset' }, list_replicasets)
httpd:route({ method = 'GET', path = 'api/v1/cluster' }, cluster_info)
httpd:start();
_G.pico.httpd = httpd