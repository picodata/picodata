#!/usr/bin/env tarantool

local nb = require("net.box")
local fiber = require("fiber")
local yaml = require('yaml')
local clock = require("clock")

local params = { ... }

if #params ~= 1 then
    print("The amount of records to generate is required")
    os.exit(1)
end

local function fail_if_error(err, msg)
    if err ~= nil then
        error(msg .. ": " .. err)
        os.exit(1)
    end
end

local start_time = clock.time();

local record_count = params[1]

local api = nb.connect("admin:app-cluster-cookie@localhost:3301")

local _, err = api:eval("function set_schema(s) local cartridge = require('cartridge'); cartridge.set_schema(s) end")
fail_if_error(err, "Failed to create set_schema function")

local config = {
    spaces = {
        t_memtx = {
            format = {
                {
                    name = "id",
                    type = "integer",
                    is_nullable = false,
                },
                {
                    name = "a",
                    type = "number",
                    is_nullable = false,
                },
                {
                    name = "bucket_id",
                    type = "unsigned",
                    is_nullable = true,
                },
            },
            temporary = false,
            engine = "memtx",
            indexes = {
                {
                    unique = true,
                    parts = {
                        {
                            path = "id",
                            type = "integer",
                            is_nullable = false,
                        },
                    },
                    type = "TREE",
                    name = "id",
                },
                {
                    unique = false,
                    parts = {
                        {
                            path = "bucket_id",
                            type = "unsigned",
                            is_nullable = true,
                        },
                    },
                    type = "TREE",
                    name = "bucket_id",
                },
            },
            is_local = false,
            sharding_key = { "id" },
        },
        t = {
            format = {
                {
                    name = "id",
                    type = "integer",
                    is_nullable = false,
                },
                {
                    name = "a",
                    type = "number",
                    is_nullable = false,
                },
                {
                    name = "bucket_id",
                    type = "unsigned",
                    is_nullable = true,
                },
            },
            temporary = false,
            engine = "vinyl",
            indexes = {
                {
                    unique = true,
                    parts = {
                        {
                            path = "id",
                            type = "integer",
                            is_nullable = false,
                        },
                    },
                    type = "TREE",
                    name = "id",
                },
                {
                    unique = false,
                    parts = {
                        {
                            path = "bucket_id",
                            type = "unsigned",
                            is_nullable = true,
                        },
                    },
                    type = "TREE",
                    name = "bucket_id",
                },
            },
            is_local = false,
            sharding_key = { "id" },
        },
        vehicle_actual = {
            format = {
                {
                    name = "id",
                    type = "integer",
                    is_nullable = false,
                },
                {
                    name = "vehiclevin",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclevin2",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclechassisnum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclereleaseyear",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationregdoctypename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationregdoc",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationregdocissuedate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationregdoccomments",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleptstypename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleptsnum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleptsissuedate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleptsissuer",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleptscomments",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclebodycolor",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclebrand",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclemodel",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclebrandmodel",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclebodynum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclecost",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclegasequip",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleproducername",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclegrossmass",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclemass",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclesteeringwheeltypeid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclekpptype",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicletransmissiontype",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicletypename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclecategory",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicletypeunit",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleecoclass",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclespecfuncname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenclosedvolume",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenginemodel",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenginenum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenginepower",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenginepowerkw",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenginetype",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holdrestrictiondate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "approvalnum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "approvaldate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "approvaltype",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "utilizationfeename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customsdoc",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customsdocdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customsdocissue",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customsdocrestriction",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customscountryremovalid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customscountryremovalname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerorgname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerinn",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerogrn",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerkpp",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersonlastname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersonfirstname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersonmiddlename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersonbirthdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerbirthplace",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersonogrnip",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "owneraddressindex",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "owneraddressmundistrict",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "owneraddresssettlement",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "owneraddressstreet",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersoninn",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersondoccode",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersondocnum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersondocdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationdepartmentname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationattorney",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationlising",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holdertypeid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersondoccode",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersondocnum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersondocdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersondocissuer",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonlastname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonfirstname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonmiddlename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonbirthdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonbirthregionid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonsex",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonbirthplace",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersoninn",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonsnils",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonogrnip",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressguid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressregionid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressregionname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressdistrict",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressmundistrict",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddresssettlement",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressstreet",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressbuilding",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressstructureid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressstructurename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressstructure",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleguid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "reestrid",
                    type = "unsigned",
                    is_nullable = false,
                },
                {
                    name = "reestrstatus",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleregno",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "sys_op",
                    type = "number",
                    is_nullable = false,
                },
                {
                    name = "sys_to",
                    type = "number",
                    is_nullable = false,
                },
                {
                    name = "sys_from",
                    type = "number",
                    is_nullable = false,
                },
                {
                    name = "bucket_id",
                    type = "unsigned",
                    is_nullable = true,
                },
            },
            temporary = false,
            engine = "vinyl",
            indexes = {
                {
                    unique = true,
                    parts = {
                        {
                            path = "id",
                            type = "integer",
                            is_nullable = false,
                        },
                        {
                            path = "sys_op",
                            type = "number",
                            is_nullable = false,
                        }
                    },
                    type = "TREE",
                    name = "id",
                },
                {
                    unique = false,
                    parts = {
                        {
                            path = "bucket_id",
                            type = "unsigned",
                            is_nullable = true,
                        },
                    },
                    type = "TREE",
                    name = "bucket_id",
                },
                {
                    unique = false,
                    parts = {
                        {
                            path = "reestrid",
                            type = "unsigned",
                            is_nullable = false,
                        },
                    },
                    type = "TREE",
                    name = "reestrid",
                },
            },
            is_local = false,
            sharding_key = { "id" },
        },
        vehicle_history = {
            format = {
                {
                    name = "id",
                    type = "integer",
                    is_nullable = false,
                },
                {
                    name = "vehiclevin",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclevin2",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclechassisnum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclereleaseyear",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationregdoctypename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationregdoc",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationregdocissuedate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationregdoccomments",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleptstypename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleptsnum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleptsissuedate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleptsissuer",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleptscomments",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclebodycolor",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclebrand",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclemodel",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclebrandmodel",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclebodynum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclecost",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclegasequip",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleproducername",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclegrossmass",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclemass",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclesteeringwheeltypeid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclekpptype",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicletransmissiontype",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicletypename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclecategory",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicletypeunit",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleecoclass",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehiclespecfuncname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenclosedvolume",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenginemodel",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenginenum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenginepower",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenginepowerkw",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleenginetype",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holdrestrictiondate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "approvalnum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "approvaldate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "approvaltype",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "utilizationfeename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customsdoc",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customsdocdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customsdocissue",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customsdocrestriction",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customscountryremovalid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "customscountryremovalname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerorgname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerinn",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerogrn",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerkpp",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersonlastname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersonfirstname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersonmiddlename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersonbirthdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerbirthplace",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersonogrnip",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "owneraddressindex",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "owneraddressmundistrict",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "owneraddresssettlement",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "owneraddressstreet",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersoninn",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersondoccode",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersondocnum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "ownerpersondocdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationdepartmentname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationattorney",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "operationlising",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holdertypeid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersondoccode",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersondocnum",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersondocdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersondocissuer",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonlastname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonfirstname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonmiddlename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonbirthdate",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonbirthregionid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonsex",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonbirthplace",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersoninn",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonsnils",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderpersonogrnip",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressguid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressregionid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressregionname",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressdistrict",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressmundistrict",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddresssettlement",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressstreet",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressbuilding",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressstructureid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressstructurename",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "holderaddressstructure",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleguid",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "reestrid",
                    type = "unsigned",
                    is_nullable = false,
                },
                {
                    name = "reestrstatus",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "vehicleregno",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "sys_op",
                    type = "number",
                    is_nullable = false,
                },
                {
                    name = "sys_to",
                    type = "number",
                    is_nullable = false,
                },
                {
                    name = "sys_from",
                    type = "number",
                    is_nullable = false,
                },
                {
                    name = "bucket_id",
                    type = "unsigned",
                    is_nullable = true,
                },
            },
            temporary = false,
            engine = "vinyl",
            indexes = {
                {
                    unique = true,
                    parts = {
                        {
                            path = "id",
                            type = "integer",
                            is_nullable = false,
                        },
                        {
                            path = "sys_op",
                            type = "number",
                            is_nullable = false,
                        }
                    },
                    type = "TREE",
                    name = "id",
                },
                {
                    unique = false,
                    parts = {
                        {
                            path = "bucket_id",
                            type = "unsigned",
                            is_nullable = true,
                        },
                    },
                    type = "TREE",
                    name = "bucket_id",
                },
                {
                    unique = false,
                    parts = {
                        {
                            path = "reestrid",
                            type = "unsigned",
                            is_nullable = false,
                        },
                    },
                    type = "TREE",
                    name = "reestrid",
                },
            },
            is_local = false,
            sharding_key = { "id" },
        }
    }
}

_, err = api:call("set_schema", { yaml.encode(config) })
fail_if_error(err, "Failed to set schema")
print("Initialized the schema")

fiber.sleep(1)
print("Start data loading")

local storage1 = nb.connect("admin:app-cluster-cookie@localhost:3302")
local storage2 = nb.connect("admin:app-cluster-cookie@localhost:3304")

_, err = storage1:eval("return box.space.vehicle_actual:truncate();")
fail_if_error(err, "Failed to truncate vehicle_actual on storage1")
_, err = storage1:eval("return box.space.vehicle_history:truncate();")
fail_if_error(err, "Failed to truncate vehicle_history on storage1")
_, err = storage2:eval("return box.space.vehicle_actual:truncate();")
fail_if_error(err, "Failed to truncate vehicle_actual on storage2")
_, err = storage2:eval("return box.space.vehicle_history:truncate();")
fail_if_error(err, "Failed to truncate vehicle_history on storage2")

local pattern = [[
insert into "%s" (
    "id",
    "reestrid",
    "vehicleguid",
    "reestrstatus",
    "vehicleregno",
    "vehiclevin",
    "vehiclevin2",
    "vehiclechassisnum",
    "vehiclereleaseyear",
    "operationregdoctypename",
    "operationregdoc",
    "operationregdocissuedate",
    "operationregdoccomments",
    "vehicleptstypename",
    "vehicleptsnum",
    "vehicleptsissuedate",
    "vehicleptsissuer",
    "vehicleptscomments",
    "vehiclebodycolor",
    "vehiclebrand",
    "vehiclemodel",
    "vehiclebrandmodel",
    "vehiclebodynum",
    "vehiclecost",
    "vehiclegasequip",
    "vehicleproducername",
    "vehiclegrossmass",
    "vehiclemass",
    "vehiclesteeringwheeltypeid",
    "vehiclekpptype",
    "vehicletransmissiontype",
    "vehicletypename",
    "vehiclecategory",
    "vehicletypeunit",
    "vehicleecoclass",
    "vehiclespecfuncname",
    "vehicleenclosedvolume",
    "vehicleenginemodel",
    "vehicleenginenum",
    "vehicleenginepower",
    "vehicleenginepowerkw",
    "vehicleenginetype",
    "holdrestrictiondate",
    "approvalnum",
    "approvaldate",
    "approvaltype",
    "utilizationfeename",
    "customsdoc",
    "customsdocdate",
    "customsdocissue",
    "customsdocrestriction",
    "customscountryremovalid",
    "customscountryremovalname",
    "ownerorgname",
    "ownerinn",
    "ownerogrn",
    "ownerkpp",
    "ownerpersonlastname",
    "ownerpersonfirstname",
    "ownerpersonmiddlename",
    "ownerpersonbirthdate",
    "ownerbirthplace",
    "ownerpersonogrnip",
    "owneraddressindex",
    "owneraddressmundistrict",
    "owneraddresssettlement",
    "owneraddressstreet",
    "ownerpersoninn",
    "ownerpersondoccode",
    "ownerpersondocnum",
    "ownerpersondocdate",
    "operationname",
    "operationdate",
    "operationdepartmentname",
    "operationattorney",
    "operationlising",
    "holdertypeid",
    "holderpersondoccode",
    "holderpersondocnum",
    "holderpersondocdate",
    "holderpersondocissuer",
    "holderpersonlastname",
    "holderpersonfirstname",
    "holderpersonmiddlename",
    "holderpersonbirthdate",
    "holderpersonbirthregionid",
    "holderpersonsex",
    "holderpersonbirthplace",
    "holderpersoninn",
    "holderpersonsnils",
    "holderpersonogrnip",
    "holderaddressguid",
    "holderaddressregionid",
    "holderaddressregionname",
    "holderaddressdistrict",
    "holderaddressmundistrict",
    "holderaddresssettlement",
    "holderaddressstreet",
    "holderaddressbuilding",
    "holderaddressstructureid",
    "holderaddressstructurename",
    "holderaddressstructure",
    "sys_from",
    "sys_to",
    "sys_op"
) values (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?
)]]

for i = 1, record_count, 1 do
    local pattern_vehicle_actual = string.format(pattern, "vehicle_actual")
    _, err = api:call("sbroad.execute", {
        pattern_vehicle_actual, {
            i, i,
            "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
            "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
            "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
            "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
            "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
            "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
            "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
            "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
            "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
            "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
            332, 332, 0
        } })
        fail_if_error(err, "Failed to insert record into vehicle_actual")

    local pattern_vehicle_history = string.format(pattern, "vehicle_history")
    _, err = api:call("sbroad.execute", {
        pattern_vehicle_history, {
            i, i,
            "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
            "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
            "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
            "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
            "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
            "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
            "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
            "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
            "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
            "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
            332, 332, 0
        } })
        fail_if_error(err, "Failed to insert record into vehicle_history")
end
api:close()

print("Finish data loading")
fiber.sleep(1)

print(
        string.format(
                "Storage 1 has %d actual and %d history records",
                storage1:eval("return box.space.vehicle_actual:count();"),
                storage1:eval("return box.space.vehicle_history:count();")
        )
)

print(
        string.format(
                "Storage 2 has %d actual and %d history records",
                storage2:eval("return box.space.vehicle_actual:count();"),
                storage2:eval("return box.space.vehicle_history:count();")
        )
)

print(

)

storage1:close()
storage2:close()

print(string.format("Execution time: %f s", (clock.time() - start_time) ))
