#!/bin/bash

exec tarantoolctl connect ${1:-3301} << _SCRIPT_
_G.fselect_max_width = 200
local whoami = pico.whoami()
local raft_status = pico.raft_status()
return
{instance_id = whoami.instance_id},
{raft_state = raft_status.raft_state},
{voters = box.space.raft_state:get("voters").value},
{learners = box.space.raft_state:get("learners").value},
{instances = box.space.raft_group:fselect()},
{replicasets = box.space.replicasets:fselect()}
_SCRIPT_
