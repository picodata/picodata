-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- The SQL runner connects to the leader, whose UUID must match pico_instance_uuid().

-- TEST: leader-equals-instance-uuid
-- SQL:
SELECT pico_raft_leader_uuid() = pico_instance_uuid();
-- EXPECTED:
true

-- TEST: leader-differs-from-instance-uuid
-- SQL:
SELECT pico_raft_leader_uuid() <> pico_instance_uuid();
-- EXPECTED:
false

-- TEST: leader-equals-system-column-one-instance
-- SKIP_FOR: 2rsX1
-- SQL:
SELECT pico_raft_leader_uuid() = uuid FROM _pico_instance;
-- EXPECTED:
true

-- TEST: leader-differs-from-system-column-one-instance
-- SKIP_FOR: 2rsX1
-- SQL:
SELECT pico_raft_leader_uuid() <> uuid FROM _pico_instance;
-- EXPECTED:
false

-- TEST: leader-equals-system-column-two-instances
-- SKIP_FOR: 1rsX1
-- SQL:
SELECT pico_raft_leader_uuid() = uuid FROM _pico_instance;
-- UNORDERED:
true, false

-- TEST: leader-differs-from-system-column-two-instances
-- SKIP_FOR: 1rsX1
-- SQL:
SELECT pico_raft_leader_uuid() <> uuid FROM _pico_instance;
-- UNORDERED:
false, true

-- TEST: leader-uuid-through-subquery
-- SQL:
SELECT x = pico_instance_uuid(), x <> pico_instance_uuid()
FROM (SELECT pico_raft_leader_uuid() AS x) AS s;
-- EXPECTED:
true, false

-- TEST: leader-uuid-text-parameter
-- SQL:
SELECT pico_raft_leader_uuid() = $1::text, pico_raft_leader_uuid() <> $1::text;
-- PARAMS:
''
-- EXPECTED:
false, true
