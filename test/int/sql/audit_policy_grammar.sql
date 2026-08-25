-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- TEST: audit_policy_error
-- SQL:
AUDIT POLICY my_policy BY pico_service;
AUDIT POLICY dml_default;
AUDIT POLICY dml_default ON pico_service;
-- ERROR:
audit policy my_policy does not exist
rule parsing error
rule parsing error
