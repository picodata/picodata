-- TEST: audit_policy_error
-- SQL:
AUDIT POLICY my_policy BY pico_service;
AUDIT POLICY dml_default;
AUDIT POLICY dml_default ON pico_service;
-- ERROR:
audit policy my_policy does not exist
rule parsing error
rule parsing error
