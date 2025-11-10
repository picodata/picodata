# Команды и термины SQL

<style>
h1,
form {
    column-span: all;
}

article {
    column-count: 3;
}

@media screen and (max-width: 59.984375em) {
    article {
        column-count: 2;
    }
}

@media screen and (max-width: 30em) {
    article {
        column-count: 1;
    }
}

article > p {
    margin-block-start: unset;
    break-after: avoid;
}

article.md-typeset.md-typeset ul {
    break-inside: avoid;
    list-style-type: none;
    margin-left: 0;
    li {
        margin-left: 0;
    }
}
</style>

<b>A</b>

* [ABS](reference/sql/abs.md)
* [ALTER INDEX](reference/sql/alter_index.md)
* [ALTER PLUGIN](reference/sql/alter_plugin.md)
* [ALTER PROCEDURE](reference/sql/alter_procedure.md)
* [ALTER SYSTEM](reference/sql/alter_system.md)
* [ALTER TABLE](reference/sql/alter_table.md)
* [ALTER USER](reference/sql/alter_user.md)
* [AUDIT POLICY](reference/sql/audit_policy.md)
* [AVG](reference/sql/aggregate.md#functions)

<b>B</b>

* [BACKUP](reference/sql/backup.md)
* [BOOLEAN](reference/sql_types.md#boolean)

<b>C</b>

* [CALL](reference/sql/call.md)
* [CASE](reference/sql/case.md)
* [CAST](reference/sql/cast.md)
* [COALESCE](reference/sql/coalesce.md)
* [column](reference/sql/object.md)
* [COUNT](reference/sql/aggregate.md#functions)
* [CREATE INDEX](reference/sql/create_index.md)
* [CREATE PLUGIN](reference/sql/create_plugin.md)
* [CREATE PROCEDURE](reference/sql/create_procedure.md)
* [CREATE ROLE](reference/sql/create_role.md)
* [CREATE TABLE](reference/sql/create_table.md)
* [CREATE USER](reference/sql/create_user.md)
* [CTE](reference/sql/with.md)
* [CURRENT_DATE](reference/sql/time_and_date.md#current_date)
* [CURRENT_TIMESTAMP](reference/sql/time_and_date.md#current_timestamp)

<b>D</b>

* [DATETIME](reference/sql_types.md#datetime)
* [DCL](reference/sql/dcl.md)
* [DDL](reference/sql/ddl.md)
* [DECIMAL](reference/sql_types.md#decimal)
* [DELETE](reference/sql/delete.md)
* [DISTINCT](reference/sql/select.md#params)
* [DML](reference/sql/dml.md)
* [DOUBLE](reference/sql_types.md#double)
* [DQL](reference/sql/dql.md)
* [DROP INDEX](reference/sql/drop_index.md)
* [DROP PLUGIN](reference/sql/drop_plugin.md)
* [DROP PROCEDURE](reference/sql/drop_procedure.md)
* [DROP ROLE](reference/sql/drop_role.md)
* [DROP TABLE](reference/sql/drop_table.md)
* [DROP USER](reference/sql/drop_user.md)

<b>E</b>

* [EXCEPT DISTINCT](reference/sql/select.md#except_with_subquery)
* [execution plan](overview/glossary.md#execution_plan)
* [EXPLAIN](reference/sql/explain.md)
* [expression](reference/sql/aggregate.md#expression)

<b>G</b>

* [GRANT](reference/sql/grant.md)
* [GROUP BY](reference/sql/select.md#filter_and_group)
* [GROUP_CONCAT](reference/sql/aggregate.md#functions)

<b>I</b>

* [ILIKE](reference/sql/ilike.md)
* [INDEXED BY](reference/sql/indexed_by.md)
* [INSTANCE_UUID](reference/sql/system_functions.md#instance_uuid)
* [INNER JOIN](reference/sql/join.md#inner_join)
* [INSERT](reference/sql/insert.md)
* [INTEGER](reference/sql_types.md#integer)

<b>J</b>

* [JOIN](reference/sql/join.md)
* [JSON_EXTRACT_PATH](reference/sql/json_extract_path.md)

<b>L</b>

* [LEFT OUTER JOIN](reference/sql/join.md#left_join)
* [LIKE](reference/sql/like.md)
* [LIMIT](reference/sql/select.md#params)
* [LIMIT ALL](reference/sql/select.md#params)
* [LIMIT NULL](reference/sql/select.md#params)
* [LOCALTIMESTAMP](reference/sql/time_and_date.md#localtimestamp)
* [LOWER](reference/sql/lower.md)

<b>M</b>

* [MAX](reference/sql/aggregate.md#functions)
* [MIN](reference/sql/aggregate.md#functions)
* [motion](reference/sql/explain.md#data_motion_types)

<b>O</b>

* [object](reference/sql/object.md)
* [opcode](overview/glossary.md#opcode)

<b>P</b>

* [PICO_INSTANCE_UUID](reference/sql/system_functions.md#pico_instance_uuid)
* [PICO_INSTANCE_NAME](reference/sql/system_functions.md#pico_instance_name)
* [PICO_REPLICASET_NAME](reference/sql/system_functions.md#pico_replicaset_name)
* [PICO_TIER_NAME](reference/sql/system_functions.md#pico_tier_name)
* [PICO_INSTANCE_DIR](reference/sql/system_functions.md#pico_instance_dir)
* [PICO_CONFIG_FILE_PATH](reference/sql/system_functions.md#pico_config_file_path)
* [PICO_RAFT_LEADER_ID](reference/sql/system_functions.md#pico_raft_leader_id)
* [PICO_RAFT_LEADER_UUID](reference/sql/system_functions.md#pico_raft_leader_uuid)
* [plugin](architecture/plugins.md)
* [procedure](admin/access_control.md#proc_access)
* [projection](reference/sql/explain.md#plan_structure)

<b>R</b>

* [REVOKE](reference/sql/revoke.md)
* [role](admin/access_control.md#role_model)

<b>S</b>

* [scan](reference/sql/explain.md#plan_structure)
* [SELECT](reference/sql/select.md)
* [sharding key](overview/glossary.md#sharding_key)
* [stored procedure](overview/glossary.md#stored_procedure)
* [SUBSTR](reference/sql/substr.md)
* [SUBSTRING](reference/sql/substring.md)
* [SUM](reference/sql/aggregate.md#functions)

<b>T</b>

* [table](overview/glossary.md#table)
* [TEXT](reference/sql_types.md#text)
* [TO_CHAR](reference/sql/time_and_date.md#to_char)
* [TO_DATE](reference/sql/time_and_date.md#to_date)
* [TOTAL](reference/sql/aggregate.md#functions)
* [TRIM](reference/sql/trim.md)
* [TRUNCATE TABLE](reference/sql/truncate_table.md)
* [type](reference/sql_types.md)

<b>U</b>

* [UNION](reference/sql/select.md#params)
* [UNION ALL](reference/sql/select.md#params)
* [UNSIGNED](reference/sql_types.md#unsigned)
* [UPDATE](reference/sql/update.md)
* [UPPER](reference/sql/lower.md)
* [user](admin/access_control.md#users)
* [UUID](reference/sql_types.md#uuid)

<b>V</b>

* [VALUES](reference/sql/values.md)
* [VARCHAR](reference/sql_types.md#text)
* [version](reference/sql/system_functions.md#version)

<b>W</b>

* [WHERE](reference/sql/select.md#select_with_filter)
* [window function](reference/sql/window.md)
