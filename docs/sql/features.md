# Поддерживаемые возможности SQL

## E011. Numeric data types.

### E011-01. INTEGER and SMALLINT data types (including all spellings).
1. Subclause 5.2, the reserved words INT, INTEGER, and SMALLINT : **no**
1. Subclause 5.3, sign unsigned integer: **yes**
1. Subclause 6.1, the INTEGER and SMALLINT exact numeric types:  **no**
1. Subclause 13.5, type correspondences for INTEGER and SMALLINT for all supported languages: **no**

### E011-02. REAL, DOUBLE PRECISON, and FLOAT data types.
1. Subclause 5.2, the reserved words REAL, DOUBLE PRECISION, and FLOAT: **no**
1. Subclause 5.3, sign approximate numeric literal: **yes**
1. Subclause 6.1, approximate numeric type: **yes**
1. Subclause 13.5, type correspondences for REAL, DOUBLE PRECISION, and FLOAT for all supported languages: **no**

### E011-03. DECIMAL and NUMERIC data types.
1. Subclause 5.2, the reserved words DECIMAL and NUMERIC: **no**
1. Subclause 5.3, exact numeric literal: **yes**
1. Subclause 6.1, the DECIMAL and NUMERIC exact numeric types: **no**
1. Subclause 13.5, type correspondences for DECIMAL and NUMERIC for all supported languages: **no**

### E011-04. Arithmetic operators.
1. Subclause 6.27, when the numeric primary is a value expression primary: **no**

### E011-05. Numeric comparison.
1. Subclause 8.2, for the numeric data types, without support for table subquery and without support for Feature F131, “Grouped operations”: **yes**

### E011-06. Implicit casting among the numeric data types.
1. Subclause 8.2, values of any of the numeric data types can be compared to each other; such values are compared with respect to their algebraic values: **yes**
1. Subclause 9.1, “Retrieval assignment”, and Subclause 9.2, “Store assignment”: Values of one numeric type can be assigned to another numeric type, subject to rounding, truncation, and out of range conditions: **yes**

## E021. Character string types.

### E021-01. CHARACTER data type (including all its spellings).
1. Subclause 5.2, the reserved words CHAR and CHARACTER: **no**
1. Subclause 6.1, the CHARACTER exact character string type: **yes**
1. Subclause 13.5, type correspondences for CHARACTER for all supported languages: **yes**

### E021-03. Character literals.
1.  Subclause 5.3: **yes**

### E021-04. CHARACTER_LENGTH function.
1. Subclause 6.28, the char length expression: **no**

### E021-05. OCTET_LENGTH function.
1. Subclause 6.28, the octet length expression: **no**

### E021-06. SUBSTRING function.
1. Subclause 6.30, the character substring function: **no**

### E021-07. Character concatenation.
1. Subclause 6.29, the concatenation expression: **yes**

### E021-08. UPPER and LOWER functions.
1. Subclause 6.30, the fold function: **no**

### E021-09. TRIM function.
1. Subclause 6.30, the trim function: **no**

### E021-10. Implicit casting among the fixed-length and variable- length character string types.
1. Subclause 8.2, values of either the CHARACTER or CHARACTER VARYING data types can be compared to each other: **no**
1. Subclause 9.1, “Retrieval assignment”, and Subclause 9.2, “Store assignment”: Values of either the CHARACTER or CHARACTER VARYING data type can be assigned to the other type, subject to truncation conditions: **no**

### E021-11. POSITION function.
1. Subclause 6.28, the position expression: **no**

### E021-12. Character comparison.
1. Subclause 8.2, for the CHARACTER and CHARACTER VARYING data types, without support for table sub-query and without support for Feature F131, “Grouped operations”: **yes**

## E031. Identifiers.

### E031-01. Delimited identifiers.
1. Subclause 5.2: **no**

### E031-02. Lower case identifiers.
1. Subclause 5.2, An alphabetic character in a regular identifier can be either lower case or upper case (meaning that non-delimited identifiers need not comprise only upper case letters): **yes**

### E031-03. Trailing underscore.
1. Subclause 5.2, The last identifier part in a regular identifier can be an underscore: **yes**

## E051. Basic query specification.

### E051-01. SELECT DISTINCT.
1.  Subclause 7.12, “query specification”: With a set quantifier of DISTINCT, but without subfeatures E051-02 through E051-09: **no**

### E051-02. GROUP BY clause.
1. Subclause 7.4, “table expression”: group by clause, but without subfeatures E051-04 through E051-09: **no**
1. Subclause 7.9, “group by clause”: With the restrictions that the group by clause shall contain all non-aggregated columns in the select list and that any column in the group by clause shall also appear in the select list: **no**

### E051-04. GROUP BY can contain columns not in select list.
1. Subclause 7.9, “group by clause”: Without the restriction that any column in the group by clause shall also appear in the select list: **no**

### E051-05. Select list items can be renamed.
1. Subclause 7.12, “query specification”: as clause:: **yes**

### E051-06. HAVING clause.
1. Subclause 7.4, “table expression”: having clause: **no**
1. Subclause 7.10, “having clause”: **no**

### E051-07. Qualified * in select list.
1. Subclause 7.12, “query specification”: qualified asterisk: **yes**

### E051-08. Correlation names in the FROM clause.
1. Subclause 7.6, “table reference”: [ AS ] correlation name: **yes**

### E051-09. Rename columns in the FROM clause.
1. Subclause 7.6, “table reference”: [ AS ] correlation name [ left paren derived column list right paren ]: **no**

## E061. Basic predicates and search conditions.

### E061-01. Comparison predicate.
1.  Subclause 8.2, “comparison predicate”: For supported data types, without support for table subquery: **yes**

### E061-02. BETWEEN predicate.
1. Subclause 8.3, “between predicate”: **yes**

### E061-03. IN predicate with list of values.
1. Subclause 8.4, “in predicate”: Without support for table subquery: **yes**

### E061-04. LIKE predicate.
1. Subclause 8.5, “like predicate”: Without [ ESCAPE escape character ]: **no**

### E061-05. LIKE predicate: ESCAPE clause.
1. Subclause 8.5, “like predicate”: With [ ESCAPE escape character ]: **no**

### E061-06. NULL predicate.
1. Subclause 8.8, “null predicate”: Without Feature F481, “Expanded NULL predicate”: **yes**

### E061-07. Quantified comparison predicate.
1. Subclause 8.2, “comparison predicate”: With support for table subquery: **no**

### E061-08. EXISTS predicate.
1. Subclause 8.9, “exists predicate”: **no**

### E061-09. Subqueries in comparison predicate.
1. Subclause 8.2, “comparison predicate”: With support for table subquery: **yes**

### E061-11. Subqueries in IN predicate.
1. Subclause 8.4, “in predicate”: With support for table subquery: **yes**

### E061-12. Subqueries in quantified comparison predicate.
1. Subclause 8.2, “quantified comparison predicate”: With support for table subquery: **no**

### E061-13. Correlated subqueries.
1. Subclause 8.1, “predicate”: When a correlation name can be used in a table subquery as a correlated reference to a column in the outer query: **no**

### E061-14. Search condition.
1. Subclause 8.21, "search condition": **yes**

## E071. Basic query expressions.

### E071-01. UNION DISTINCT table operator.
1. Subclause 7.13, “query expression”: With support for UNION [ DISTINCT ]: **no**

### E071-02. UNION ALL table operator.
1. Subclause 7.13, “query expression”: With support for UNION [ ALL ]: **yes**

### E071-03. EXCEPT DISTINCT table operator.
1. Subclause 7.13, “query expression”: With support for EXCEPT [ DISTINCT ]: **yes**

### E071-04. Columns combined via table operators need not have exactly the same data type.
1. Subclause 7.13, “query expression”: Columns combined via UNION and EXCEPT need not have exactly the same data type: **yes**

### E071-05. Table operators in subqueries.
1. Subclause 7.13, “query expression”: table subquerys can specify UNION and EXCEPT: **yes**

## E081. Basic Privileges.

### E081-01. SELECT privilege at the table level.
1. Subclause 12.3, “privileges”: With action of SELECT without privilege column list: **no**

### E081-02. DELETE privilege.
1. Subclause 12.4, “privileges”: With action of DELETE: **no**

### E081-03. INSERT privilege at the table level.
1. Subclause 12.5, “privileges”: With action of INSERT without privilege column list: **no**

### E081-04. UPDATE privilege at the table level.
1. Subclause 12.6, “privileges”: With action of UPDATE without privilege column list: **no**

### E081-05. UPDATE privilege at the col- umn level.
1. Subclause 12.6, “privileges”: With action of UPDATE left paren privilege column list right paren: **no**

### E081-06. SELECT privilege at the column level.
1. Subclause 12.3, “privileges”: With action of SELECT left paren privilege column list right paren: **no**

### E081-07. REFERENCES privilege at the column level.
1. Subclause 12.7, “privileges”: With action of REFERENCES left paren privilege column list right paren: **no**

### E081-08. WITH GRANT OPTION.
1. Subclause 12.3, “grant privilege statement”: WITH GRANT OPTION: **no**

### E081-09. USAGE privilege.
1. Subclause 12.8, “privileges”: With action of USAGE: **no**

### E081-10. EXECUTE privilege.
1. Subclause 12.9, “privileges”: With action of EXECUTE: **no**

## E091. Set functions.

### E091-01. AVG.
1. Subclause 6.9, “set function specification”: With computational operation of AVG: **no**

### E091-02. COUNT.
1. Subclause 6.9, “set function specification”: With computational operation of COUNT: **no**

### E091-03. MAX.
1. Subclause 6.9, “set function specification”: With computational operation of MAX: **no**

### E091-04. MIN.
1. Subclause 6.9, “set function specification”: With computational operation of MIN: **no**

### E091-05. SUM.
1. Subclause 6.9, “set function specification”: With computational operation of SUM: **no**

### E091-06. ALL quantifier.
1. Subclause 6.9, “set function specification”: With set quantifier of ALL: **no**

### E091-07. DISTINCT quantifier.
1. Subclause 6.9, “set function specification”: With set quantifier of DISTINCT: **no**

## E101. Basic data manipulation.

### E101-01. INSERT statement.
1. Subclause 14.11, “insert statement”: When a contextually typed table value constructor can consist of no more than a single contextually typed row value expression: **yes**

### E101-03. Searched UPDATE statement.
1. Subclause 14.14, “update statement: searched”: But without support either of Feature E153, “Updatable queries with subqueries”, or Feature F221, “Explicit defaults”: **no**

### E101-04. Searched DELETE statement.
1. Subclause 14.9, “delete statement: searched”: **no**

## E111. Single row SELECT statement.
1. Subclause 14.7, “select statement: single row”: Without support of Feature F131, “Grouped operations”: **yes**

## E121. Basic cursor support.

### E121-01. DECLARE CURSOR.
1. Subclause 14.1, “declare cursor”: When each value expression in the sort key shall be a column reference and that column reference shall also be in the select list, and cursor holdability is not specified: **no**

### E121-02. ORDER BY columns need not be in select list.
1. Subclause 14.1, “declare cursor”: Extend subfeature E121-01 so that column reference need not also be in the select list: **no**

### E121-03. Value expressions in ORDER BY clause.
1. Subclause 14.1, “declare cursor”: Extend subfeature E121-01 so that the value expression in the sort key need not be a column reference: **no**

### E121-04. OPEN statement.
1. Subclause 14.4, “open statement”: **no**

### E121-06. Positioned UPDATE statement.
1. Subclause 14.13, “update statement: positioned”: Without support of either Feature E153, “Updateable queries with subqueries” or Feature F221, “Explicit defaults”: **no**

### E121-07. Positioned DELETE statement.
1. Subclause 14.8, “delete statement: positioned”: **no**

### E121-08. CLOSE statement.
1. Subclause 14.5, “close statement”: **no**

### E121-10. FETCH statement: implicit NEXT.
1. Subclause 14.6, “fetch statement”: **no**

### E121-17. WITH HOLD cursors.
1.Subclause 14.1, “declare cursor”: Where the value expression in the sort key need not be a column reference and need not be in the select list, and cursor holdability may be specified: **no** 

## E131. Null value support (nulls in lieu of values).
1. Subclause 4.13, “Columns, fields, and attributes”: Nullability characteristic: **yes**
1. Subclause 6.5, “contextually typed value specification”: null specification: **yes**

## E141. Basic integrity constraints.

### E141-01. NOT NULL constraints.
1. Subclause 11.6, “table constraint definition”: As specified by the subfeatures of this feature in this table: **no**

### E141-02. UNIQUE constraints of NOT NULL columns.
1. Subclause 11.4, “column definition”: With unique specification of UNIQUE for columns specified as NOT NULL: **no**
1. Subclause 11.7, “unique constraint definition”: With unique specification of UNIQUE: **no**

### E141-03. PRIMARY KEY constraints.
1. Subclause 11.4, “column definition”: With primary key specification of PRIMARY KEY: **no**
1. Subclause 11.7, “unique constraint definition”: With unique specification of PRIMARY KEY: **no**

### E141-04. Basic FOREIGN KEY con- straint with the NO ACTION default for both referential delete action and referential update action.
1. Subclause 11.4, “column definition”: With column constraint of references specification: **no**
1. Subclause 11.8, “referential constraint definition”: Where the columns in the column name list, if specified, shall be in the same order as the names in the unique column list of the applicable unique constraint definition and the data types of the matching columns shall be the same: **no**

### E141-06. CHECK constraints.
1. Subclause 11.4, “column definition”: With column constraint of check constraint definition: **no**
1. Subclause 11.9, “check constraint definition”: **no**

### E141-07. Column defaults.
1. Subclause 11.4, “column definition”: With default clause: **no**

### E141-08. NOT NULL inferred on PRIMARY KEY.
1. Subclause 11.4, “column definition”, and Subclause 11.7, “unique constraint definition”: Remove the restriction in subfeatures E141-02 and E141-03 that NOT NULL be specified along with every PRIMARY KEY and UNIQUE constraint: **no** 
1. Subclause 11.4, “column definition”: NOT NULL is implicit on PRIMARY KEY constraints: **no**

### E141-10. Names in a foreign key can be specified in any order.
1. Subclause 11.4, “column definition”, and Subclause 11.8, “referential constraint definition”: Extend subfeature: **no**
1. E141-04 so that the columns in the column name list, if specified, need not be in the same order as the names in the unique column list of the applicable unique constraint definition: **no**

## E151. Transaction support.

### E151-01. COMMIT statement.
1. Subclause 17.7, “commit statement”: **no**

### E151-02. ROLLBACK statement.
1. Subclause 17.8, “rollback statement”: **no**

## E152. Basic SET TRANSACTION statement.

### E152-01. SET TRANSACTION statement: ISOLATION LEVEL SERIALIZABLE clause.
1. Subclause 17.2, “set transaction statement”: With transaction mode of ISOLATION LEVEL SERIALIZABLE clause: **no**

### E152-02. SET TRANSACTION statement: READ ONLY and READ WRITE clauses.
1. Subclause 17.2, “set transaction statement”: with transaction access mode of READ ONLY or READ WRITE: **no** 

## E153. Updatable queries with subqueries.
1. Subclause 7.13, “query expression”: A query expression is updatable even though its where clause contains a subquery: **no**

## E161. SQL comments using leading double minus.
1. Subclause 5.2, “token and separator”: simple comment: **no**

## E171. SQLSTATE support.
1. Subclause 24.1, “SQLSTATE”: **no**

## E182. Host language binding.
1. Clause 13, “SQL-client modules”: **no**

## F031. Basic schema manipulation.

### F031-01. CREATE TABLE statement to create persistent base tables.
1. Subclause 11.3, “table definition”: Not in the context of a schema definition: **no**

### F031-02. CREATE VIEW statement.
1.  Subclause 11.32, “view definition”: Not in the context of a schema definition, and without support of Feature F081, “UNION and EXCEPT in views”: **no**

### F031-03. GRANT statement.
1. Subclause 12.1, “grant statement”: Not in the context of a schema definition: **no**

### F031-04. ALTER TABLE statement: ADD COLUMN clause.
1. Subclause 11.10, “alter table statement”: The add column definition clause: **no**
1. Subclause 11.11, “add column definition”: **no**

### F031-13. DROP TABLE statement: RESTRICT clause.
1. Subclause 11.31, “drop table statement”: With a drop behavior of RESTRICT: **no**

### F031-16. DROP VIEW statement: RESTRICT clause.
1. Subclause 11.33, “drop view statement”: With a drop behavior of RESTRICT: **no**

### F031-19. REVOKE statement: RESTRICT clause.
1. Subclause 12.7, “revoke statement”: With a drop behavior of RESTRICT, only where the use of this statement can be restricted to the owner of the table being dropped: **no**

## F041. Basic joined table.

### F041-01. Inner join (but not necessarily the INNER keyword).
1. Subclause 7.6, “table reference”: The joined table clause, but without support for subfeatures F041-02 through F041-08: **yes**

### F041-02. INNER keyword.
1. Subclause 7.7, “joined table”: join type of INNER: **yes**

### F041-03. LEFT OUTER JOIN.
1. Subclause 7.7, “joined table”: outer join type of LEFT: **no**

### F041-04. RIGHT OUTER JOIN.
1. Subclause 7.7, “joined table”: outer join type of RIGHT: **no**

### F041-05. Outer joins can be nested.
1. Subclause 7.7, “joined table”: Subfeature F041-01 extended so that a table reference within the joined table can itself be a joined table: **yes**

### F041-07. The inner table in a left or right outer join can also be used in an inner join.
1. Subclause 7.7, “joined table”: Subfeature F041-01 extended so that a table name within a nested joined table can be the same as a table name in an outer joined table: **no**

### F041-08. All comparison operators are supported (rather than just =.
1. Subclause 7.7, “joined table”: Subfeature F041-01 extended so that the join condition is not limited to a comparison predicate with a comp op of equals operator: **yes**

## F051. Basic date and time.

### F051-01. DATE data type (including support of DATE literal).
1. Subclause 5.3, “literal”: The date literal form of datetime literal: **no**
1. Subclause 6.1, “data type”: The DATE datetime type: **no**
1. Subclause 6.31, “datetime value expression”: For values of type DATE: **no**

### F051-02. TIME data type (including support of TIME literal) with fractional seconds precision of at least 0.
1. Subclause 5.3, “literal”: The time literal form of datetime literal, where the value of unquoted time string simply contains time value that does not include the optional time zone interval: **no**
1. Subclause 6.1, “data type”: The TIME datetime type without the with or without time zone clause: **no** 
1. Subclause 6.31, “datetime value expression”: For values of type TIME: **no**

### F051-03. TIMESTAMP data type (including support of TIMES- TAMP literal) with fractional seconds precision of at least 0 and 6.
1. Subclause 5.3, “literal”: The timestamp literal form of datetime literal, where the value of unquoted timestamp string simply contains a time value that does not include the optional time zone interval: **no**
1. Subclause 6.1, “data type”: The TIMESTAMP datetime type without the with or without time zone clause: **no**
1. Subclause 6.31, “datetime value expression”: For values of type TIMESTAMP: **no**

### F051-04. Comparison predicate on DATE, TIME, and TIMESTAMP data types.
1. Subclause 8.2, “comparison predicate”: For comparison between values of the following types: DATE and DATE, TIME and TIME, TIMESTAMP and TIMESTAMP: **no**

### F051-05. Explicit CAST between datetime types and character string types.
1. Subclause 6.13, “cast specification”: If support for Feature F201, “CAST function” is available, then CASTing between the following types: from character string to DATE, TIME, and TIMESTAMP; from DATE to DATE, TIMES- TAMP, and character string; from TIME to TIME, TIMESTAMP, and character string; from TIMESTAMP to DATE, TIME, TIMESTAMP, and character string: **no**

### F051-06. CURRENT_DATE.
1. Subclause 6.32, “datetime value function”: The current date value function: **no**
1. Subclause 6.31, “datetime value expression”: When the value is a current date value function: **no**

### F051-07. LOCALTIME.
1. Subclause 6.32, “datetime value function”: The current local time value function: **no**
1. Subclause 6.31, “datetime value expression”: When the value is a current local time value function: **no**
1. Subclause 11.5, “default clause”: LOCALTIME option of datetime value function: **no**

### F051-08. LOCALTIMESTAMP.
1. Subclause 6.32, “datetime value function”: The current local timestamp value function: **no**
1. Subclause 6.31, “datetime value expression”: When the value is a current local timestamp value function: **no**
1. Subclause 11.5, “default clause”: LOCALTIMESTAMP option of datetime value function: **no**

## F081. UNION and EXCEPT in views.
1. Subclause 11.32, “view definition”: A query expression in a view definition may specify UNION, UNION ALL, and/or EXCEPT: **no**

## F131. Grouped operations.

### F131-01. WHERE, GROUP BY, and HAVING clauses supported in queries with grouped views.
1. Subclause 7.4, “table expression”: Even though a table in the from clause is a grouped view, the where clause, group by clause, and having clause may be specified: **no**

### F131-02. Multiple tables supported in queries with grouped views.
1. Subclause 7.5, “from clause”: Even though a table in the from clause is a grouped view, the from clause may specify more than one table reference: **no**

### F131-03. Set functions supported in queries with grouped views.
1. Subclause 7.12, “query specificationg”: Even though a table in the from clause is a grouped view, the select list may specify a set function specification: **no**

### F131-04. Subqueries with GROUP BY and HAVING clauses and grouped views.
1. Subclause 7.15, “subquery”: A subquery in a comparison predicate is allowed to contain a group by clause and/or a having clause and/or it may identify a grouped view: **no**

### F131-05. Single row SELECT with GROUP BY and HAVING clauses and grouped views.
1. Subclause 14.7, “select statement: single row”: The table in a from clause can be a grouped view: **no**
1. Subclause 14.7, “select statement: single row”: The table expression may specify a group by clause and/or a having clause: **no**

## F181. Multiple module support.
1. Subclause 13.1, “SQL-client module definition”:An SQL-agent can be associated with more than one SQL-client module definition With this feature, it is possible to compile SQL-client module definitions or embedded SQL host programs separately and rely on the SQL-implementation to “link” them together properly at execution time. To ensure portability, applications should adhere to the following limitations:
— Avoid linking modules having cursors with the same cursor name.
— Avoid linking modules that prepare statements using the same SQL statement name. —Avoid linking modules that allocate descriptors with the same descriptor name.
— Assume that the scope of an embedded exception declaration is a single compilation unit.
— Assume that an embedded variable name can be referenced only in the same compilation unit in which it is declared:
**no**

## F201. CAST function.
1. Subclause 6.13, “cast specification”: For all supported data types: **yes**
1. Subclause 6.26, “value expression”: cast specification: **yes**

## F221. Explicit defaults.
1. Subclause 6.5, “contextually typed value specification”: default specification: **no**

## F261. CASE expression.

### F261-01. Simple CASE.
1. Subclause 6.12, “case expression”: The simple case variation: **no**

### F261-02. Searched CASE.
1. Subclause 6.12, “case expression”: The searched case variation: **no**

### F261-03. NULLIF.
1. Subclause 6.12, “case expression”: The NULLIF case abbreviation: **no**

### F261-04. COALESCE.
1. Subclause 6.12, “case expression”: The COALESCE case abbreviation: **no**

## F311. Schema definition statement.

### F311-01. CREATE SCHEMA.
1. Subclause 11.1, “schema definition”: Support for circular references in that referential constraint definitions in two different table definitions may reference columns in the other table: **no**

### F311-02. CREATE TABLE for persistent base tables.
1. Subclause 11.1, “schema definition”: A schema element that is a table definition: **no**
1. Subclause 11.3, “table definition”: In the context of a schema definition: **no**

### F311-03. CREATE VIEW.
1. Subclause 11.1, “schema definition”: A schema element that is a view definition: **no**
1. Subclause 11.32, “view definition”: In the context of a schema definition without the WITH CHECK OPTION clause and without support of Feature F081, “UNION and EXCEPT in views”: **no**

### F311-04. CREATE VIEW: WITH CHECK OPTION.
1. Subclause 11.32, “view definition”: The WITH CHECK OPTION clause, in the context of a schema definition, but without support of Feature F081, “UNION and EXCEPT in views”: **no**

### F311-05. GRANT statement.
1. Subclause 11.1, “schema definition”: A schema element that is a grant statement: **no**
1. Subclause 12.1, “grant statement”: In the con- text of a schema definition: **no**

## F471. Scalar subquery values.
1. Subclause 6.26, “value expression”: A value expression primary can be a scalar subquery: **yes**

## F481. Expanded NULL predicate.
1. Subclause 8.8, “null predicate”: The row value expression can be something other than a column reference: **no**

## F812. Basic flagging.
1. Part 1, Subclause 8.5, “SQL flagger”: With “level of flagging” specified to be Core SQL Flagging and “extent of checking” specified to be Syntax Only: **no**

## S011. Distinct data types.
1. Subclause 11.51, “user-defined type definition”: When representation is predefined type: **no**
1. Subclause 11.59, “drop data type statement”: **no**

## T321. Basic SQL-invoked routines.

### T321-01. User-defined functions with no overloading.
1. Subclause 11.60, “SQL-invoked routine”: With function specification: **no**

### T321-02. User-defined stored procedures with no overloading.
1. Subclause 11.60, “SQL-invoked routine”: With SQL-invoked procedure: **no**

### T321-03. Function invocation.
1. Subclause 6.4, “value specification and target specification”: With a value expression primary that is a routine invocation: **no**
1. Subclause 10.4, “routine invocation”: For user-defined functions: **no**

### T321-04. CALL statement.
1. Subclause 10.4, “routine invocation”: Used by call statements: **no**
1. Subclause 16.1, “call statement”: **no**

### T321-05. RETURN statement.
1. Subclause 16.2, “return statement”, if the SQL-implementation supports SQL routines: **no**

## T631. IN predicate with one list element.
1. Subclause 8.4, “in predicate”: in value list containing exactly one row value expression: **yes**


---
[Исходный код страницы](https://git.picodata.io/picodata/picodata/docs/-/blob/main/docs/sql/features.md)
