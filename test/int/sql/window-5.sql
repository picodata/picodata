-- TEST: window5
-- SQL:
DROP TABLE IF EXISTS sales;
CREATE TABLE sales(emp TEXT PRIMARY KEY, region TEXT, total INT);
INSERT INTO sales VALUES
('Alice',     'North', 34),
('Frank',     'South', 22),
('Charles',   'North', 45),
('Darrell',   'South', 8),
('Grant',     'South', 23),
('Brad' ,     'North', 22),
('Elizabeth', 'South', 99),
('Horace',    'East',   1);


-- TEST: window5-10.1
-- SQL:
SELECT emp, region, total FROM (
    SELECT
    emp, region, total,
    row_number() OVER (
        PARTITION BY region ORDER BY total DESC
    ) AS rownumber
    FROM sales
) WHERE rownumber::int <= 2 ORDER BY region, total DESC;
-- EXPECTED:
'Horace', 'East', 1,
'Charles', 'North', 45,
'Alice', 'North', 34,
'Elizabeth', 'South', 99,
'Grant', 'South', 23

-- TEST: window5-10.2
-- SQL:
SELECT emp, region, sum(total) OVER win FROM sales
WINDOW win AS (PARTITION BY region ORDER BY total)
ORDER BY emp;
-- EXPECTED:
'Alice', 'North', 56,
'Brad', 'North', 22,
'Charles', 'North', 101,
'Darrell', 'South', 8,
'Elizabeth', 'South', 152,
'Frank', 'South', 30,
'Grant', 'South', 53,
'Horace', 'East', 1

-- TEST: window5-10.3
-- SQL:
SELECT emp, region, sum(total) OVER win FROM sales
WINDOW win AS (PARTITION BY region ORDER BY total)
ORDER BY emp LIMIT 5;
-- EXPECTED:
'Alice', 'North', 56,
'Brad', 'North', 22,
'Charles', 'North', 101,
'Darrell', 'South', 8,
'Elizabeth', 'South', 152

-- TEST: window5-10.5
-- SQL:
SELECT emp, region, sum(total) OVER win FROM sales
WINDOW win AS (
    PARTITION BY region ORDER BY total
    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
)
ORDER BY emp;
-- EXPECTED:
'Alice', 'North', 79,
'Brad', 'North', 101,
'Charles', 'North', 45,
'Darrell', 'South', 152,
'Elizabeth', 'South', 99,
'Frank', 'South', 144,
'Grant', 'South', 122,
'Horace', 'East', 1

-- TEST: window5-10.7
-- SQL:
SELECT emp, region, CAST((
    SELECT sum(total) OVER (
    ORDER BY total RANGE BETWEEN UNBOUNDED PRECEDING
    AND UNBOUNDED FOLLOWING
    ) FROM sales LIMIT 1
) AS TEXT) || emp FROM sales
ORDER BY emp;
-- EXPECTED:
'Alice','North','254Alice',
'Brad','North','254Brad',
'Charles','North','254Charles',
'Darrell','South','254Darrell',
'Elizabeth','South','254Elizabeth',
'Frank','South','254Frank',
'Grant','South','254Grant',
'Horace','East','254Horace'
