-- TEST: test_join
-- SQL:
DROP TABLE IF EXISTS testing_space;
CREATE TABLE testing_space ("id" int primary key, "name" string, "product_units" int);
insert into "testing_space" ("id", "name", "product_units") values
    (1, 'a', 1),
    (2, 'a', 1),
    (3, 'a', 2),
    (4, 'b', 1),
    (5, 'b', 2),
    (6, 'b', 3),
    (7, 'c', 4);

-- TEST: join1
-- SQL:
select *
          from
            "testing_space"
          join
            (select t1."id" as f, t2."id" as s
             from
                (select "id" from "testing_space") t1
             join
                (select "id" from "testing_space") t2
             on true
            )
          on "id" = f and "id" = s order by 1
-- EXPECTED:
1, 'a', 1, 1, 1,
2, 'a', 1, 2, 2,
3, 'a', 2, 3, 3,
4, 'b', 1, 4, 4,
5, 'b', 2, 5, 5,
6, 'b', 3, 6, 6,
7, 'c', 4, 7, 7
