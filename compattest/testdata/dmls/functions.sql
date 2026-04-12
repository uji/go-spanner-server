Test SQL functions: UPPER, LOWER, CONCAT, COALESCE, CAST, CASE, IF

-- ddl.sql --
CREATE TABLE Singers (
    SingerId INT64 NOT NULL,
    FirstName STRING(1024),
    LastName STRING(1024),
    NickName STRING(1024),
) PRIMARY KEY (SingerId)

-- dml.sql --
INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (1, 'Marc', 'Richards');
INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (2, 'catalina', 'smith');
INSERT INTO Singers (SingerId, FirstName) VALUES (3, 'Alice')

-- query.sql --
SELECT UPPER(FirstName) FROM Singers WHERE SingerId = 1

-- expect.out --
("MARC")

-- query.sql --
SELECT LOWER(FirstName) FROM Singers WHERE SingerId = 2

-- expect.out --
("catalina")

-- query.sql --
SELECT CONCAT(FirstName, ' ', LastName) FROM Singers WHERE SingerId = 1

-- expect.out --
("Marc Richards")

-- query.sql --
SELECT COALESCE(NickName, FirstName) FROM Singers WHERE SingerId = 3

-- expect.out --
("Alice")

-- query.sql --
SELECT CAST(SingerId AS STRING) FROM Singers WHERE SingerId = 1

-- expect.out --
("1")

-- query.sql --
SELECT CASE WHEN SingerId = 1 THEN 'one' WHEN SingerId = 2 THEN 'two' ELSE 'other' END FROM Singers ORDER BY SingerId

-- expect.out --
("one")
("two")
("other")

-- query.sql --
SELECT IF(SingerId > 1, 'big', 'small') FROM Singers WHERE SingerId = 2

-- expect.out --
("big")

-- query.sql --
SELECT LENGTH(FirstName) FROM Singers WHERE SingerId = 1

-- expect.out --
(4)

-- query.sql --
SELECT STARTS_WITH(FirstName, 'Ma') FROM Singers WHERE SingerId = 1

-- expect.out --
(true)

-- query.sql --
SELECT ENDS_WITH(FirstName, 'ce') FROM Singers WHERE SingerId = 3

-- expect.out --
(true)

-- query.sql --
SELECT SUBSTR(FirstName, 2) FROM Singers WHERE SingerId = 1

-- expect.out --
("arc")

-- query.sql --
SELECT TRIM('  hello  ')

-- expect.out --
("hello")

-- query.sql --
SELECT REPLACE(FirstName, 'a', 'A') FROM Singers WHERE SingerId = 1

-- expect.out --
("MArc")

-- query.sql --
SELECT STRPOS(FirstName, 'ar') FROM Singers WHERE SingerId = 1

-- expect.out --
(2)

-- query.sql --
SELECT REVERSE(FirstName) FROM Singers WHERE SingerId = 1

-- expect.out --
("craM")

-- query.sql --
SELECT REPEAT('ab', 3)

-- expect.out --
("ababab")

-- query.sql --
SELECT LPAD(FirstName, 6, '0') FROM Singers WHERE SingerId = 1

-- expect.out --
("00Marc")

-- query.sql --
SELECT RPAD(FirstName, 6, '0') FROM Singers WHERE SingerId = 1

-- expect.out --
("Marc00")

-- query.sql --
SELECT ABS(-42)

-- expect.out --
(42)

-- query.sql --
SELECT MOD(10, 3)

-- expect.out --
(1)

-- query.sql --
SELECT CEIL(1.3)

-- expect.out --
(2.0)

-- query.sql --
SELECT FLOOR(1.7)

-- expect.out --
(1.0)

-- query.sql --
SELECT ROUND(2.5)

-- expect.out --
(3.0)

-- query.sql --
SELECT SIGN(-5)

-- expect.out --
(-1)

-- query.sql --
SELECT GREATEST(1, 3, 2)

-- expect.out --
(3)

-- query.sql --
SELECT LEAST(3, 1, 2)

-- expect.out --
(1)

-- query.sql --
SELECT IFNULL(NULL, 'fallback')

-- expect.out --
("fallback")

-- query.sql --
SELECT NULLIF('hello', 'world')

-- expect.out --
("hello")

-- query.sql --
SELECT SAFE_CAST('not_a_number' AS INT64)

-- expect.out --
(NULL)

-- query.sql --
SELECT SingerId FROM Singers ORDER BY SingerId LIMIT 2

-- expect.out --
(1)
(2)

-- query.sql --
SELECT SingerId FROM Singers ORDER BY SingerId LIMIT 2 OFFSET 1

-- expect.out --
(2)
(3)
