Test SQL aggregate functions: COUNT, SUM, AVG, MIN, MAX, GROUP BY, HAVING, DISTINCT

-- ddl.sql --
CREATE TABLE Orders (
    OrderId INT64 NOT NULL,
    Region  STRING(64),
    Amount  INT64,
) PRIMARY KEY (OrderId)

-- dml.sql --
INSERT INTO Orders (OrderId, Region, Amount) VALUES (1, 'East', 100);
INSERT INTO Orders (OrderId, Region, Amount) VALUES (2, 'East', 200);
INSERT INTO Orders (OrderId, Region, Amount) VALUES (3, 'West', 300);
INSERT INTO Orders (OrderId, Region, Amount) VALUES (4, 'West', 400);
INSERT INTO Orders (OrderId, Region, Amount) VALUES (5, 'East', 150)

-- query.sql --
SELECT COUNT(*) FROM Orders

-- expect.out --
(5)

-- query.sql --
SELECT SUM(Amount) FROM Orders

-- expect.out --
(1150)

-- query.sql --
SELECT MIN(Amount) FROM Orders

-- expect.out --
(100)

-- query.sql --
SELECT MAX(Amount) FROM Orders

-- expect.out --
(400)

-- query.sql --
SELECT AVG(Amount) FROM Orders

-- expect.out --
(230.0)

-- query.sql --
SELECT Region, COUNT(*) AS cnt FROM Orders GROUP BY Region ORDER BY Region

-- expect.out --
("East", 3)
("West", 2)

-- query.sql --
SELECT Region, SUM(Amount) AS total FROM Orders GROUP BY Region ORDER BY Region

-- expect.out --
("East", 450)
("West", 700)

-- query.sql --
SELECT Region, COUNT(*) AS cnt FROM Orders GROUP BY Region HAVING COUNT(*) >= 3

-- expect.out --
("East", 3)

-- query.sql --
SELECT DISTINCT Region FROM Orders ORDER BY Region

-- expect.out --
("East")
("West")

-- query.sql --
SELECT COUNT(*) FROM Orders WHERE Amount > 9999

-- expect.out --
(0)
