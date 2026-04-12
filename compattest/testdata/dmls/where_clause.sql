Test various WHERE clause conditions

-- ddl.sql --
CREATE TABLE Singers (
    SingerId INT64 NOT NULL,
    FirstName STRING(1024),
    LastName STRING(1024),
) PRIMARY KEY (SingerId)

-- dml.sql --
INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (1, 'Marc', 'Richards');
INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (2, 'Catalina', 'Smith');
INSERT INTO Singers (SingerId, FirstName) VALUES (3, 'Alice');
INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (4, 'Maria', 'Garcia')

-- query.sql --
SELECT SingerId FROM Singers WHERE SingerId = 1

-- expect.out --
(1)

-- query.sql --
SELECT SingerId FROM Singers WHERE SingerId > 1 AND SingerId < 4 ORDER BY SingerId

-- expect.out --
(2)
(3)

-- query.sql --
SELECT SingerId FROM Singers WHERE FirstName LIKE 'Ma%' ORDER BY SingerId

-- expect.out --
(1)
(4)

-- query.sql --
SELECT SingerId FROM Singers WHERE SingerId IN (1, 3) ORDER BY SingerId

-- expect.out --
(1)
(3)

-- query.sql --
SELECT SingerId FROM Singers WHERE LastName IS NULL

-- expect.out --
(3)
