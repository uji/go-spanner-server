Test DELETE DML statements

-- ddl.sql --
CREATE TABLE Singers (
    SingerId INT64 NOT NULL,
    FirstName STRING(1024),
    LastName STRING(1024),
) PRIMARY KEY (SingerId)

-- dml.sql --
INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (1, 'Marc', 'Richards');
INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (2, 'Catalina', 'Smith');
INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (3, 'Alice', 'Trentor');
INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (4, 'Maria', 'Garcia')

-- dml.sql --
DELETE FROM Singers WHERE SingerId = 2

-- query.sql --
SELECT SingerId FROM Singers ORDER BY SingerId

-- expect.out --
(1)
(3)
(4)

-- dml.sql --
DELETE FROM Singers WHERE LastName LIKE 'G%'

-- query.sql --
SELECT SingerId FROM Singers ORDER BY SingerId

-- expect.out --
(1)
(3)
