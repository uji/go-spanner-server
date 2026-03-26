Test various WHERE clause conditions

-- ddl --
CREATE TABLE Singers (
    SingerId INT64 NOT NULL,
    FirstName STRING(1024),
    LastName STRING(1024),
) PRIMARY KEY (SingerId)

-- exec --
INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (1, 'Marc', 'Richards');
INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (2, 'Catalina', 'Smith');
INSERT INTO Singers (SingerId, FirstName) VALUES (3, 'Alice');
INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (4, 'Maria', 'Garcia')

-- query --
SELECT SingerId FROM Singers WHERE SingerId = 1

-- expect --
(1)

-- query --
SELECT SingerId FROM Singers WHERE SingerId > 1 AND SingerId < 4 ORDER BY SingerId

-- expect --
(2)
(3)

-- query --
SELECT SingerId FROM Singers WHERE FirstName LIKE 'Ma%' ORDER BY SingerId

-- expect --
(1)
(4)

-- query --
SELECT SingerId FROM Singers WHERE SingerId IN (1, 3) ORDER BY SingerId

-- expect --
(1)
(3)

-- query --
SELECT SingerId FROM Singers WHERE LastName IS NULL

-- expect --
(3)
