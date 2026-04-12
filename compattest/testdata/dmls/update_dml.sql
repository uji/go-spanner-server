Test UPDATE DML statements

-- ddl.sql --
CREATE TABLE Singers (
    SingerId INT64 NOT NULL,
    FirstName STRING(1024),
    LastName STRING(1024),
    Age INT64,
) PRIMARY KEY (SingerId)

-- dml.sql --
INSERT INTO Singers (SingerId, FirstName, LastName, Age) VALUES (1, 'Marc', 'Richards', 30);
INSERT INTO Singers (SingerId, FirstName, LastName, Age) VALUES (2, 'Catalina', 'Smith', 25);
INSERT INTO Singers (SingerId, FirstName, LastName, Age) VALUES (3, 'Alice', 'Trentor', 28)

-- dml.sql --
UPDATE Singers SET LastName = 'Johnson' WHERE SingerId = 1

-- query.sql --
SELECT SingerId, FirstName, LastName FROM Singers WHERE SingerId = 1

-- expect.out --
(1, "Marc", "Johnson")

-- dml.sql --
UPDATE Singers SET FirstName = 'Bob', LastName = 'Williams' WHERE SingerId = 2

-- query.sql --
SELECT SingerId, FirstName, LastName FROM Singers WHERE SingerId = 2

-- expect.out --
(2, "Bob", "Williams")

-- dml.sql --
UPDATE Singers SET Age = Age + 1 WHERE Age >= 28

-- query.sql --
SELECT SingerId, Age FROM Singers WHERE Age >= 29 ORDER BY SingerId

-- expect.out --
(1, 31)
(3, 29)
