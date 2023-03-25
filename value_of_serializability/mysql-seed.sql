CREATE PROCEDURE seedDB()
BEGIN
    DECLARE i int DEFAULT 1;
    WHILE i <= 1000000 DO
        INSERT INTO serializability_1 (a, b) VALUES (i, i);
        SET i = i + 1;
    END WHILE;
END
