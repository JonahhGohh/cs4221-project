DROP TABLE IF EXISTS "nonrepeatable_read_accounts";
CREATE TABLE "nonrepeatable_read_accounts" (
    "id" INT PRIMARY KEY,
    "balance" INT NOT NULL DEFAULT 0
);