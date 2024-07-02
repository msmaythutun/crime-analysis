-- Create CrimeType_Dim table
CREATE TABLE CrimeType_Dim (
	IUCR VARCHAR(10) PRIMARY KEY,
    PrimaryType VARCHAR(255),
    Description VARCHAR(255),
    FBICode VARCHAR(10)
);

-- Create LocationDetails_Dim table
CREATE TABLE LocationDetails_Dim (
    Beat VARCHAR(10) PRIMARY KEY,
    District VARCHAR(10),
    Ward VARCHAR(10),
    CommunityArea VARCHAR(255),
	Location VARCHAR(255),
	LocationDescription VARCHAR(255)
);
ALTER TABLE LocationDetails_Dim ALTER COLUMN CommunityArea TYPE VARCHAR(255);

-- DROP TABLE ArrestDetails_Dim
-- -- Create ArrestDetails_Dim table
-- CREATE TABLE ArrestDetails_Dim (
--     CaseNumber VARCHAR(255) PRIMARY KEY,
--     ArrestDate VARCHAR(255), 
--     Race VARCHAR(255),
--     Charge1Statute VARCHAR(255),
--     Charge1Description VARCHAR(255),
--     Charge1Type VARCHAR(255),
--     Charge1Class VARCHAR(255),
--     Charge2Statute VARCHAR(255),
--     Charge2Description VARCHAR(255),
--     Charge2Type VARCHAR(255),
--     Charge2Class VARCHAR(255),
--     Charge3Statute VARCHAR(255),
--     Charge3Description VARCHAR(255),
--     Charge3Type VARCHAR(255),
--     Charge3Class VARCHAR(255),
--     Charge4Statute VARCHAR(255),
--     Charge4Description VARCHAR(255),
--     Charge4Type VARCHAR(255),
--     Charge4Class VARCHAR(255),
--     ChargesStatute VARCHAR(255),
--     ChargesDescription VARCHAR(255),
--     ChargesType VARCHAR(255),
--     ChargesClass VARCHAR(255)
-- );                                                       

-- ALTER TABLE ArrestDetails_Dim
-- ALTER COLUMN ArrestDate
-- TYPE timestamp without Time Zone
-- USING TO_TIMESTAMP(ArrestDate, 'YYYY-MM-DD HH24:MI:SS') At Time Zone 'UTC';

DROP TABLE Time_Dim;
CREATE TABLE Time_Dim (
    DateId TEXT PRIMARY KEY,
    Year INT,
    Month INT,
    Day INT,
    Hour INT,
    Minute INT,
    Second INT,
	Date TIMESTAMP
);

ALTER TABLE Time_Dim
ALTER COLUMN Date
TYPE timestamp without Time Zone
USING TO_TIMESTAMP(DateId, 'YYYY-MM-DD HH24:MI:SS') At Time Zone 'UTC';


DELETE FROM time_dim
WHERE Date IN (
    SELECT Date
    FROM time_dim
    GROUP BY Date
    HAVING COUNT(*) > 1
);

SET max_parallel_workers = 0;
ALTER TABLE Time_Dim
ALTER COLUMN Date
TYPE timestamp without Time Zone
USING TO_TIMESTAMP(Date, 'YYYY-MM-DD HH24:MI:SS') At Time Zone 'UTC';

DROP TABLE Incident_Fact;
ALTER TABLE Incident_Fact ALTER COLUMN Date TYPE VARCHAR(255);

DELETE FROM incident_fact
WHERE casenumber NOT IN (SELECT casenumber FROM arrestdetails_dim);

DELETE FROM time_dim
WHERE NOT EXISTS (
    SELECT 1
    FROM incident_fact
    WHERE incident_fact.Date = time_dim.Date
);

-- DELETE FROM incident_fact
-- WHERE Date NOT IN (SELECT Date FROM Time_dim);


-- Create Incident_Fact table
CREATE TABLE Incident_Fact (
    ID INT PRIMARY KEY,
    CaseNumber VARCHAR(255),
    DateId TEXT,
    Block VARCHAR(255),
    IUCR VARCHAR(10),
    Arrest Boolean,
    Domestic Boolean,
    Beat VARCHAR(10),
	CONSTRAINT FK_CrimeType
	FOREIGN KEY (IUCR)
	REFERENCES CrimeType_DIM(IUCR),
	CONSTRAINT FK_LocationDetails
	FOREIGN KEY (Beat)
	REFERENCES LocationDetails_DIM(Beat),
	CONSTRAINT FK_Time
	FOREIGN KEY (DateId)
	REFERENCES Time_DIM(DateId)
);

-- add CrimeCount to incident_fact table
ALTER TABLE Incident_Fact
ADD COLUMN CrimeCount INT; 

-- update CrimeCount values
UPDATE Incident_Fact
SET CrimeCount = subquery.CrimeCount
FROM (
    SELECT
        i.ID,
        COUNT(*) AS CrimeCount
    FROM
        Incident_Fact i
    JOIN
        CrimeType_Dim ct ON i.IUCR = ct.IUCR
    JOIN
        LocationDetails_Dim ld ON i.Beat = ld.Beat
    JOIN
        Time_Dim t ON i.DateId = t.DateId
    GROUP BY
        i.ID
) AS subquery
WHERE
    Incident_Fact.ID = subquery.ID;
	
-- Queries 
-- crime count and arrest count by year
SELECT
    t.Year,
    SUM(inc.CrimeCount) AS CrimeCount,
    SUM(CAST(inc.Arrest AS INT)) AS ArrestCount
FROM
    Incident_Fact inc
JOIN
    Time_Dim t ON inc.DateId = t.DateId
GROUP BY
    t.Year;

-- crime count by top 20 primary types
SELECT
	ct.PrimaryType AS CrimeType,
	SUM(inc.CrimeCount) AS CrimeCount
FROM
	Incident_Fact inc
JOIN
	CrimeType_Dim ct ON inc.IUCR = ct.IUCR
GROUP BY
	ct.PrimaryType
ORDER BY
	CrimeCount DESC
LIMIT 20;
	
-- crime count by location
SELECT
    ld.LocationDescription AS CrimeLocation,
    SUM(inc.CrimeCount) AS CrimeCount
FROM
    Incident_Fact inc
JOIN
    LocationDetails_Dim ld ON inc.Beat = ld.Beat
GROUP BY
    ld.LocationDescription
ORDER BY
    CrimeCount DESC
LIMIT 20;

-- crime location for Sexual Assault 
SELECT
    ld.LocationDescription AS CrimeLocationAssault,
    SUM(inc.CrimeCount) AS CrimeCount
FROM
    Incident_Fact inc
JOIN
    LocationDetails_Dim ld ON inc.Beat = ld.Beat
JOIN
    CrimeType_Dim ct ON inc.IUCR = ct.IUCR
WHERE
    ct.PrimaryType = 'CRIMINAL SEXUAL ASSAULT'
GROUP BY
    ld.LocationDescription
ORDER BY
    CrimeCount DESC
LIMIT 20;

-- crime count and arrest count by year
SELECT
    t.Year,
    COUNT(*) AS CrimeCount,
    SUM(CAST(inc.Arrest AS INT)) AS ArrestCount
FROM
    Incident_Fact inc
JOIN
    Time_Dim t ON inc.DateId = t.DateId
GROUP BY
    t.Year;

-- peak crime hour of the day
SELECT
    Hour,
    SUM(inc.CrimeCount) AS CrimeCount
FROM
    Time_Dim t
JOIN
    Incident_Fact inc ON t.DateId = inc.DateId
GROUP BY
    Hour
ORDER BY
    CrimeCount DESC
LIMIT 10; 

select count(*) from incident_fact;

-- crime seasonality 
SELECT
    t.Year,
    t.Month,
    SUM(inc.CrimeCount) AS CrimeCount
FROM
    Time_Dim t
JOIN
    Incident_Fact inc ON t.DateId = inc.DateId
GROUP BY
    t.Year, t.Month
ORDER BY
    t.Year, t.Month;
