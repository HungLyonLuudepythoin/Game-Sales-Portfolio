-- Cleaning data
SELECT *, row_number() 
OVER(PARTITION BY `Rank`, `Name`, `Platform`, 'Year', 'Genre', "Publisher", 'NA_Sales', 
'EU_Sales', "JP_Sales", 'Other_Sales', 'Global_Sales')
FROM vgsales;

WITH Cte AS
(SELECT *, row_number() 
OVER(PARTITION BY `Rank`, `Name`, `Platform`, 'Year', 'Genre', "Publisher", 'NA_Sales', 
'EU_Sales', "JP_Sales", 'Other_Sales', 'Global_Sales') row_num
FROM vgsales
) SELECT * 
FROM Cte
WHERE row_num > 1;

# No duplicated value

-- Standardizing data
SELECT * FROM 
vgsales
WHERE `Name` LIKE 'Pok_mon%';

UPDATE vgsales
SET `Name` = 'Pokemon%'
WHERE `Name` LIKE 'Pok__mon%';

SELECT *
FROM vgsales;

-- Data Exploration

-- Analyze the breakdown of sales by regions (NA, EU, JP, Other) and globally

SELECT SUM(NA_Sales) AS Total_NA_Sales, 
    SUM(EU_Sales) AS Total_EU_Sales,
    SUM(JP_Sales) AS Total_JP_Sales,
    SUM(Other_Sales) AS Total_Other_Sales
FROM vgsales;

-- Identify which platforms have the highest total sales and how each region contributes to those sales

SELECT platform, SUM(NA_Sales) AS Total_NA_Sales, 
    SUM(EU_Sales) AS Total_EU_Sales,
    SUM(JP_Sales) AS Total_JP_Sales,
    SUM(Other_Sales) AS Total_Other_Sales
FROM vgsales
GROUP BY platform
ORDER BY 2 DESC;


WITH duplicate_cte AS 
(SELECT *, ROW_NUMBER() OVER( PARTITION BY platform, year, Total_NA_Sales, Total_EU_Sales, Total_JP_Sales, Total_Other_Sales) row_num
FROM (SELECT 
	Publisher
    platform, 
    year, 
    SUM(NA_Sales) OVER(PARTITION BY platform, publisher ORDER BY year) AS Total_NA_Sales, 
    SUM(EU_Sales) OVER(PARTITION BY platform, publisher ORDER BY year) AS Total_EU_Sales, 
    SUM(JP_Sales) OVER(PARTITION BY platform, publisher ORDER BY year) AS Total_JP_Sales, 
    SUM(Other_Sales) OVER(PARTITION BY platform, publisher ORDER BY year) AS Total_Other_Sales
FROM 
    vgsales
) platform_each_year)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE `platform_each_year` (
  `Platform` text,
  `Year` int DEFAULT NULL,
  `NA_Sales` double DEFAULT NULL,
  `EU_Sales` double DEFAULT NULL,
  `JP_Sales` double DEFAULT NULL,
  `Other_Sales` double DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO platform_each_year 
WITH duplicate_cte AS 
(SELECT *, ROW_NUMBER() OVER( PARTITION BY platform, year, Total_NA_Sales, Total_EU_Sales, Total_JP_Sales, Total_Other_Sales) row_num
FROM (SELECT 
	Publisher
    platform, 
    year, 
    SUM(NA_Sales) OVER(PARTITION BY platform, publisher ORDER BY year) AS Total_NA_Sales, 
    SUM(EU_Sales) OVER(PARTITION BY platform, publisher ORDER BY year) AS Total_EU_Sales, 
    SUM(JP_Sales) OVER(PARTITION BY platform, publisher ORDER BY year) AS Total_JP_Sales, 
    SUM(Other_Sales) OVER(PARTITION BY platform, publisher ORDER BY year) AS Total_Other_Sales
FROM 
    vgsales
) platform_each_year)
SELECT *
FROM duplicate_cte;


DELETE
FROM platform_each_year
WHERE row_num > 1;

ALTER TABLE platform_each_year
DROP COLUMN row_num;

SELECT * 
FROM platform_each_year;


-- Investigate how sales have evolved over the years. Do older or newer games perform better globally or regionally

SELECT `Year`, SUM(NA_Sales) AS Total_NA_Sales, 
    SUM(EU_Sales) AS Total_EU_Sales,
    SUM(JP_Sales) AS Total_JP_Sales,
    SUM(Other_Sales) AS Total_Other_Sales
FROM vgsales
GROUP BY `Year`
ORDER BY 1;

-- See which genres perform better overall and by region.
SELECT genre, SUM(NA_Sales) AS Total_NA_Sales, 
    SUM(EU_Sales) AS Total_EU_Sales,
    SUM(JP_Sales) AS Total_JP_Sales,
    SUM(Other_Sales) AS Total_Other_Sales
FROM vgsales
GROUP BY genre;

SELECT * 
FROM vgsales;

SELECT genre, Platform, SUM(Global_Sales)
FROM vgsales
GROUP BY genre, platform
ORDER BY 3 DESC;

-- Analyze which publishers dominate sales in different regions.

SELECT publisher, SUM(NA_Sales) AS Total_NA_Sales, 
    SUM(EU_Sales) AS Total_EU_Sales,
    SUM(JP_Sales) AS Total_JP_Sales,
    SUM(Other_Sales) AS Total_Other_Sales
FROM vgsales
GROUP BY publisher;

-- Analyze which publishers dominate sales in different regions in period time given.

SELECT 
    publisher, 
    `year`, 
    SUM(NA_Sales) OVER(PARTITION BY publisher ORDER BY `year`) AS Total_NA_Sales, 
    SUM(EU_Sales) OVER(PARTITION BY publisher ORDER BY `year`) AS Total_EU_Sales, 
    SUM(JP_Sales) OVER(PARTITION BY publisher ORDER BY `year`) AS Total_JP_Sales, 
    SUM(Other_Sales) OVER(PARTITION BY publisher ORDER BY `year`) AS Total_Other_Sales
FROM 
    vgsales;

SELECT * 
FROM vgsales
WHERE publisher = 'Infogrames' AND `year` = 2002;

WITH publisher_each_year AS (
    SELECT 
        publisher, 
        `year`, 
        SUM(NA_Sales) OVER(PARTITION BY publisher ORDER BY `year`) AS Total_NA_Sales, 
        SUM(EU_Sales) OVER(PARTITION BY publisher ORDER BY `year`) AS Total_EU_Sales, 
        SUM(JP_Sales) OVER(PARTITION BY publisher ORDER BY `year`) AS Total_JP_Sales, 
        SUM(Other_Sales) OVER(PARTITION BY publisher ORDER BY `year`) AS Total_Other_Sales
    FROM vgsales
),
duplicate_cte AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY publisher, `year` ORDER BY Total_NA_Sales, Total_EU_Sales, Total_JP_Sales, Total_Other_Sales) AS row_num
    FROM publisher_each_year
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

WITH duplicate_cte AS 
(SELECT *, ROW_NUMBER() OVER( PARTITION BY publisher, year, Total_NA_Sales, Total_EU_Sales, Total_JP_Sales, Total_Other_Sales) row_num
FROM (SELECT 
    publisher, 
    year, 
    SUM(NA_Sales) OVER(PARTITION BY publisher ORDER BY year) AS Total_NA_Sales, 
    SUM(EU_Sales) OVER(PARTITION BY publisher ORDER BY year) AS Total_EU_Sales, 
    SUM(JP_Sales) OVER(PARTITION BY publisher ORDER BY year) AS Total_JP_Sales, 
    SUM(Other_Sales) OVER(PARTITION BY publisher ORDER BY year) AS Total_Other_Sales
FROM 
    vgsales
) publisher_each_year)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE `publisher_each_year` (
  `Publisher` text,
  `Year` int DEFAULT NULL,
  `NA_Sales` double DEFAULT NULL,
  `EU_Sales` double DEFAULT NULL,
  `JP_Sales` double DEFAULT NULL,
  `Other_Sales` double DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO publisher_each_year 
WITH duplicate_cte AS 
(SELECT *, ROW_NUMBER() OVER( PARTITION BY publisher, year, Total_NA_Sales, Total_EU_Sales, Total_JP_Sales, Total_Other_Sales) row_num
FROM (SELECT 
    publisher, 
    `Year`, 
    SUM(NA_Sales) OVER(PARTITION BY publisher ORDER BY year) AS Total_NA_Sales, 
    SUM(EU_Sales) OVER(PARTITION BY publisher ORDER BY year) AS Total_EU_Sales, 
    SUM(JP_Sales) OVER(PARTITION BY publisher ORDER BY year) AS Total_JP_Sales, 
    SUM(Other_Sales) OVER(PARTITION BY publisher ORDER BY year) AS Total_Other_Sales
FROM 
    vgsales
) publisher_each_year)
SELECT *
FROM duplicate_cte;

DELETE
FROM publisher_each_year
WHERE row_num > 1;

ALTER TABLE publisher_each_year
DROP COLUMN row_num;

SELECT * 
FROM publisher_each_year;


