CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT * 
FROM layoffs_staging;
# Change staging database

-- Checking duplicated data

SELECT *,  ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, `date` , stage, country, funds_raised_millions) row_num
FROM layoffs_staging;

WITH duplicate_cte AS 
(SELECT *,  ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, `date`,
 stage, country, funds_raised_millions) row_num
FROM layoffs_staging) 
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging
WHERE company = 'Cazoo';

SELECT *,  ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, `date` , stage, country, funds_raised_millions) row_num
FROM layoffs_staging;

WITH duplicate_cte AS 
(SELECT *,  ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, `date`,
 stage, country, funds_raised_millions) row_num
FROM layoffs_staging) 
DELETE
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT	
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *,  ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, `date` , stage, country, funds_raised_millions) row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;

-- Standardizing data

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry 
FROM layoffs_staging2 
ORDER BY 1;

SELECT * 
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry = 'Crypto%';

SELECT DISTINCT country 
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country  = TRIM(TRAILING '.' FROM country)
WHERE country = 'United States%';

UPDATE layoffs_staging2
SET `date`  = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2 
MODIFY COLUMN `date` DATE;

-- NULL VALUES

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL
OR (t1.industry AND t2.industry) IS NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = null 
WHERE industry = '';

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM layoffs_staging2