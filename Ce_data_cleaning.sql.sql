-- MYSQL Project - structured Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022
show tables;
select * from layoffs;
# data cleaning 
# 1. REMOVE DUPLICATEs  
# 2. Standardize the data 
# 3. remove null values or blank values
# 4. Remove Any Column 
-- Duplicate Removal 
-- #created a layoff_staging tables, that is the tables i used to start up the cleaning 
select * 
from layoff;

create table layoffs_staging 
like layoffs;

select * 
from layoffs_staging;

INSERT layoffs_staging
select * from layoffs;
-- i used this query to check for duplicates 
select * ,
ROW_NUMBER() OVER(
partition by company, LOCATION, industry, TOTAL_LAID_OFF, PERCENTAGE_LAID_OFF,
 `DATE`,Stage, country,funds_raised_millions) AS ROW_NUM
from layoffs_staging;

WITH DUPLICATE_CTE AS
(select * ,
ROW_NUMBER() OVER(
partition by company,location, industry, TOTAL_LAID_OFF, PERCENTAGE_LAID_OFF, `DATE`, 
stage, country, funds_raised_millions) AS ROW_NUM
from layoffs_staging
)
select * 
FROM DUPLICATE_CTE 
WHERE ROW_NUM > 1;

-- a recheck query 
select * 
from layoffs_staging
WHERE COMPANY = 'casper';
 -- A delete statemnt for the duplicate roles *(recreated the statement as a table to create a new column Row_num)
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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

 insert into layoffs_staging2
 select * ,
ROW_NUMBER() OVER(
partition by company,location, industry, TOTAL_LAID_OFF, PERCENTAGE_LAID_OFF, `DATE`, 
stage, country, funds_raised_millions) AS ROW_NUM
from layoffs_staging2;

WITH DELETE_CTE AS (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, 
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM layoffs_staging2
)
DELETE FROM layoffs_staging2
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
	FROM DELETE_CTE
) AND row_num > 1;

DELETE FROM layoffs_staging2
WHERE row_num >= 2;

-- # Standidized Query Format of the data 

SELECT * 
FROM layoffs_staging2;

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SELECT *
FROM .layoffs_staging2
WHERE company LIKE 'Bally%';
-- Nothing wrong here
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'airbnb%';

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What i did was I Wrote 
-- A query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands i wouldn't have to manually check them all

-- query statement to set the blanks to nulls since those are typically easier to work with
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- now if you check those are all null

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- now we need to populate those nulls if possible

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- and if you check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

#############################################################

-- I also noticed the Crypto has multiple different variations. i  standardize that - all to Crypto
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- now that's taken care of:
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;
##############################################
-- i aso looked at 

SELECT *
FROM layoffs_staging2;

-- everything looks good except apparently there are  some "United States" and some "United States." with a period at the end. Let's standardize this.
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- now if runned this again it is fixed
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

## query to fix column 

-- Let's also fix the date columns:
SELECT *
FROM layoffs_staging2;

--  use str to date query language to update this field
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- now execute this convert query language on the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM layoffs_staging2;


-- 3. Look at Null Values
 
-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values




-- 4. remove any columns and rows i used this column!

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- i cant trust such data so i deleted them using the delete statement 
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


SELECT * 
FROM layoffs_staging2;


