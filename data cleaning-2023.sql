SELECT * 
FROM layoffs_2023;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values
--  4. Remove Any Columns 

# first to make sure the mistakes or the columns does not effect the raw data that is used elsewhere we need to secure the data by creating the duplicate of the data 
# that can be done by

CREATE TABLE layoffs_stagging
LIKE layoffs_2023;

SELECT * 
FROM layoffs_stagging;


# these above commands will create a emnpty table as same as the layoffs tables without any data
#$ Now we need to add data to the layoffs_stagging table 

INSERT layoffs_stagging
SELECT *
FROM layoffs_2023;
# this will insert the layoffs table data into the layoff stagging table

SELECT * 
FROM layoffs_stagging;

# now we created a instance of the lay offs table which can be used without effecting the raw data.
# know we need to know are there any duplicates in the data set  with use of Cte

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num
FROM layoffs_stagging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

# DELETE
# FROM duplicate_cte
# WHERE row_num > 1;

#WITH duplicate_cte AS ( SELECT *, ROW_NUMBER() OVER( PARTITION BY company, location,  industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised) 
# AS row_num FROM layoffs_stagging ) DELETE FROM duplicate_cte WHERE row_num > 1	
#Error Code: 1288. The target table duplicate_cte of the DELETE is not updatable	0.000 sec

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num
FROM layoffs_stagging
)
SELECT *
FROM layoffs_stagging
WHERE industry IS NULL
OR industry = '';

# as we can not delete the duplicates from the stagging in the CTE we gp and create our next staging table to filter and delete by using row number

CREATE TABLE `layoffs_stagging2` (
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

#here we are adding the row_num as a extra row

SELECT * 
FROM layoffs_stagging2;

#Know Lets insert the stagging into stagging2

INSERT INTO layoffs_stagging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num
FROM layoffs_stagging;

SELECT *
FROM layoffs_stagging2;

SELECT * 
FROM layoffs_stagging2
WHERE row_num > 1;
# know we can delete the columns by using the row number
DELETE 
FROM layoffs_stagging2
WHERE row_num > 1;

SELECT * 
FROM layoffs_stagging2
WHERE row_num > 1;


# ---standardizing data ---

SELECT company, TRIM(company)
FROM layoffs_stagging2;

UPDATE layoffs_stagging2
SET company = TRIM(company);

SELECT  DISTINCT industry
FROM layoffs_stagging2
ORDER BY 1;

SELECT *
FROM layoffs_stagging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_stagging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_stagging2
ORDER BY 1;

UPDATE layoffs_stagging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT *
FROM layoffs_stagging2;

# for the time series analysis the date should be in date format 

SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_stagging2;

UPDATE layoffs_stagging2
SET  `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

SELECT *
FROM layoffs_stagging2;

# as the format was changed lets change the data type

ALTER TABLE layoffs_stagging2
MODIFY COLUMN `date` DATE;

# -- Null and Blank values

SELECT *
FROM layoffs_stagging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

SELECT *
FROM layoffs_stagging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_stagging2
WHERE company = 'Airbnb';

# to update the balmk andc null values first change the  balnk values to null values

UPDATE layoffs_stagging2
SET industry = NULL
WHERE industry = '';

SELECT t1.industry, t2.industry
FROM layoffs_stagging2 t1
JOIN layoffs_stagging2 t2
	ON t1.company = t2.company   
WHERE (t1.industry IS NULL or t1.industry = '')
AND t2.industry IS NOT NULL
;

# update the industry table to fill the balnk values with appropriate industry type by using the Self join 

UPDATE layoffs_stagging2 t1
JOIN layoffs_stagging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL
;

# -- removing the columns and rows

SELECT *
FROM layoffs_stagging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

# deleting the columns having null values in both total_laid_off and Percentage_laid_off
DELETE
FROM layoffs_stagging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

ALTER TABLE layoffs_stagging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_stagging2;