CREATE TABLE layoffs_stagging 	
LIKE layoffs;

SELECT * FROM layoffs_stagging;

INSERT layoffs_stagging
SELECT *
FROM layoffs;

--- To add row number in column ---
SELECT *,
    ROW_NUMBER () OVER(
    PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM
    layoffs_stagging;
    
    

--- create CTE for duplicates ---
WITH duplicate_cte AS
(
SELECT *,
    ROW_NUMBER () OVER(
    PARTITION BY company,location,  industry, total_laid_off, percentage_laid_off,`date`,stage,country, funds_raised_millions) AS row_num
FROM
    layoffs_stagging
    )
SELECT * 
FROM duplicate_cte 
WHERE row_num > 1;


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
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_stagging2
WHERE row_num > 1;

INSERT INTO layoffs_stagging2
SELECT *,
    ROW_NUMBER () OVER(
    PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM
    layoffs_stagging;

DELETE
 FROM layoffs_stagging2
WHERE row_num > 1;

SELECT * FROM layoffs_stagging2;

----- Standardizing data -----
--- It means finding issue in data and fixing it which is called standardizing data ---

SELECT company, TRIM(company)
FROM layoffs_stagging2;

UPDATE layoffs_stagging2
SET company = TRIM(company);

SELECT *
FROM layoffs_stagging2
WHERE industry LIKE 'Crypto%';

SELECT distinct industry
FROM layoffs_stagging2;

UPDATE layoffs_stagging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT*
FROM layoffs_stagging2
WHERE country LIKE 'United States%'
ORDER BY 1;



SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_stagging2 
ORDER BY 1;

UPDATE layoffs_stagging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_stagging2;

UPDATE layoffs_stagging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

select *
from layoffs_stagging2;

ALTER TABLE layoffs_stagging2
MODIFY COLUMN `date` DATE; 

select *
from layoffs_stagging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

select *
from layoffs_stagging2
WHERE industry is NULL 
OR industry = '';

SELECT * 
FROM layoffs_stagging2
WHERE company LIKE 'Bally%';




SELECT t1.industry, t2.industry
FROM layoffs_stagging2 AS t1
JOIN layoffs_stagging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
    WHERE (t1.industry IS NULL OR t1.industry = '')
    ANd t2.industry IS NOT NULL;

UPDATE layoffs_stagging2
SET industry = null 
WHERE industry = '';


UPDATE layoffs_stagging2 t1
JOIN layoffs_stagging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
    ANd t2.industry IS NOT NULL;

SELECT * 
FROM layoffs_stagging2;


SELECT * 
FROM layoffs_stagging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_stagging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


ALTER TABLE layoffs_stagging2
DROP COLUMN row_num;