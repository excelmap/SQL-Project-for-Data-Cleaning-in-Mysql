---- Exploratory Data Analysis of world_layoffs ----

SELECT * 
FROM layoffs_stagging1;


SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_stagging1;

SELECT * 
FROM layoffs_stagging2
WHERE percentage_laid_off =1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY company 
ORDER BY 2 DESC;


SELECT MIN(`date`), MAX(`date`)
FROM layoffs_stagging2;

SELECT industry, SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY industry 
ORDER BY 2 DESC;

SELECT country, `date`, SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY country, `date`
ORDER BY `date`; 

SELECT  YEAR(`date`), SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC; 


SELECT  stage, SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY stage
ORDER BY 2 DESC; 

SELECT company, AVG(total_laid_off)
FROM layoffs_stagging2
GROUP BY company
ORDER BY 2 DESC;

SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_stagging2
WHERE  SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY MONTH
ORDER BY 1 ASC; 

WITH Rolling_Total AS 
(
SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_stagging2
WHERE  SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY MONTH
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off, 
	SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;


SELECT company,country,YEAR(`date`), SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY company,country, YEAR(`date`)
ORDER BY 3 DESC;

SELECT company,YEAR(`date`), SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;


WITH Company_Year (company, year, total_laid_off) AS 
(
SELECT company,YEAR(`date`), SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS 
(SELECT *, DENSE_RANK() OVER(PARTITION BY year ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year 
WHERE  year IS NOT NULL
)
SELECT * 
FROM Company_Year_Rank
WHERE Ranking <=5; 
 
