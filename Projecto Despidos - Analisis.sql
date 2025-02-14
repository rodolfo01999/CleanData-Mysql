-- Visualizar empresas con maximos despidos y porcentaje
select	MAX(total_laid_off) , max(percentage_laid_off)
from layoffs_staging2;
-- EMPRESAS CON UN PORCENTAJE DE DESPIDOS DEL 100%
select *
from layoffs_staging2
where percentage_laid_off = 1
order by 1 desc;


-- empresas que han logrado la mayor cantidad de despidos historicamente
select	company , MAX(total_laid_off)
from layoffs_staging2
group by 1
order by 2 desc;

-- industrias con mas despidos
select	industry , sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

-- paises con mayor despido
select	country , sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

-- fechas con mayor despido
select	date , sum(total_laid_off)
from layoffs_staging2
group by date
order by 2 desc;
-- por año 
select YEAR(date) , sum(total_laid_off)
from layoffs_staging2
group by YEAR(date)
order by 2 desc;

-- Meses con mayor tendencia de despido con rolling
 WITH Rolling_Total AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
-- no ponemos el alias dates ya que WHERE se ejecuta antes de SELECT, así que MySQL aún no sabe qué es MONTH.
where SUBSTRING(date,1,7) is not null
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, total_laid_off,SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM Rolling_Total
ORDER BY dates ASC;

WITH COMPANY_YEAR (Company , Years , total_laid_off ) as 
(
select	company , YEAR(`date`) AS AÑO , SUM(total_laid_off)
from layoffs_staging2
group by company , AÑO 
order by 3 DESC
),
company_year_ranking as 
(select * , 
DENSE_RANK() OVER (PARTITION BY Years ORDER BY total_laid_off DESC) AS RANKING
from COMPANY_YEAR
WHERE Years is not null
)
select * from company_year_ranking 
where ranking <= 5; 
