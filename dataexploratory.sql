-- data cleaning 
select *
from layoffs;

-- 1. remove duplicate
-- 2. standarize data
-- 3. null values or black values 
-- 4. remove any columns
 
 create table layoffs_staging
 like layoffs;
 
 select *
 from layoffs_staging;
 
 insert layoffs_staging
 select*
 from layoffs;
 
 select*,
 row_number() over(
 partition by company,industry,total_laid_off,percentage_laid_off,date) as row_num
 from layoffs_staging;
 
 with duplicate_cte as
 (
 select*,
 row_number() over(
 partition by company,location,industry,total_laid_off,percentage_laid_off,'date', stage,country, funds_raised_millions) as row_num
 from layoffs_staging
 )
 select*
 from duplicate_cte
 where row_num > 1; 
 
 select* 
 from layoffs_staging
 where company = 'casper';
 
 

create TABLE `layoffs_staging2` (
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

select*
from layoffs_staging2
 where row_num > 1;

insert into layoffs_staging2
select*,
 row_number() over(
 partition by company,location,industry,total_laid_off,percentage_laid_off,'date', stage,country, funds_raised_millions) as row_num
 from layoffs_staging;

delete
from layoffs_staging2
 where row_num > 1;
 select*
from layoffs_staging2
 where row_num > 1;
 
 -- standarising data
 
 select distinct country, trim(trailing '.' from country)
 from layoffs_staging2
 order by 1;
 
 update layoffs_staging2
 set country  = trim(trailing '.' from country)
 where country like 'united states%';
 
 select `date`,
 str_to_date(`date`, '%m/%d/%y')
 from layoffs_staging2;
 

select*
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select*
from layoffs_staging2
where industry is null
or industry = ''
;
 
select*
from layoffs_staging2
where company = 'airbnb';
 


select*
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select*
from layoffs_staging;
alter table layoffs_staging
drop column location; 


-- exploratory data analysis


 select*
from layoffs_staging2;

select MAX(total_laid_off),  MAX(percentage_laid_off)
from layoffs_staging2;

select*
from layoffs_staging2
where percentage_laid_off=1
order by funds_raised_millions desc;

 select company, sum(total_laid_off)
from layoffs_staging2
group by company 
order by 2 desc;

select min(`date`),max(`date`)
from layoffs_staging2;

 select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

select*
from layoffs_staging2;

select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

select company, avg(percentage_laid_off)
from layoffs_staging2
group by company 
order by 2 desc;

select substring(`date`,1,7) as `month`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc ;

with rolling_total as
(
select substring(`date`,1,7) as `month`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc
)
select `month`,total_off
, sum(total_off) over (order by `month`) as rolling_total
from rolling_total;

select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

 select company, sum(total_laid_off)
from layoffs_staging2
group by company 
order by 2 desc;

 select company,year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company , year(`date`)
order by 3 desc;

with company_year (company,years,total_laid_off) as 
(
 select company,year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company , year(`date`)
), company_year_rank as 
(select *,
dense_rank() over(partition by years order by total_laid_off desc)
from company_year
)
select *
from company_year_rank
where ranking <= 5
;














 
 
 
 
 
