 /* @Author: Nagashree C R
    @Date: 17-08-2024
    @Last Modified by: Nagashree C R
    @Last Modified : 27-08-2024
    @Title :Solving CSV datafile Queries using SQL*/

use test;

select * from dbo.covid_19_clean_complete;
select * from dbo.worldometer_data;

use joins;
select * from dbo.covid_19_clean_complete;
select * from dbo.covid_19_india;
select * from dbo.covid_vaccine_statewise;
select * from dbo.day_wise;

-- P1. globally death percentage and country--
select Country,
	CASE
		WHEN SUM(CAST(Confirmed AS float)) > 0 THEN CAST((SUM(CAST(Deaths AS float)) * 100.0) / SUM(CAST(Confirmed AS float)) as decimal (10,2))
		else 0
	end as global_death_percentage
from dbo.covid_19_clean_complete
group by Country order by Country asc;


--P1. locally death percentage and state--
select Country,
	CASE
		WHEN SUM(CAST(Confirmed AS float)) > 0 THEN CAST((SUM(CAST(Deaths AS float)) * 100.0) / SUM(CAST(Confirmed AS float)) as decimal (10,2))
		else 0
	end as local_death_percentage
from dbo.covid_19_clean_complete
where Country = 'India'
group by Country order by Country asc;


--P2. globally infected percentage --
select [Country Region],
	CASE
        WHEN SUM(CAST(Population AS float)) > 0 THEN 
            CAST((SUM(CAST(TotalCases AS float)) * 100.0) / SUM(CAST(Population AS float)) AS decimal(10, 2))
        ELSE 0
	end as global_infection_rate
from dbo.worldometer_data
group by [Country Region] order by [Country Region] asc;



--P2. locally infected percentage--

select [Country Region],
	CASE
        WHEN SUM(CAST(Population AS float)) > 0 THEN 
            CAST((SUM(CAST(TotalCases AS float)) * 100.0) / SUM(CAST(Population AS float)) AS decimal(10, 2))
        ELSE 0
	end as local_infection_rate
from dbo.worldometer_data
where [Country Region] = 'India'
group by [Country Region] order by [Country Region] asc;


--P3. To find out the countries with the highest infection rates--
SELECT TOP 1 [Country Region],
    CASE
        WHEN SUM(CAST(Population AS float)) > 0 THEN 
            CAST((SUM(CAST(TotalCases AS float)) * 100.0) / SUM(CAST(Population AS float)) AS decimal(10, 2))
        ELSE 0
    END AS infection_rate
FROM dbo.worldometer_data
GROUP BY [Country Region]  ORDER BY infection_rate DESC;



--P4. To find out the countries and continents with the highest death counts--

select TOP 1 Country,
	CASE
		WHEN SUM(CAST(Confirmed AS float)) > 0 THEN CAST((SUM(CAST(Deaths AS float)) * 100.0) / SUM(CAST(Confirmed AS float)) as decimal (10,2))
		else 0
	end as global_death_rate
from dbo.covid_19_clean_complete
group by Country order by global_death_rate desc;


--P5. Average number of deaths by day (Continents and Countries)--
select Country, Date, AVG(CAST(Deaths AS INT)) as TotalDeath
from dbo.covid_19_clean_complete
group by Country,Date ;


--P6. Average of cases divided by the number of population of each country (TOP 10)--

SELECT TOP 10 [Country Region],
    CASE
        WHEN SUM(CAST(Population AS float)) > 0 THEN 
            CAST((SUM(CAST(TotalCases AS float)) * 100.0) / SUM(CAST(Population AS float)) AS decimal(10, 2))
        ELSE 0
    END AS infection_rate
FROM dbo.worldometer_data
GROUP BY [Country Region]  ORDER BY infection_rate DESC;


--P7. Considering the highest value of total cases, which countries have the highest rate of infection in relation to population?--
SELECT TOP 1 [Country Region],
    CASE
        WHEN SUM(CAST(Population AS float)) > 0 THEN 
            CAST((SUM(CAST(TotalCases AS float)) * 100.0) / SUM(CAST(Population AS float)) AS decimal(10, 2))
        ELSE 0
    END AS infection_rate
FROM dbo.worldometer_data
GROUP BY [Country Region]  ORDER BY infection_rate DESC;


/* Joining Problems */
--P8. To find out the population vs the number of people vaccinated--
select dbo.worldometer_data.[Country Region], dbo.worldometer_data.Population,dbo.covid_vaccine_statewise.[Updated On], dbo.covid_vaccine_statewise.[Total Doses Administered]
from dbo.worldometer_data
JOIN dbo.covid_vaccine_statewise on dbo.worldometer_data.[Country Region] = dbo.covid_vaccine_statewise.State;


--P9. To find out the percentage of different vaccine taken by people in a country--
select dbo.worldometer_data.[Country Region], dbo.worldometer_data.Population, dbo.covid_vaccine_statewise.[Updated On],
CASE 
	WHEN CAST(dbo.covid_vaccine_statewise.[Total Doses Administered] AS FLOAT) = 0 THEN NULL
        ELSE CAST((CAST(dbo.covid_vaccine_statewise.[CoviShield (Doses Administered)] AS FLOAT) * 100) / CAST(dbo.covid_vaccine_statewise.[Total Doses Administered] AS FLOAT) AS decimal(10, 2))
    END AS Covishield_rate,
CASE
	WHEN CAST(dbo.covid_vaccine_statewise.[Total Doses Administered] AS FLOAT) = 0 THEN NULL
        ELSE CAST((CAST(dbo.covid_vaccine_statewise.[ Covaxin (Doses Administered)] AS FLOAT) * 100) / CAST(dbo.covid_vaccine_statewise.[Total Doses Administered] AS FLOAT) AS decimal (10,2))
    END AS Covaxin_rate
from dbo.worldometer_data
JOIN dbo.covid_vaccine_statewise on dbo.worldometer_data.[Country Region] = dbo.covid_vaccine_statewise.State;


-- P10. To find out percentage of people who took both the doses --
select dbo.worldometer_data.[Country Region], dbo.worldometer_data.Population, dbo.covid_vaccine_statewise.[Updated On],
CASE 
	WHEN CAST(dbo.covid_vaccine_statewise.[Total Doses Administered] AS FLOAT) = 0 THEN NULL
        ELSE CAST((CAST(dbo.covid_vaccine_statewise.[Second Dose Administered] AS FLOAT) * 100) / CAST(dbo.covid_vaccine_statewise.[Total Doses Administered] AS FLOAT) AS decimal(10, 2))
    END AS both_dose_taken_rate
from dbo.worldometer_data
JOIN dbo.covid_vaccine_statewise on dbo.worldometer_data.[Country Region] = dbo.covid_vaccine_statewise.State;