select distinct city_name from forecasts;

SELECT * FROM (
	SELECT *, RANK() OVER (
		PARTITION BY YEAR(created), MONTH(created), DAY(created), city_name 
        ORDER BY created DESC) AS forecasts_rank
	FROM forecasts
) AS ranked_table
WHERE forecasts_rank = 1;
        
SELECT created, city_name, avg(the_temp) AS avg_temp FROM (
	SELECT *, RANK() OVER (
		PARTITION BY YEAR(created), MONTH(created), DAY(created), city_name 
        ORDER BY created DESC) AS forecasts_rank
	FROM forecasts
) AS ranked_table
WHERE forecasts_rank <= 3
GROUP BY YEAR(created), MONTH(created), DAY(created), city_name;

SELECT city_name, avg(min_temp), avg(max_temp), avg(the_temp), avg(wind_speed), avg(air_pressure), avg(humidity), avg(visibility), avg(predictability)
FROM forecasts
GROUP BY city_name;
