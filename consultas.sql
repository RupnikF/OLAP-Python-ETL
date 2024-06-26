
/*
 ¿Existe correlación entre el PBI per cápita de un país y su tasa de mortalidad por Covid?
 Por año y territorio, el PBI per cápita y la tasa de mortalidad
 */
WITH year_deaths_covid AS (
    SELECT country_key, year, sum(deaths) as covid_deaths
    FROM daily_data NATURAL JOIN time
    GROUP BY country_key, year
)
SELECT name, year ,pbi, (covid_deaths*100000/(population::numeric)) as mortality_rate
FROM year_data NATURAL JOIN year_deaths_covid NATURAL  JOIN country
ORDER BY pbi DESC;


/*
Determinar si existe un cambio de mortalidad frente a diferentes estaciones.
Para cada estación la tasa de mortalidad
*/
SELECT avg(deaths) as avg_deaths,avg(infections) as avg_infections,season
FROM daily_data
GROUP BY season;

/*
¿Existe una vinculación entre la edad promedio de una población y la tasa de mortalidad?
Por cada año y territorio, edad promedio de la población y la tasa de mortalidad

Se puede ver que a medida que aumenta la edad promedio aumenta la mortalidad
*/
SELECT name, median_age, sum(deaths) * 100000/(population::numeric) as mortality_rate
FROM daily_data NATURAL JOIN country
WHERE median_age is not null
GROUP BY name, median_age, population
ORDER BY median_age;


/*
¿Existe una correlación entre la tasa de mortalidad y la densidad de población?
Por pais, densidad poblacional y la tasa de mortalidad
*/
SELECT  country.name, population/area AS density, coalesce(sum(deaths)*100000/(population::numeric), 0) as mortality_rate
FROM daily_data NATURAL JOIN country
WHERE area is not null
GROUP BY  country.name, population, area
ORDER BY density DESC;

/*
 ¿Existe relación entre el presupuesto en sistemas de salud y la tasa de mortalidad?
 Por cada año y territorio, presupuesto anual para sistema de salud y tasa de mortalidad
*/
WITH year_deaths_covid AS (
    SELECT country_key, year, sum(deaths) as covid_deaths
    FROM daily_data NATURAL  JOIN  time
    GROUP BY country_key, year
)
SELECT year, name, health_budget, covid_deaths * 100000/(population::numeric) as mortality_rate
FROM year_deaths_covid NATURAL JOIN country NATURAL JOIN year_data
WHERE health_budget is not  null
ORDER BY  health_budget DESC;

/*
Identificar periodos de aumento de casos por región, acciones realizadas e infecciones después de 3 meses.
Ranking de aumento de casos e infecciones en base a promedio móvil de x semanas.

SUPONGO: “aumento en casos” : prom movil 3 meses - prom movil 3 meses
SUPONGO: policiy level de cada mes como el promedio del mes
*/

WITH
infections_policy_por_mes AS(
    SELECT year, monthnumber, region.name as region_name, country.name as country_name, avg(policy_level) as policy_level_mensual, sum(infections) as infecciones_mensuales
    FROM time NATURAL JOIN daily_data NATURAL JOIN country JOIN region on country.region_key = region.region_key
    GROUP BY region.name, year, monthnumber, country.name),
promedio_movil_3 AS (
     SELECT year, monthnumber, region_name, country_name, policy_level_mensual, avg(infecciones_mensuales) OVER (PARTITION BY country_name ORDER BY year, monthnumber ROWS 2 PRECEDING) as avg_movible_3
     FROM infections_policy_por_mes
)
SELECT pm1.region_name, pm1.country_name, pm1.policy_level_mensual as policy_level, pm1.avg_movible_3 as prom_mobil, pm2.avg_movible_3 as prom_movil_3meses, (pm1.avg_movible_3 - pm2.avg_movible_3) as delta
FROM promedio_movil_3 pm1 LEFT JOIN promedio_movil_3 pm2 ON
    pm1.country_name = pm2.country_name
    and ((pm1.monthnumber -1  = pm2.monthnumber and pm1.year = pm2.year) or
    (pm1.monthnumber -11  = pm2.monthnumber and pm1.year -1 = pm2.year))
WHERE pm1.policy_level_mensual is not null and pm1.avg_movible_3 is not null and pm2.avg_movible_3 is not null
ORDER BY policy_level DESC ;


/*
Entre las políticas sanitarias utilizadas ¿Cuáles fueron las más efectivas frente a un aumento en los casos?
Politica sanitaria actual y Cantidad de casos comparado con un lag de 20 dias
*/
WITH stringency_resumida AS (SELECT avg(policy_level) as avg_policy, sum(deaths) AS deaths, sum(infections) AS infections, monthnumber AS month, country.name AS country_name, year,
                                 CASE
                                     WHEN avg(policy_level) BETWEEN 1 AND 20 THEN 1
                                     WHEN avg(policy_level) BETWEEN 21 AND 40 THEN 2
                                     WHEN avg(policy_level) BETWEEN 41 AND 60 THEN 3
                                     WHEN avg(policy_level) BETWEEN 61 AND 80 THEN 4
                                     WHEN avg(policy_level) BETWEEN 81 AND 100 THEN 5
                                     ELSE 0
                                     END AS policy_group
                             FROM daily_data NATURAL JOIN time NATURAL JOIN country
                             GROUP BY year, monthnumber, country_name)
SELECT year, month, country_name, policy_group, LAG(policy_group, 3) OVER (PARTITION BY country_name ORDER BY year, month) as previous_policy, deaths, LAG(deaths, 3) OVER (PARTITION BY country_name ORDER BY year, month) AS change_in_deaths, infections, LAG(infections, 3) OVER (PARTITION BY country_name ORDER BY year, month ) AS change_in_infections
FROM stringency_resumida
WHERE  policy_group <> 0
ORDER BY policy_group desc;


/*
Entre los países cercanos geográficamente,
¿Existe una variación en sus tasas de mortalidad?
Para las regiones (Sudamérica, América Central, Norteamérica, Europa del Oeste, Europa del Este, etc), mes y año, la tasa de mortalidad promedio.

NOTAMOS QUE EN EUROPA TUVIERON MUCHA DIFERENCIA PERO EN EL RESTO NO
*/
WITH avg_region as(
    SELECT avg(deaths) as avg_region, region.name as region_name
    FROM daily_data NATURAL JOIN country JOIN region on country.region_key = region.region_key
    GROUP BY region.name),
avg_country as(
    SELECT avg(deaths) as avg_country,country_key as country ,region.name as region_name
    FROM daily_data NATURAL JOIN country JOIN region on country.region_key = region.region_key
    GROUP BY country_key,region_name)
SELECT r.region_name, avg(r.avg_region - c.avg_country) as delta
FROM avg_region r JOIN avg_country c  ON r.region_name = c.region_name
GROUP BY r.region_name
ORDER BY delta;


/*
¿Existe una diferencia en la tasa de mortalidad entre países con diferentes regimenes sanitarios?
Discretizar los tipos de regímenes, ponerle una clasificación. Para los regimenes de 1 a 5 , el promedio de muertes y casos

ESTA ESTA BUENA PARA HACER EL ANALISIS
*/
WITH country_stringency AS (
    SELECT country_key,
    CASE
        WHEN avg(policy_level) BETWEEN 1 AND 20 THEN 1
        WHEN avg(policy_level) BETWEEN 21 AND 40 THEN 2
        WHEN avg(policy_level) BETWEEN 41 AND 60 THEN 3
        WHEN avg(policy_level) BETWEEN 61 AND 80 THEN 4
        WHEN avg(policy_level) BETWEEN 81 AND 100 THEN 5
        ELSE 0 -- In case policy_level is outside the expected range
        END AS policy_group
    FROM daily_data
    GROUP BY country_key)
SELECT policy_group, avg(deaths) as avg_deaths, avg(infections) as avg_infections
FROM country_stringency NATURAL JOIN daily_data
GROUP BY policy_group;


/*
¿Qué estrategias de policy tuvieron mejor resultado después de 3 meses? ¿Y después de 6 meses? ¿Y año?
Mismo que antes ponerle una categorizar cada país
A 3 y a 6 MESES VEMOS UNA CAIDA DE LOS CASOS -> DESPUE SPONER BIEN
*/
WITH country_stringency_month AS (
    SELECT country_key, year, monthnumber, sum(infections) as infections,
           CASE
               WHEN avg(policy_level) BETWEEN 1 AND 20 THEN 1
               WHEN avg(policy_level) BETWEEN 21 AND 40 THEN 2
               WHEN avg(policy_level) BETWEEN 41 AND 60 THEN 3
               WHEN avg(policy_level) BETWEEN 61 AND 80 THEN 4
               WHEN avg(policy_level) BETWEEN 81 AND 100 THEN 5
               ELSE 0 -- In case policy_level is outside the expected range
               END AS policy_group
    FROM daily_data NATURAL JOIN time
    GROUP BY year, monthnumber, country_key),
compare_infections AS (
    SELECT country.name, year, policy_group, monthnumber, infections,
        LEAD(infections,3) OVER (PARTITION BY country.name ORDER BY year, monthnumber) as month_3,
        LEAD(infections,6) OVER (PARTITION BY country.name ORDER BY year, monthnumber) as month_6,
        LEAD(infections,12) OVER (PARTITION BY country.name ORDER BY year, monthnumber) as month_12
    FROM country_stringency_month NATURAL JOIN country)
SELECT policy_group,  sum(infections), sum((infections - month_3)) as delta_3, sum(infections - month_6) as delta_6, sum(infections - month_12) as delta_12
FROM compare_infections
GROUP BY policy_group
ORDER BY policy_group;



/*
¿Un aumento en el sistema de salud mejora las tasas de mortalidad e infección?
Por año y territorio, la tasa de mortalidad e infección per cápita y la diferencia de presupuesto en el sistema de salud(% del PBI) con el año anterior
*/
WITH year_deaths AS (
    SELECT year, country_key, sum(deaths) as deaths_year
    FROM daily_data NATURAL  JOIN time
    GROUP BY year, country_key
),
compare_years AS (
    SELECT d.year, d.country_key, deaths_year, LAG(deaths_year, 1) over (PARTITION BY d.country_key ORDER BY d.year) as deaths_year_prev, health_budget, LAG(health_budget, 1) over (PARTITION BY y.country_key ORDER BY y.year) as prev_health_budget
   FROM year_deaths d join year_data y on d.country_key = y.country_key and d.year= y.year
   WHERE health_budget is not null
)
SELECT year, country_key, (health_budget - prev_health_budget) as delta_budget, (deaths_year - deaths_year_prev) as delta_deaths
FROM compare_years
WHERE prev_health_budget is not  null and deaths_year_prev is not  null
ORDER BY delta_budget DESC ;


/*
¿Qué meses fueron los mejores en tasa de mortalidad por región?
Por mes del año y región, la tasa de mortalidad mensual

HACER ESTE ANALISIS DE QUE LOS MEJORES SON AL AL INICIO Y AL FINAL
*/
WITH monthly_deaths_region AS (
    SELECT sum(deaths) as monthly_deaths_by_region, region.name as region_name, monthnumber, year
    FROM daily_data natural join time natural join country join region on country.region_key = region.region_key
    GROUP BY monthnumber, year, region.name),
total_ranking AS (
    SELECT region_name,  rank() OVER (PARTITION BY region_name ORDER BY monthly_deaths_by_region) as rank, monthly_deaths_by_region, year, monthnumber
    FROM monthly_deaths_region
    ORDER BY region_name,rank
)
SELECT  *
FROM total_ranking
WHERE rank <= 5;



/*
¿Qué meses fueron los peores en tasa de mortalidad por región?
Por mes del año y región, la tasa de mortalidad mensual

EN CAMBIO LOS PEORES MESES SON EN EL MEDIO DE LA PANDEMIA
*/
WITH monthly_deaths_region AS (
    SELECT sum(deaths) as monthly_deaths_by_region, region.name as region_name, monthnumber, year
    FROM daily_data natural join time natural join country join region on country.region_key = region.region_key
    GROUP BY monthnumber, year, region.name),
     total_ranking AS (
         SELECT region_name,  rank() OVER (PARTITION BY region_name ORDER BY monthly_deaths_by_region DESC) as rank, monthly_deaths_by_region, year, monthnumber
         FROM monthly_deaths_region
         ORDER BY region_name,rank
     )
SELECT  *
FROM total_ranking
WHERE rank <= 5;



/*
¿Existe un aumento de casos para fechas festivas?
SUPONGO LAG DE 20 días
*/
WITH day_sum as(
    SELECT sum(infections) as infections, time_key,date
    FROM daily_data natural join time
    GROUP BY time_key,date
),
prev_20_data AS(
    SELECT date,infections, sum(infections) OVER (ORDER BY date ROWS BETWEEN CURRENT ROW AND 20 FOLLOWING) as post_20,
           sum(infections) OVER (ORDER BY date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) as pre_20
    FROM day_sum
)
SELECT date,post_20,pre_20, (post_20 - pre_20) as delta_20
FROM prev_20_data
WHERE post_20 is not null and infections is not null and
 (to_char(date, 'MM-DD') = '12-25' -- navidad
    OR TO_CHAR(date, 'MM-DD') = '01-01' -- año nuevo
);

/*
 Que politica restrictiva se uso mas?
conteo de la canitdad de paises que usaron en promedio un regimen
 */
WITH stringency_country AS (
    SELECT country_key,
           CASE
               WHEN avg(policy_level) BETWEEN 1 AND 20 THEN 1
               WHEN avg(policy_level) BETWEEN 21 AND 40 THEN 2
               WHEN avg(policy_level) BETWEEN 41 AND 60 THEN 3
               WHEN avg(policy_level) BETWEEN 61 AND 80 THEN 4
               WHEN avg(policy_level) BETWEEN 81 AND 100 THEN 5
               ELSE 0
               END AS policy_group
    FROM daily_data
    GROUP BY country_key)
SELECT policy_group, count(country_key)
FROM stringency_country
GROUP BY policy_group
ORDER BY policy_group;