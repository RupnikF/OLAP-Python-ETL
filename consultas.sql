
/*
 ¿Existe correlación entre el PBI per cápita de un país y su tasa de mortalidad por Covid?
 Por año y territorio, el PBI per cápita y la tasa de mortalidad
 */
WITH year_deaths_covid AS (
    SELECT country_key, year, sum(deaths) as covid_deaths
    FROM daily_data NATURAL JOIN time
    GROUP BY country_key, year
)
SELECT pbi, deaths, covid_deaths
FROM year_data NATURAL JOIN year_deaths_covid
ORDER BY  pbi;


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
*/
WITH Casos_anuales as (
    SELECT sum(deaths) as muertes_anuales, year
    FROM daily_data NATURAL JOIN time
    GROUP BY year)
SELECT year, name, median_age, muertes_anuales
FROM Casos_anuales NATURAL JOIN country
ORDER BY muertes_anuales;



/*
¿Existe una correlación entre la tasa de mortalidad y la densidad de población?
Por cada año y territorio, densidad poblacional y la tasa de mortalidad
*/
WITH Casos_anuales as (
    SELECT sum(deaths) as annual_deaths, year
    FROM daily_data NATURAL JOIN time
    GROUP BY year)
SELECT population/area AS density, annual_deaths, year, country.name
FROM Casos_anuales NATURAL JOIN country NATURAL JOIN year_data
GROUP BY year, country.name, population, area, annual_deaths;

/*
 ¿Existe relación entre el presupuesto en sistemas de salud y la tasa de mortalidad? Por cada año y territorio, presupuesto anual para sistema de salud y tasa de mortalidad
*/

SELECT health_budget, d.deaths, policy_level, year, region.name
FROM daily_data d NATURAL JOIN country NATURAL JOIN region NATURAL JOIN year_data;

/*
Identificar periodos de aumento de casos por región, acciones realizadas e infecciones después de 3 meses.
Ranking de aumento de casos e infecciones en base a promedio móvil de x semanas.

SUPONGO: “aumento en casos” : prom movil 3 meses - prom movil 3 meses
SUPONGO: policiy level de cada mes como el promedio del mes
*/
WITH
infections_policy_por_mes AS(
    SELECT year, monthnumber, region.name as region_name, country.name as country_name, avg(policy_level) as policy_level_mensual, sum(infections) as infecciones_mensuales
    FROM time NATURAL JOIN daily_data NATURAL JOIN country NATURAL JOIN region
    GROUP BY region.name, year, monthnumber, country.name),
promedio_movil_3 AS (
     SELECT year, monthnumber, region_name, country_name, policy_level_mensual, avg(infecciones_mensuales) OVER (PARTITION BY country_name ORDER BY year, monthnumber ROWS 2 PRECEDING) as avg_movible_3
     FROM infections_policy_por_mes
)
SELECT pm1.region_name, pm1.country_name, pm1.policy_level_mensual, pm1.avg_movible_3, pm2.avg_movible_3, (pm1.avg_movible_3 - pm2.avg_movible_3) as delta
FROM promedio_movil_3 pm1 LEFT JOIN promedio_movil_3 pm2 ON
    pm1.country_name = pm2.country_name
    and ((pm1.monthnumber -1  = pm2.monthnumber and pm1.year = pm2.year) or
    (pm1.monthnumber -11  = pm2.monthnumber and pm1.year -1 = pm2.year))
ORDER BY delta;


/*
Entre las políticas sanitarias utilizadas ¿Cuáles fueron las más efectivas frente a un aumento en los casos?
Cantidad de casos ( con lag de 3 meses) promedio agrupando por nivel de política
*/

WITH stringency_resumida AS (SELECT policy_level, deaths, infections, date, country.name AS country_name,
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
¿Existe una diferencia en la tasa de mortalidad entre países con regímenes obligatorios de vacunación y aquellos con regímenes opcionales?
Discretizar los tipos de regímenes, ponerle una clasificación
Para un país, clasificado en vacunación obligatoria y opcional, la tasa de mortalidad anual
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
¿Qué estrategias de vacunación vieron una mejora sustancial (en comparación con otras) después de 3 meses? ¿Y después de 6 meses? ¿Y año?
Mismo que antes ponerle una categorizar cada país
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
    SELECT country.name, year, monthnumber, infections,
        LAG(infections,3) OVER (PARTITION BY country.name ORDER BY year, monthnumber) as month_3,
        LAG(infections,6) OVER (PARTITION BY country.name ORDER BY year, monthnumber) as month_6,
        LAG(infections,12) OVER (PARTITION BY country.name ORDER BY year, monthnumber) as month_12
    FROM country_stringency_month NATURAL JOIN country)
SELECT name, year, monthnumber, infections, (infections - month_3) as delta_3, (infections - month_6) as delta_6, (infections - month_12) as delta_12
FROM compare_infections
GROUP BY policy_group
ORDER BY policy_group;



/*
¿Un aumento en el sistema de salud mejora las tasas de mortalidad e infección? Por año y territorio,
la tasa de mortalidad e infección per cápita y la diferencia de presupuesto en el sistema de salud(% del PBI) con el año anterior
*/

SELECT deaths, health_budget, country.name as country_name, LAG(deaths,1) OVER (PARTITION BY country.name ORDER BY year) as prev_year_deaths, year
FROM year_data NATURAL JOIN country
ORDER BY country, year;


/*
¿Qué meses fueron los peores en tasa de mortalidad por región? Por mes del año y región, la tasa de mortalidad mensual
*/
WITH monthly_deaths AS (SELECT sum(deaths) as monthly_deaths_by_country, region, country.name, monthnumber, year
                        FROM daily_data natural join time natural join country natural join region
                        GROUP BY monthnumber, country.name, year),
    almost_there AS (SELECT AVG(monthly_deaths_by_country) as deaths, region, monthnumber as month, year
                     FROM monthly_deaths
                     GROUP BY region,monthnumber, year
                     ORDER BY deaths)
SELECT *, rank() OVER (PARTITION BY region ORDER BY deaths)
FROM almost_there;


/*
¿Frente a qué temperaturas se registró un aumento en los casos?
*/

SELECT season, sum(infections) as total_casos, country.name
FROM daily_data NATURAL JOIN country
GROUP BY season, country.name
ORDER BY country.name, total_casos;


/*
¿Existe un aumento de casos para fechas festivas?
SUPONGO LAG DE 20 días EXCELENTE
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