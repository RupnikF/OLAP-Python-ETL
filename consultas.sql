
/*
 ¿Existe correlación entre el PBI per cápita de un país y su tasa de mortalidad?
 Por año y territorio, el PBI per cápita y la tasa de mortalidad
 */
SELECT pbi, deaths
FROM year_data;


/*
(CAMBIADA) Determinar si existe un cambio de mortalidad frente a diferentes estaciones. Para un mes, año y territorio, estación de cada país y la tasa de mortalidad
*/
SELECT avg(deaths),avg(infections),season
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
     SELECT year, monthnumber, region_name, country_name, policy_level_mensual, avg(infecciones_mensuales) OVER (PARTITION BY name ORDER BY year, monthnumber ROWS 2 PRECEDING) as avg_movible_3
     FROM infections_policy_por_mes
)
SELECT pm1.region_name, pm1.country_name, pm1.policy_level_mensual, pm1.avg_movible_3, pm2.avg_movible_3, (pm1.avg_movible_3 - pm2.avg_movible_3) as delta
FROM promedio_movil_3 pm1 LEFT JOIN promedio_movil_3 pm2 ON
    pm1.country_name = pm2.country_name
    and ((pm1.monthnumber -1  = pm2.monthnumber and pm1.year = pm2.year) or
    (pm1.monthnumber -12  = pm2.monthnumber and pm1.year -1 = pm2.year))
ORDER BY delta;


/*
Entre las políticas sanitarias utilizadas ¿Cuáles fueron las más efectivas frente a un aumento en los casos?
Cantidad de casos ( con lag de 3 meses) promedio agrupando por nivel de política
*/

WITH stringency_resumida AS (SELECT policy_level, deaths, infections, date, country.name AS country_name,
                                 CASE
                                     WHEN policy_level BETWEEN 1 AND 20 THEN 1
                                     WHEN policy_level BETWEEN 21 AND 40 THEN 2
                                     WHEN policy_level BETWEEN 41 AND 60 THEN 3
                                     WHEN policy_level BETWEEN 61 AND 80 THEN 4
                                     WHEN policy_level BETWEEN 81 AND 100 THEN 5
                                     ELSE 0 -- In case policy_level is outside the expected range
                                     END AS policy_group
                             FROM daily_data NATURAL JOIN time NATURAL JOIN country)
SELECT date, country_name, policy_level, deaths, LAG(deaths, 3) OVER (PARTITION BY policy_level ORDER BY date) AS change_in_deaths, infections, LAG(infections, 3) OVER (PARTITION BY policy_level ORDER BY date) AS change_in_infections
FROM stringency_resumida;


/*
Entre los países cercanos geográficamente,
¿Existe una variación en sus tasas de mortalidad?
Para las regiones (Sudamérica, América Central, Norteamérica, Europa del Oeste, Europa del Este, etc), mes y año, la tasa de mortalidad promedio.
*/
WITH avg_region as(
    SELECT avg(deaths) as avg_region, region.name as region_name
    FROM daily_data NATURAL JOIN country NATURAL JOIN region
    GROUP BY region.name
)
SELECT region_name, avg(deaths - avg_region) as delta
FROM avg_region NATURAL JOIN daily_data NATURAL JOIN country
GROUP BY region_name
ORDER BY delta;

/*
¿Existe una diferencia en la tasa de mortalidad entre países con regímenes obligatorios de vacunación y aquellos con regímenes opcionales?
Discretizar los tipos de regímenes, ponerle una clasificación
Para un país, clasificado en vacunación obligatoria y opcional, la tasa de mortalidad anual
*/
WITH stringency_resumida AS (
    SELECT deaths, infections,
    CASE
        WHEN policy_level BETWEEN 1 AND 20 THEN 1
        WHEN policy_level BETWEEN 21 AND 40 THEN 2
        WHEN policy_level BETWEEN 41 AND 60 THEN 3
        WHEN policy_level BETWEEN 61 AND 80 THEN 4
        WHEN policy_level BETWEEN 81 AND 100 THEN 5
        ELSE 0 -- In case policy_level is outside the expected range
        END AS policy_group
    FROM daily_data NATURAL JOIN time)
SELECT policy_group, avg(deaths) as avg_deaths, avg(infections) as avg_infections
FROM stringency_resumida
GROUP BY policy_group;


/*
¿Qué estrategias de vacunación vieron una mejora sustancial (en comparación con otras) después de 3 meses? ¿Y después de 6 meses? ¿Y año?
Mismo que antes ponerle una categorizar cada país
*/

