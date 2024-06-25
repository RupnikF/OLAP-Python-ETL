import psycopg2, csv
from config import load_config
from datetime import date, datetime

# Continents primarily in the Northern Hemisphere
northern_hemisphere_continents = ["North America", "Europe", "Asia", "Africa"]

# Continents primarily in the Southern Hemisphere
southern_hemisphere_continents = ["Australia", "Antarctica", "South America", "Oceania"]

insertRegion = """INSERT INTO region(name)
             VALUES(%s) RETURNING region_key;"""

insertCountry = """INSERT INTO country(country_key, name, population,area,median_age, region_key)
             VALUES(%s,%s,%s,%s,%s,%s) RETURNING country_key;"""

insertTime = """INSERT INTO time(time_key, year, monthnumber, monthname, day, date)
             VALUES(%s,%s,%s,%s,%s,TO_DATE(%s, 'YYYY-MM-DD')) RETURNING time_key;"""

insertDaily = """INSERT INTO daily_data(deaths, infections, vaccinations, season, policy_level, time_key, country_key)
             VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING daily_data_key;"""


# Function to determine the season, it needs a
def getSeason(continent, date_str):
    input_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    if continent in northern_hemisphere_continents:
        # Northern Hemisphere seasons
        if date(input_date.year, 3, 21) <= input_date < date(input_date.year, 6, 21):
            return "Spring"
        elif date(input_date.year, 6, 21) <= input_date < date(input_date.year, 9, 23):
            return "Summer"
        elif date(input_date.year, 9, 23) <= input_date < date(input_date.year, 12, 21):
            return "Autumn"
        else:
            return "Winter"
    elif continent in southern_hemisphere_continents:
        # Southern Hemisphere seasons
        if date(input_date.year, 3, 21) <= input_date < date(input_date.year, 6, 21):
            return "Autumn"
        elif date(input_date.year, 6, 21) <= input_date < date(input_date.year, 9, 23):
            return "Winter"
        elif date(input_date.year, 9, 23) <= input_date < date(input_date.year, 12, 21):
            return "Spring"
        else:
            return "Summer"
    else:
        raise ValueError("Continent not recognized or not supported")


# Leer la continent(region) y agregarla si no esta
# Leer country y si no esta agregarlo (calcular area como population / density), asociar con continent key
# Leer date y agregarlo si no esta
# Leer los daily data, asociarlo con country y date, calcular season y guardarlo
def daily_etl(config):
    """ Connect to the PostgreSQL database server """

    # connecting to the PostgreSQL server
    with open('owid-covid-data.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        read_continents = []
        read_countries = []
        dates = []
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                for row in reader:
                    country_iso = row['iso_code']
                    if len(country_iso) > 3:
                        continue
                    continent_name = row['continent']
                    continent_key = 0
                    for continent in read_continents:
                        # Buscamos el continent
                        if continent['name'] == continent_name:
                            continent_key = continent['key']
                    if continent_key == 0:
                        # Agregamos el continent
                        cur.execute(insertRegion, (continent_name,))
                        continent_key = cur.fetchone()[0]
                        read_continents.append({'name': continent_name, 'key': continent_key})
                    country_found = False
                    for country in read_countries:
                        if country == country_iso:
                            country_found = True
                    if not country_found:
                        country_name = row['location']
                        country_pop = float(row['population'])
                        country_density = row['population_density']
                        num_area = None
                        if country_density != '':
                            num_area = country_pop / float(country_density)
                        median_age = row['median_age']
                        num_age = None
                        if median_age != '':
                            num_age = float(median_age)
                        cur.execute(insertCountry, (country_iso, country_name, country_pop, num_area, num_age,
                                                    continent_key))
                        read_countries.append(country_iso)
                    daily_date = row['date']
                    date_found = False
                    for date_ in dates:
                        if date_ == daily_date:
                            date_found = True
                    if not date_found:
                        full_date = datetime.strptime(daily_date, "%Y-%m-%d").date()
                        year = full_date.year
                        monthnumber = full_date.month
                        monthname = full_date.strftime("%B")
                        day = full_date.day
                        cur.execute(insertTime, (daily_date, year, monthnumber, monthname, day, daily_date))
                        dates.append(daily_date)
                    deaths = row['new_deaths']
                    num_deaths = None
                    if deaths != '':
                        num_deaths = float(deaths)
                    infections = row['new_cases']
                    num_infections = None
                    if infections != '':
                        num_infections = float(infections)
                    vaccinations = row['new_vaccinations']
                    number_vacc = None
                    if vaccinations != '':
                        number_vacc = float(vaccinations)
                    season = getSeason(continent_name, daily_date)
                    policy_level = row['stringency_index']
                    num_policy = None
                    if policy_level != '':
                        num_policy = float(policy_level)
                    cur.execute(insertDaily, (num_deaths, num_infections, number_vacc, season, num_policy, daily_date,
                                              country_iso))
                    conn.commit()


if __name__ == '__main__':
    config = load_config()
    daily_etl(config)
