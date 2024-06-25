import psycopg2, csv
from config import load_config
from datetime import date, datetime

# Continents primarily in the Northern Hemisphere
northern_hemisphere_continents = ["North America", "Europe", "Asia", "Africa"]

# Continents primarily in the Southern Hemisphere
southern_hemisphere_continents = ["Australia", "Antarctica", "South America", "Oceania"]


# Function to determine the season, it needs a
def season(continent, date_str):
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
    try:
        # connecting to the PostgreSQL server
        with open(config["dailydata"], newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            with psycopg2.connect(**config) as conn:
                for row in reader:
                    continent = row['continent']





    except (psycopg2.DatabaseError, Exception) as error:
        print(error)


if __name__ == '__main__':
    config = load_config()
    daily_etl(config)
