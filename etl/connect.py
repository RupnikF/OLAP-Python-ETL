import psycopg2
from config import load_config


# Continents primarily in the Northern Hemisphere
northern_hemisphere_continents = ["North America", "Europe", "Asia", "Africa"]

# Continents primarily in the Southern Hemisphere
southern_hemisphere_continents = ["Australia", "Antarctica", "South America", "Oceania"]

def connect(config):
    """ Connect to the PostgreSQL database server """
    try:
        # connecting to the PostgreSQL server
        with psycopg2.connect(**config) as conn:
            print('Connected to the PostgreSQL server.')
            return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)


if __name__ == '__main__':
    config = load_config()
    connect(config)
