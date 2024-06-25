--SCHEMA FOR COVID DW

CREATE TABLE strategy
(
    strategy_key serial PRIMARY KEY,
    name         text NOT NULL
);

CREATE TABLE region
(
    region_key serial PRIMARY KEY,
    name       TEXT NOT NULL
);


CREATE TABLE time
(
    time_key    serial PRIMARY KEY,
    year        int         NOT NULL,
    monthnumber int         NOT NULL,
    monthname   text        NOT NULL,
    day         int         NOT NULL,
    date        date UNIQUE NOT NULL
);

CREATE TABLE country
(
    country_key  serial PRIMARY KEY,
    name         text NOT NULL,
    population   int  NOT NULL,
    area         int  NOT NULL,
    region_key   int  NOT NULL REFERENCES region (region_key),
    strategy_key int  NOT NULL REFERENCES strategy (strategy_key)
);


CREATE TABLE daily_data
(
    daily_data_key    serial PRIMARY KEY,
    deaths       int  NOT NULL,
    infections   int  NOT NULL,
    vaccinations int  NOT NULL,
    season       text NOT NULL,
    time_key     int  NOT NULL REFERENCES time (time_key),
    country_key  int  NOT NULL REFERENCES country (country_key)
);

CREATE TABLE year_data
(
    year_data_key serial PRIMARY KEY,
    pbi INT NOT NULL,
    health_budget int,
    deaths        int NOT NULL,
    country_key   int NOT NULL REFERENCES country(country_key),
    year          int NOT NULL
);