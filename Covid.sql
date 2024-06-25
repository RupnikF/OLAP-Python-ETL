--SCHEMA FOR COVID DW


CREATE TABLE region
(
    region_key serial PRIMARY KEY,
    name       TEXT NOT NULL
);

CREATE TABLE time
(
    time_key    text PRIMARY KEY, --Date in ISO 'yyyy-mm-dd'
    year        int         NOT NULL,
    monthnumber int         NOT NULL,
    monthname   text        NOT NULL,
    day         int         NOT NULL,
    date        date UNIQUE NOT NULL
);

CREATE TABLE country
(
    country_key varchar(3) PRIMARY KEY, --ISO 3166-1 alpha-3
    name         text NOT NULL,
    population   int  NOT NULL,
    area         int  NOT NULL,
    region_key   int  NOT NULL REFERENCES region (region_key)
);


CREATE TABLE daily_data
(
    daily_data_key    serial PRIMARY KEY,
    deaths       int  NOT NULL,
    infections   int  NOT NULL,
    vaccinations int  NOT NULL,
    season       text NOT NULL,
    policy_level int NOT NULL, --Stringency Index from data
    time_key     text  NOT NULL REFERENCES time (time_key),
    country_key  varchar(3)  NOT NULL REFERENCES country (country_key)
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