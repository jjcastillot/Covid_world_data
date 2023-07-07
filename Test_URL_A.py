import pandas as pd
import psycopg2
from datetime import datetime,timedelta
from psycopg2 import sql
from creds import my_host, my_database, my_user, my_password

# Read the CSV file from the URL
url = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
df = pd.read_csv(url)

# Reads the last updated date from the TXT file
with open('last_date_updated.txt', 'r') as f:
    last_date_str = f.read().strip()
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d')

# Takes into account just the rows since the last update
df = df[df['date'].apply(pd.to_datetime) > last_date]
most_recent_date = df['date'].max()

# Updates the TXT file
with open('last_date_updated.txt', 'w') as f:
    f.write(most_recent_date)

# Create a PostgreSQL connection
conn = psycopg2.connect(host=my_host, database=my_database, user=my_user, password=my_password)

# Create a table in the database
cur = conn.cursor()
#cur.execute("DROP TABLE IF EXISTS covid_data;")

table_create = '''
    CREATE TABLE IF NOT EXISTS covid_data (
        iso_code TEXT,
        continent TEXT,
        location TEXT,
        date DATE,
        total_cases REAL,
        new_cases REAL,
        new_cases_smoothed REAL,
        total_deaths REAL,
        new_deaths REAL,
        new_deaths_smoothed REAL,
        total_cases_per_million FLOAT,
        new_cases_per_million FLOAT,
        new_cases_smoothed_per_million FLOAT,
        total_deaths_per_million FLOAT,
        new_deaths_per_million FLOAT,
        new_deaths_smoothed_per_million FLOAT,
        reproduction_rate FLOAT,
        icu_patients REAL,
        icu_patients_per_million FLOAT,
        hosp_patients REAL,
        hosp_patients_per_million FLOAT,
        weekly_icu_admissions REAL,
        weekly_icu_admissions_per_million FLOAT,
        weekly_hosp_admissions REAL,
        weekly_hosp_admissions_per_million FLOAT,
        total_tests REAL,
        new_tests REAL,
        total_tests_per_thousand FLOAT,
        new_tests_per_thousand FLOAT,
        new_tests_smoothed REAL,
        new_tests_smoothed_per_thousand FLOAT,
        positive_rate FLOAT,
        tests_per_case FLOAT,
        tests_units TEXT,
        total_vaccinations REAL,
        people_vaccinated REAL,
        people_fully_vaccinated REAL,
        total_boosters REAL,
        new_vaccinations REAL,
        new_vaccinations_smoothed REAL,
        total_vaccinations_per_hundred FLOAT,
        people_vaccinated_per_hundred FLOAT,
        people_fully_vaccinated_per_hundred FLOAT,
        total_boosters_per_hundred FLOAT,
        new_vaccinations_smoothed_per_million FLOAT,
        new_people_vaccinated_smoothed REAL,
        new_people_vaccinated_smoothed_per_hundred FLOAT,
        stringency_index FLOAT,
        population_density FLOAT,
        median_age FLOAT,
        aged_65_older FLOAT,
        aged_70_older FLOAT,
        gdp_per_capita FLOAT,
        extreme_poverty FLOAT,
        cardiovasc_death_rate FLOAT,
        diabetes_prevalence FLOAT,
        female_smokers FLOAT,
        male_smokers FLOAT,
        handwashing_facilities FLOAT,
        hospital_beds_per_thousand FLOAT,
        life_expectancy FLOAT,
        human_development_index FLOAT,
        population FLOAT,
        excess_mortality_cumulative_absolute FLOAT,
        excess_mortality_cumulative FLOAT,
        excess_mortality FLOAT,
        excess_mortality_cumulative_per_million FLOAT
    )
'''
cur.execute(table_create)

i=0
# Insert the data into the table
for row in df.itertuples(index=False):
    query = sql.SQL('INSERT INTO covid_data VALUES ({})').format(
        sql.SQL(', ').join(sql.Placeholder() * len(row))
    )
    cur.execute(query, row)
    print(i)
    i += 1

conn.commit()
cur.close()
conn.close()

