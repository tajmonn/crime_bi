import psycopg2

# Connect to your PostgreSQL database
conn = psycopg2.connect(
    dbname="postgres", user="postgres", password="crime", host="localhost"
)

# Create a cursor object
cur = conn.cursor()

# cur.execute("""DROP TABLE IF EXISTS Populacja;""")

# POPULACJA
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS Populacja (
        id_populacja SERIAL PRIMARY KEY,
        "Population - Age 0-17" INT,
        "Population - Age 18-29" INT,
        "Population - Age 30-39" INT,
        "Population - Age 40-49" INT,
        "Population - Age 50-59" INT,
        "Population - Age 60-69" INT,
        "Population - Age 70-79" INT,
        "Population - Age 80+" INT,
        "Population - Age 0-4" INT,
        "Population - Age 5+" INT,
        "Population - Age 18+" INT,
        "Population - Age 65+" INT,
        "Population - Female" INT,
        "Population - Male" INT,
        "Population - Latinx" INT,
        "Population - White Non-Latinx" INT
    )
"""
)

# cur.execute("""DROP TABLE IF EXISTS Przychod;""")

# Przychod
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS Przychod (
        id_przychod SERIAL PRIMARY KEY,
        "Percent of housing crowded" FLOAT,
        "Hardship index" INT,
        "Per capita income" INT,
        "Percent aged 16+ unemployed" FLOAT,
        "Percent aged 25+ without highschool diploma" FLOAT,
        "Percent aged under 18 or over 64" FLOAT,
        "Percent households below poverty" FLOAT
    )
"""
)

# cur.execute("""DROP TABLE IF EXISTS Pogoda;""")

# Pogoda
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS Pogoda (
        id_pogoda SERIAL PRIMARY KEY,
        precip FLOAT,
        precipcover FLOAT,
        temp FLOAT,
        windspeed FLOAT,
        sealevelpressure FLOAT,
        cloudcover FLOAT,
        sunrise TIME,
        sunset TIME
    )
"""
)

# cur.execute("""DROP TABLE IF EXISTS Opis;""")

# Opis
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS Opis (
        id_opis SERIAL PRIMARY KEY,
        "primary description" TEXT,
        "secondary description" TEXT
    )
"""
)

# cur.execute("""DROP TABLE IF EXISTS Typ;""")

# Typ
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS Typ (
        id_typ SERIAL PRIMARY KEY,
        arrest BOOL,
        domestic BOOL
    )
"""
)

# cur.execute("""DROP TABLE IF EXISTS Sasiedztwo;""")

# Sasiedztwo
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS Sasiedztwo (
        id_sasiedztwo SERIAL PRIMARY KEY,
        name TEXT,
        side TEXT
    )
"""
)

# cur.execute("""DROP TABLE IF EXISTS Data;""")

# Data
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS Data (
        date DATE PRIMARY KEY,
        day INT,
        day_of_week TEXT,
        month TEXT,
        year INT,
        quarter INT
    )
"""
)

# cur.execute("""DROP TABLE IF EXISTS Czas;""")

# Czas
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS Czas (
        time TIME PRIMARY KEY,
        hour INT,
        morning BOOL,
        minutes INT
        )"""
)

# cur.execute("""DROP TABLE IF EXISTS Przestepstwo;""")

# Przestepstwo
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS Przestepstwo (
        id_sasiedztwo INT,
        id_typ INT,
        data DATE,
        time TIME,
        id_opis INT,
        id_pogoda INT,
        id_przychod INT,
        id_populacja INT,
        FOREIGN KEY (id_sasiedztwo) REFERENCES Sasiedztwo(id_sasiedztwo),
        FOREIGN KEY (id_populacja) REFERENCES Populacja(id_populacja),
        FOREIGN KEY (id_typ) REFERENCES Typ(id_typ),
        FOREIGN KEY (data) REFERENCES Data(date),
        FOREIGN KEY (time) REFERENCES Czas(time),
        FOREIGN KEY (id_opis) REFERENCES Opis(id_opis),
        FOREIGN KEY (id_pogoda) REFERENCES Pogoda(id_pogoda),
        FOREIGN KEY (id_przychod) REFERENCES Przychod(id_przychod)
    )
"""
)

# Commit the transaction
conn.commit()

# Query the database

# Close the cursor and connection
cur.close()
conn.close()
