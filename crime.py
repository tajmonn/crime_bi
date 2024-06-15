import psycopg2
import csv
from datetime import datetime
from itertools import islice

# Połączenie z bazą danych PostgreSQL
conn = psycopg2.connect(
    dbname="postgres", user="postgres", password="crime", host="localhost"
)

# Utworzenie kursora
cur = conn.cursor()

# Ścieżka do pliku CSV
csv_file_path = "raw/crime.tsv"

# some extra file for going through all of them would be nice

start_record = 0
no_records = 1452551  # 10_000


def get_typ_id(row):
    arrest = True if row["arrest"] == "TRUE" else False
    domestic = True if row["domestic"] == "TRUE" else False
    cur.execute(
        """SELECT id FROM Typ WHERE arrest = %s AND domestic = %s""",
        (arrest, domestic),
    )
    return cur.fetchone()[0]


def get_pogoda_id(data: datetime):
    data = data.date()
    with open("raw/weather.csv", mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            if datetime.strptime(row["datetime"], "%Y-%m-%d").date() == data:

                cur.execute(
                    """SELECT id FROM Pogoda where precip = %s AND precipcover = %s AND temp = %s AND windspeed = %s AND sealevelpressure = %s AND cloudcover = %s and sunrise = %s and sunset = %s""",
                    (
                        float(row["precip"]),
                        float(row["precipcover"]),
                        float(row["temp"]),
                        float(row["windspeed"]),
                        float(row["sealevelpressure"]),
                        float(row["cloudcover"]),
                        datetime.strptime(row["sunrise"], "%Y-%m-%dT%H:%M:%S"),
                        datetime.strptime(row["sunset"], "%Y-%m-%dT%H:%M:%S"),
                    ),
                )
                return cur.fetchone()[0]


def get_sasiedztwo_id(community_area):
    with open("raw/Community_Area.tsv", mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file, delimiter="\t")
        for row in reader:
            if community_area == row["area_number"]:
                cur.execute(
                    """SELECT id FROM Sasiedztwo WHERE name = %s AND side = %s;""",
                    (row["community_area_name"], row["side"]),
                )
                return cur.fetchone()[0]


def get_opis_id(iucr):
    with open("raw/IUCR.tsv", mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file, delimiter="\t")
        for row in reader:
            if iucr == row["iucr"]:
                cur.execute(
                    """SELECT id FROM Opis WHERE "primary description" = %s AND "secondary description" = %s;""",
                    (row["primary_description"], row["secondary_description"]),
                )
                return cur.fetchone()[0]


def get_przychod_id(community_area):
    with open("raw/Per_Capita_Income.tsv", mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file, delimiter="\t")
        for row in reader:
            if community_area == row["Community Area Number"]:
                cur.execute(
                    """SELECT id FROM Przychod WHERE "Percent of housing crowded" = %s AND "Hardship index" = %s AND "Per capita income" = %s AND "Percent aged 16+ unemployed" = %s AND "Percent aged 25+ without highschool diploma" = %s AND "Percent aged under 18 or over 64" = %s AND "Percent households below poverty" = %s;""",
                    (
                        float(row["PERCENT OF HOUSING CROWDED"]),
                        int(row["HARDSHIP INDEX"]),
                        int(row["PER CAPITA INCOME"]),
                        float(row["PERCENT AGED 16+ UNEMPLOYED"]),
                        float(row["PERCENT AGED 25+ WITHOUT HIGH SCHOOL DIPLOMA"]),
                        float(row["PERCENT AGED UNDER 18 OR OVER 64"]),
                        float(row["PERCENT HOUSEHOLDS BELOW POVERTY"]),
                    ),
                )
                return cur.fetchone()[0]


def get_populacja_id(year, community_area):
    with open("raw/populacja.csv", mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row["Year"] == str(year) and row["Geography"] == str(community_area):
                cur.execute(
                    """SELECT id FROM Populacja WHERE
                                "Population - Age 0-17" = %s AND
                                "Population - Age 18-29" = %s AND
                                "Population - Age 30-39" = %s AND
                                "Population - Age 40-49" = %s AND
                                "Population - Age 50-59" = %s AND
                                "Population - Age 60-69" = %s AND
                                "Population - Age 70-79" = %s AND
                                "Population - Age 80+" = %s AND
                                "Population - Age 0-4" = %s AND
                                "Population - Age 5+" = %s AND
                                "Population - Age 18+" = %s AND
                                "Population - Age 65+" = %s AND
                                "Population - Female" = %s AND
                                "Population - Male" = %s AND
                                "Population - Latinx" = %s AND
                                "Population - White Non-Latinx" = %s;""",
                    (
                        float(row["Population - Age 0-17"]),
                        float(row["Population - Age 18-29"]),
                        float(row["Population - Age 30-39"]),
                        float(row["Population - Age 40-49"]),
                        float(row["Population - Age 50-59"]),
                        float(row["Population - Age 60-69"]),
                        float(row["Population - Age 70-79"]),
                        float(row["Population - Age 80+"]),
                        float(row["Population - Age 0-4"]),
                        float(row["Population - Age 5+"]),
                        float(row["Population - Age 18+"]),
                        float(row["Population - Age 65+"]),
                        float(row["Population - Female"]),
                        float(row["Population - Male"]),
                        float(row["Population - Latinx"]),
                        float(row["Population - White Non-Latinx"]),
                    ),
                )
                return cur.fetchone()[0]


def get_dummy_ids():
    cur.execute("""SELECT id FROM Sasiedztwo WHERE name = %s;""", (None,))
    sasiedztwo_id = cur.fetchone()[0]
    cur.execute("""SELECT id FROM Przychod WHERE 'Hardship index' = %s;""", (None,))
    przychod_id = cur.fetchone()[0]
    cur.execute("""SELECT id FROM Populacja WHERE 'Population - Male' = %s;""", (None,))
    populacja_id = cur.fetchone()[0]
    return sasiedztwo_id, przychod_id, populacja_id


def insert_crime(
    id_sasiedztwo, id_typ, data, time, id_opis, id_pogoda, id_przychod, id_populacja
):
    cur.execute(
        """
            INSERT INTO Przestepstwo (
                id_sasiedztwo,
                id_typ,
                data,
                time,
                id_opis,
                id_pogoda,
                id_przychod,
                id_populacja
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
        (
            id_sasiedztwo,
            id_typ,
            data,
            time,
            id_opis,
            id_pogoda,
            id_przychod,
            id_populacja,
        ),
    )


with open(csv_file_path, mode="r", encoding="utf-8") as file:
    reader = csv.DictReader(file, delimiter="\t")

    for _ in range(start_record):
        next(reader)
    percent = 0
    # for row in islice(reader, no_records):
    for row in reader:
        percent += 1
        if percent % 100 == 0:
            print(round(percent / no_records * 100), "%")
        data = datetime.strptime(row["date"], "%m/%d/%Y %H:%M")
        if row["community_area"] == "":
            sasiedztwo_id, przychod_id, populacja_id = get_dummy_ids()
            insert_crime(
                sasiedztwo_id,
                get_typ_id(row),
                data.date(),
                data.time(),
                get_opis_id(row["iucr"]),
                get_pogoda_id(data),
                przychod_id,
                populacja_id,
            )

        insert_crime(
            get_sasiedztwo_id(row["community_area"]),
            get_typ_id(row),
            data.date(),
            data.time(),
            get_opis_id(row["iucr"]),
            get_pogoda_id(data),
            get_przychod_id(row["community_area"]),
            get_populacja_id(data.year, row["community_area"]),
        )

    # Zatwierdzanie transakcji
    conn.commit()


# Zamknięcie kursora i połączenia
cur.close()
conn.close()
