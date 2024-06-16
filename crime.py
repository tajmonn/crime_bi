import psycopg2
import csv
from datetime import datetime


def main() -> None:
    # Establish connection to the PostgreSQL database
    conn = psycopg2.connect(
        dbname="postgres", user="postgres", password="crime", host="localhost"
    )

    # Create a cursor object
    cur = conn.cursor()

    # File paths
    csv_file_path = "raw/crime.tsv"
    weather_file_path = "raw/weather.csv"
    community_area_file_path = "raw/Community_Area.tsv"
    iucr_file_path = "raw/IUCR.tsv"
    income_file_path = "raw/Per_Capita_Income.tsv"
    population_file_path = "raw/populacja.csv"

    # Number of records to process
    no_records = 1452551

    def get_type_id(row: dict) -> int:
        arrest = row["arrest"] == "TRUE"
        domestic = row["domestic"] == "TRUE"
        cur.execute(
            """SELECT id_typ FROM Typ WHERE arrest = %s AND domestic = %s""",
            (arrest, domestic),
        )
        return cur.fetchone()[0]

    def get_weather_id(date: datetime) -> int:
        date_str = date.strftime("%Y-%m-%d")
        with open(weather_file_path, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row["datetime"].startswith(date_str):
                    cur.execute(
                        """SELECT id_pogoda FROM Pogoda WHERE precip = %s AND precipcover = %s AND temp = %s AND windspeed = %s AND sealevelpressure = %s AND cloudcover = %s AND sunrise = %s AND sunset = %s;""",
                        (
                            float(row["precip"]),
                            float(row["precipcover"]),
                            float(row["temp"]),
                            float(row["windspeed"]),
                            float(row["sealevelpressure"]),
                            float(row["cloudcover"]),
                            datetime.strptime(
                                row["sunrise"], "%Y-%m-%dT%H:%M:%S"
                            ).strftime("%H:%M"),
                            datetime.strptime(
                                row["sunset"], "%Y-%m-%dT%H:%M:%S"
                            ).strftime("%H:%M"),
                        ),
                    )
                    return cur.fetchone()[0]

    def get_community_area_id(community_area: str) -> int:
        with open(community_area_file_path, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file, delimiter="\t")
            for row in reader:
                if community_area == row["area_number"]:
                    cur.execute(
                        """SELECT id_sasiedztwo FROM Sasiedztwo WHERE name = %s AND side = %s;""",
                        (row["community_area_name"], row["side"]),
                    )
                    return cur.fetchone()[0]

    def get_description_id(iucr: str) -> int:
        with open(iucr_file_path, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file, delimiter="\t")
            for row in reader:
                if iucr == row["iucr"]:
                    cur.execute(
                        """SELECT id_opis FROM Opis WHERE "primary description" = %s AND "secondary description" = %s;""",
                        (row["primary_description"], row["secondary_description"]),
                    )
                    return cur.fetchone()[0]

    def get_income_id(community_area: str) -> int:
        with open(income_file_path, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file, delimiter="\t")
            for row in reader:
                if community_area == row["Community Area Number"]:
                    cur.execute(
                        """SELECT id_przychod FROM Przychod WHERE "Percent of housing crowded" = %s AND "Hardship index" = %s AND "Per capita income" = %s AND "Percent aged 16+ unemployed" = %s AND "Percent aged 25+ without highschool diploma" = %s AND "Percent aged under 18 or over 64" = %s AND "Percent households below poverty" = %s;""",
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

    def get_population_id(year: int, community_area: str) -> int:
        with open(population_file_path, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row["Year"] == str(year) and row["Geography"] == str(community_area):
                    cur.execute(
                        """SELECT id_populacja FROM Populacja WHERE
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

    def get_dummy_ids() -> tuple[int, int, int]:
        cur.execute("""SELECT id_sasiedztwo FROM Sasiedztwo WHERE name IS NULL;""")
        sasiedztwo_id = cur.fetchone()[0]
        cur.execute(
            """SELECT id_przychod FROM Przychod WHERE "Hardship index" IS NULL;"""
        )
        przychod_id = cur.fetchone()[0]
        cur.execute(
            """SELECT id_populacja FROM Populacja WHERE "Population - Male" IS NULL;"""
        )
        populacja_id = cur.fetchone()[0]
        return sasiedztwo_id, przychod_id, populacja_id

    def insert_crime(
        id_sasiedztwo: int,
        id_typ: int,
        data: datetime.date,
        time: datetime.time,
        id_opis: int,
        id_pogoda: int,
        id_przychod: int,
        id_populacja: int,
    ) -> None:
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

        percent = 0
        for row in reader:
            percent += 1
            if percent % 100 == 0:
                print(round(percent / no_records * 100), "%")
            data = datetime.strptime(row["date"], "%m/%d/%Y %H:%M")
            if row["community_area"] == "":
                sasiedztwo_id, przychod_id, populacja_id = get_dummy_ids()
                insert_crime(
                    sasiedztwo_id,
                    get_type_id(row),
                    data.date(),
                    data.time(),
                    get_description_id(row["iucr"]),
                    get_weather_id(data),
                    przychod_id,
                    populacja_id,
                )
            else:
                insert_crime(
                    get_community_area_id(row["community_area"]),
                    get_type_id(row),
                    data.date(),
                    data.time(),
                    get_description_id(row["iucr"]),
                    get_weather_id(data),
                    get_income_id(row["community_area"]),
                    get_population_id(data.year, row["community_area"]),
                )

    # Commit the transaction
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
