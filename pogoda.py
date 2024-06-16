import psycopg2
import csv
from datetime import datetime


def main() -> None:
    # Połączenie z bazą danych PostgreSQL
    conn = psycopg2.connect(
        dbname="postgres", user="postgres", password="crime", host="localhost"
    )

    # Utworzenie kursora
    cur = conn.cursor()

    # Ścieżka do pliku CSV
    csv_file_path = "raw/weather.csv"

    def repeat(row: dict) -> bool:
        cur.execute("SELECT * FROM Populacja")
        rows = cur.fetchall()
        for a in rows:
            if a[1:] == (
                float(row["precip"]),
                float(row["precipcover"]),
                float(row["temp"]),
                float(row["windspeed"]),
                float(row["sealevelpressure"]),
                float(row["cloudcover"]),
                datetime.strptime(row["sunrise"], "%Y-%m-%dT%H:%M:%S").strftime(
                    "%H:%M"
                ),
                datetime.strptime(row["sunset"], "%Y-%m-%dT%H:%M:%S").strftime("%H:%M"),
            ):
                return True
        return False

    with open(csv_file_path, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            if repeat(row):
                continue
            cur.execute(
                """
                INSERT INTO Pogoda (
                    precip,
                    precipcover,
                    temp,
                    windspeed,
                    sealevelpressure,
                    cloudcover,
                    sunrise,
                    sunset
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    float(row["precip"]),
                    float(row["precipcover"]),
                    float(row["temp"]),
                    float(row["windspeed"]),
                    float(row["sealevelpressure"]),
                    float(row["cloudcover"]),
                    datetime.strptime(row["sunrise"], "%Y-%m-%dT%H:%M:%S").strftime(
                        "%H:%M"
                    ),
                    datetime.strptime(row["sunset"], "%Y-%m-%dT%H:%M:%S").strftime(
                        "%H:%M"
                    ),
                ),
            )
        # Zatwierdzanie transakcji
        conn.commit()

    # Zamknięcie kursora i połączenia
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
