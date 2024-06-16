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
    csv_file_path = "raw/crime.tsv"

    no_records = 1452551

    def repeat(data: datetime) -> int:
        cur.execute("SELECT * FROM Czas")
        rows = cur.fetchall()
        for a in rows:
            if a[0] == (data.time()):
                return True
        return False

    with open(csv_file_path, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file, delimiter="\t")
        percent = 0
        for row in reader:
            percent += 1
            if percent % 100 == 0:
                print(round(percent / no_records * 100), "%")
            data = datetime.strptime(row["date"], "%m/%d/%Y %H:%M")
            if repeat(data):
                continue
            cur.execute(
                """
                INSERT INTO Czas (
                    time,
                    hour,
                    morning,
                    minutes
                ) VALUES (%s, %s, %s, %s)
                """,
                (
                    data.time(),
                    data.hour,
                    True if data.hour < 12 else False,
                    data.minute,
                ),
            )
        # Zatwierdzanie transakcji
        conn.commit()

    # Zamknięcie kursora i połączenia
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
