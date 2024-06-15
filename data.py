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

start_record = 10_000
no_records = 1452551  # 10_000


def repeat(data: datetime):
    cur.execute("SELECT * FROM Data")
    rows = cur.fetchall()
    for a in rows:
        if a[0] == (data.date()):
            return True
    return False


def exeptions(data):
    if repeat(data):
        return True
    return False


with open(csv_file_path, mode="r", encoding="utf-8") as file:
    reader = csv.DictReader(file, delimiter="\t")

    # for _ in range(start_record):
    #    next(reader)
    percent = 0
    # for row in islice(reader, no_records):
    for row in reader:
        percent += 1
        if percent % 100 == 0:
            print(round(percent / no_records * 100), "%")
        data = datetime.strptime(row["date"], "%m/%d/%Y %H:%M")
        if exeptions(data):
            continue
        cur.execute(
            """
            INSERT INTO Data (
                date,
                day,
                day_of_week,
                month,
                year,
                quarter
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                data.date(),
                data.day,
                data.strftime("%A"),
                data.month,
                data.year,
                (data.month - 1) // 3 + 1,
            ),
        )

    # Zatwierdzanie transakcji
    conn.commit()


# Zamknięcie kursora i połączenia
cur.close()
conn.close()
