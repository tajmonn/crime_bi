import psycopg2
import csv

# Połączenie z bazą danych PostgreSQL
conn = psycopg2.connect(
    dbname="postgres", user="postgres", password="crime", host="localhost"
)

# Utworzenie kursora
cur = conn.cursor()

# Ścieżka do pliku CSV
csv_file_path = "raw/Community_Area.tsv"


def repeat(row):
    cur.execute("SELECT * FROM Sasiedztwo")
    rows = cur.fetchall()
    for a in rows:
        if a[1:] == (row["community_area_name"], row["side"]):
            return True
    return False


def exeptions(row):
    if repeat(row):
        return True
    return False


with open(csv_file_path, mode="r", encoding="utf-8") as file:
    reader = csv.DictReader(file, delimiter="\t")
    for row in reader:
        if exeptions(row):
            continue
        cur.execute(
            """
            INSERT INTO Sasiedztwo (
                name,
                side
            ) VALUES (%s, %s)
            """,
            (row["community_area_name"], row["side"]),
        )
    # Zatwierdzanie transakcji
    conn.commit()

    cur.execute(
        """
            INSERT INTO Sasiedztwo (
                name,
                side
            ) VALUES (%s, %s)
            """,
        (None, None),
    )
    conn.commit()


# Zamknięcie kursora i połączenia
cur.close()
conn.close()
