import psycopg2
import csv

# Połączenie z bazą danych PostgreSQL
conn = psycopg2.connect(
    dbname="postgres", user="postgres", password="crime", host="localhost"
)

# Utworzenie kursora
cur = conn.cursor()

# Ścieżka do pliku CSV
csv_file_path = "raw/Per_Capita_Income.tsv"


def repeat(row):
    cur.execute("SELECT * FROM Przychod")
    rows = cur.fetchall()
    for a in rows:
        if a[1:] == (
            float(row["PERCENT OF HOUSING CROWDED"]),
            int(row["HARDSHIP INDEX"]),
            int(row["PER CAPITA INCOME"]),
            float(row["PERCENT AGED 16+ UNEMPLOYED"]),
            float(row["PERCENT AGED 25+ WITHOUT HIGH SCHOOL DIPLOMA"]),
            float(row["PERCENT AGED UNDER 18 OR OVER 64"]),
            float(row["PERCENT HOUSEHOLDS BELOW POVERTY"]),
        ):
            return True
    return False


def chicago(row):
    if row["COMMUNITY AREA NAME"] == "CHICAGO":
        return True
    return False


def exeptions(row):
    if chicago(row):
        return True
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
            INSERT INTO Przychod (
                "Percent of housing crowded",
                "Hardship index",
                "Per capita income",
                "Percent aged 16+ unemployed",
                "Percent aged 25+ without highschool diploma",
                "Percent aged under 18 or over 64",
                "Percent households below poverty"
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
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
    # Zatwierdzanie transakcji
    conn.commit()
    # Dummy
    cur.execute(
        """
            INSERT INTO Przychod (
                "Percent of housing crowded",
                "Hardship index",
                "Per capita income",
                "Percent aged 16+ unemployed",
                "Percent aged 25+ without highschool diploma",
                "Percent aged under 18 or over 64",
                "Percent households below poverty"
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
        (
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
    )
    conn.commit()


# Zamknięcie kursora i połączenia
cur.close()
conn.close()
