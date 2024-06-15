import psycopg2
import csv

# Połączenie z bazą danych PostgreSQL
conn = psycopg2.connect(
    dbname="postgres", user="postgres", password="crime", host="localhost"
)

# Utworzenie kursora
cur = conn.cursor()

bools = [True, False]

for bool1 in bools:
    for bool2 in bools:
        cur.execute(
            """
                INSERT INTO Typ (
                    arrest,
                    domestic
                ) VALUES (%s, %s)
            """,
            (bool1, bool2),
        )

"""
1 True True
2 True False
3 False True
4 False False
"""
# Zatwierdzanie transakcji
conn.commit()


# Zamknięcie kursora i połączenia
cur.close()
conn.close()
