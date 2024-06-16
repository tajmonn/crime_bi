import psycopg2
import csv


def main() -> None:
    # Połączenie z bazą danych PostgreSQL
    conn = psycopg2.connect(
        dbname="postgres", user="postgres", password="crime", host="localhost"
    )

    # Utworzenie kursora
    cur = conn.cursor()

    # Ścieżka do pliku CSV
    csv_file_path = "raw/IUCR.tsv"

    def repeat(row: dict) -> bool:
        cur.execute(f"SELECT * FROM Opis")
        rows = cur.fetchall()
        for a in rows:
            if a[1:] == (row["primary_description"], row["secondary_description"]):
                return True
        return False

    with open(csv_file_path, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file, delimiter="\t")
        for row in reader:
            if repeat(row):
                continue
            cur.execute(
                """
                INSERT INTO Opis (
                    "primary description",
                    "secondary description"
                ) VALUES (%s, %s)
                """,
                (row["primary_description"], row["secondary_description"]),
            )
        # Zatwierdzanie transakcji
        conn.commit()

    # Zamknięcie kursora i połączenia
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
