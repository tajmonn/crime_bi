import psycopg2
import csv


def main() -> None:
    # Establish connection to the PostgreSQL database
    conn = psycopg2.connect(
        dbname="postgres", user="postgres", password="crime", host="localhost"
    )

    # Create a cursor object
    cur = conn.cursor()

    # Path to the TSV file
    csv_file_path = "raw/Per_Capita_Income.tsv"

    def extract_row_data(row: dict) -> tuple:
        """Extract the required fields from the row."""
        return (
            float(row["PERCENT OF HOUSING CROWDED"]),
            int(row["HARDSHIP INDEX"]),
            int(row["PER CAPITA INCOME"]),
            float(row["PERCENT AGED 16+ UNEMPLOYED"]),
            float(row["PERCENT AGED 25+ WITHOUT HIGH SCHOOL DIPLOMA"]),
            float(row["PERCENT AGED UNDER 18 OR OVER 64"]),
            float(row["PERCENT HOUSEHOLDS BELOW POVERTY"]),
        )

    def is_row_repeated(row: dict) -> bool:
        """Check if the row already exists in the database."""
        cur.execute("SELECT * FROM Przychod")
        rows = cur.fetchall()
        row_data = extract_row_data(row)
        for a in rows:
            if a[1:] == row_data:
                return True
        return False

    def is_chicago(row: dict) -> bool:
        """Check if the row corresponds to Chicago."""
        return row["COMMUNITY AREA NAME"] == "CHICAGO"

    def should_skip_row(row: dict) -> bool:
        """Determine if the row should be skipped based on certain conditions."""
        return is_chicago(row) or is_row_repeated(row)

    def insert_row(row: dict) -> None:
        """Insert the row into the database."""
        insert_query = """
        INSERT INTO Przychod (
            "Percent of housing crowded",
            "Hardship index",
            "Per capita income",
            "Percent aged 16+ unemployed",
            "Percent aged 25+ without highschool diploma",
            "Percent aged under 18 or over 64",
            "Percent households below poverty"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(insert_query, extract_row_data(row))

    # Read the TSV file and insert rows into the database
    with open(csv_file_path, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file, delimiter="\t")
        for row in reader:
            if should_skip_row(row):
                continue
            insert_row(row)

    # Commit the transaction
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
