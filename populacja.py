import psycopg2
import csv


def main() -> None:
    # Establish connection to the PostgreSQL database
    conn = psycopg2.connect(
        dbname="postgres", user="postgres", password="crime", host="localhost"
    )

    # Create a cursor object
    cur = conn.cursor()

    # Path to the CSV file
    csv_file_path = "raw/Chicago_Population_Counts.csv"
    community_zip_path = "raw/community_zip.csv"
    output_csv_path = "raw/populacja.csv"

    def is_chicago(row: dict) -> bool:
        """Check if the row corresponds to Chicago."""
        return row["Geography"] == "Chicago"

    def fetch_existing_rows() -> list:
        """Fetch existing rows from the Populacja table."""
        cur.execute("SELECT * FROM Populacja")
        return cur.fetchall()

    def is_row_repeated(values: list, existing_rows: list) -> bool:
        """Check if the row already exists in the database."""
        for row in existing_rows:
            if row[1:] == tuple(values):
                return True
        return False

    def generate_translator(file_path: str) -> dict:
        """Generate a translator dictionary from ZIP codes to community areas."""
        translator = {}
        with open(file_path, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row["ZCTA5"] in translator:
                    translator[row["ZCTA5"]].append(row["CHGOCA"])
                else:
                    translator[row["ZCTA5"]] = [row["CHGOCA"]]
        return translator

    def extract_values(row: dict) -> list:
        """Extract values from a row for database insertion."""
        return [
            int(row.get("Population - Age 0-17", 0)),
            int(row.get("Population - Age 18-29", 0)),
            int(row.get("Population - Age 30-39", 0)),
            int(row.get("Population - Age 40-49", 0)),
            int(row.get("Population - Age 50-59", 0)),
            int(row.get("Population - Age 60-69", 0)),
            int(row.get("Population - Age 70-79", 0)),
            int(row.get("Population - Age 80+", 0)),
            int(row.get("Population - Age 0-4", 0)),
            int(row.get("Population - Age 5+", 0)),
            int(row.get("Population - Age 18+", 0)),
            int(row.get("Population - Age 65+", 0)),
            int(row.get("Population - Female", 0)),
            int(row.get("Population - Male", 0)),
            int(row.get("Population - Latinx", 0)),
            int(row.get("Population - White Non-Latinx", 0)),
        ]

    insert_query = """
        INSERT INTO Populacja (
            "Population - Age 0-17",
            "Population - Age 18-29",
            "Population - Age 30-39",
            "Population - Age 40-49",
            "Population - Age 50-59",
            "Population - Age 60-69",
            "Population - Age 70-79",
            "Population - Age 80+",
            "Population - Age 0-4",
            "Population - Age 5+",
            "Population - Age 18+",
            "Population - Age 65+",
            "Population - Female",
            "Population - Male",
            "Population - Latinx",
            "Population - White Non-Latinx"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

    def insert_data_into_db(values: list) -> None:
        """Insert data into the database."""
        cur.execute(insert_query, values)

    def aggregate_population_data(reader: csv.DictReader, translator: dict) -> dict:
        """Aggregate population data from the CSV reader."""
        agg_data = {}
        for row in reader:
            if is_chicago(row):
                continue
            community_areas = translator[row["Geography"]]
            year = row["Year"]
            for community_area in community_areas:
                if (year, community_area) not in agg_data:
                    agg_data[(year, community_area)] = {
                        key: 0
                        for key in row.keys()
                        if key
                        not in ["Geography Type", "Year", "Geography", "Record ID"]
                    }
                for key in agg_data[(year, community_area)].keys():
                    if row[key]:
                        agg_data[(year, community_area)][key] += int(row[key])
        return agg_data

    def write_aggregated_data_to_csv(agg_data: dict, output_path: str) -> None:
        """Write the aggregated data to a CSV file."""
        fieldnames = [
            "Year",
            "Geography",
            "Population - Age 0-17",
            "Population - Age 18-29",
            "Population - Age 30-39",
            "Population - Age 40-49",
            "Population - Age 50-59",
            "Population - Age 60-69",
            "Population - Age 70-79",
            "Population - Age 80+",
            "Population - Age 0-4",
            "Population - Age 5+",
            "Population - Age 18+",
            "Population - Age 65+",
            "Population - Female",
            "Population - Male",
            "Population - Latinx",
            "Population - White Non-Latinx",
        ]
        with open(output_path, "w", newline="", encoding="utf-8") as output_csvfile:
            writer = csv.DictWriter(output_csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for (year, geography), data in agg_data.items():
                row = {
                    "Year": year,
                    "Geography": geography,
                    **{
                        key: data.get(key, 0)
                        for key in fieldnames
                        if key not in ["Year", "Geography"]
                    },
                }
                writer.writerow(row)

    # Main processing
    existing_rows = fetch_existing_rows()
    translator = generate_translator(community_zip_path)

    with open(csv_file_path, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        agg_data = aggregate_population_data(reader, translator)

    for (year, community_area), data in agg_data.items():
        values = extract_values(data)
        if is_row_repeated(values, existing_rows):
            continue
        insert_data_into_db(values)
        conn.commit()

    write_aggregated_data_to_csv(agg_data, output_csv_path)

    cur.execute(
        insert_query,
        (
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
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
    # Close the cursor and connection
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
