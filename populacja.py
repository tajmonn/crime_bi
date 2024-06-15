import psycopg2
import csv

# Połączenie z bazą danych PostgreSQL
conn = psycopg2.connect(
    dbname="postgres", user="postgres", password="crime", host="localhost"
)

# Utworzenie kursora
cur = conn.cursor()

# Ścieżka do pliku CSV
csv_file_path = "raw/Chicago_Population_Counts.csv"


def row_chicago(row):
    if row["Geography"] == "Chicago":
        return True
    return False


def repeat(values):
    cur.execute("SELECT * FROM Populacja")
    rows = cur.fetchall()
    for a in rows:
        if a[1:] == values:
            return True
    return False


def exeptions(row):
    if repeat(row):
        return True
    return False


def generate_translator():
    with open("raw/community_zip.csv", mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        translator = {}
        for row in reader:
            if row["ZCTA5"] in translator:
                translator[row["ZCTA5"]].append(row["CHGOCA"])
            else:
                translator[row["ZCTA5"]] = [(row["CHGOCA"])]
    return translator


with open(csv_file_path, mode="r", encoding="utf-8") as file:
    translator = generate_translator()
    reader = csv.DictReader(file)
    agg_data = {}
    for row in reader:
        if row_chicago(row):
            continue
        community_areas = translator[row["Geography"]]
        year = row["Year"]
        for community_area in community_areas:
            if (year, community_area) in agg_data:
                for key in row.keys():
                    if key not in [
                        "Geography Type",
                        "Year",
                        "Geography",
                        "Population - Age 5-11",
                        "Population - Age 12-17",
                        "Population - Asian Non-Latinx",
                        "Population - Black Non-Latinx",
                        "Population - Other Race Non-Latinx",
                        "Record ID",
                    ]:
                        if row[key]:
                            agg_data[(year, community_area)][key] += int(row[key])
            else:
                agg_data[(year, community_area)] = {}
                for key in row.keys():
                    if key not in [
                        "Geography Type",
                        "Year",
                        "Geography",
                        "Population - Age 5-11",
                        "Population - Age 12-17",
                        "Population - Asian Non-Latinx",
                        "Population - Black Non-Latinx",
                        "Population - Other Race Non-Latinx",
                        "Record ID",
                    ]:
                        if row[key]:
                            agg_data[(year, community_area)][key] = int(row[key])

for (year, community_area), data in agg_data.items():
    values = [
        data["Population - Age 0-17"],
        data["Population - Age 18-29"],
        data["Population - Age 30-39"],
        data["Population - Age 40-49"],
        data["Population - Age 50-59"],
        data["Population - Age 60-69"],
        data["Population - Age 70-79"],
        data["Population - Age 80+"],
        data["Population - Age 0-4"],
        data["Population - Age 5+"],
        data["Population - Age 18+"],
        data["Population - Age 65+"],
        data["Population - Female"],
        data["Population - Male"],
        data["Population - Latinx"],
        data["Population - White Non-Latinx"],
    ]
    # SQL INSERT statement
    sql = """
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
    if exeptions(values):
        continue
    cur.execute(sql, values)
    # Zatwierdzanie transakcji
    conn.commit()

    # Dummy
    cur.execute(
        """
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
        """,
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


# Zamknięcie kursora i połączenia
cur.close()
conn.close()
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
with open("raw/populacja.csv", "w", newline="", encoding="utf-8") as output_csvfile:
    writer = csv.DictWriter(output_csvfile, fieldnames=fieldnames)
    writer.writeheader()

    # Write aggregated data to new CSV file
    for (year, geography), data in agg_data.items():
        row = {
            "Year": year,
            "Geography": geography,
            "Population - Age 0-17": data.get("Population - Age 0-17", 0),
            "Population - Age 18-29": data.get("Population - Age 18-29", 0),
            "Population - Age 30-39": data.get("Population - Age 30-39", 0),
            "Population - Age 40-49": data.get("Population - Age 40-49", 0),
            "Population - Age 50-59": data.get("Population - Age 50-59", 0),
            "Population - Age 60-69": data.get("Population - Age 60-69", 0),
            "Population - Age 70-79": data.get("Population - Age 70-79", 0),
            "Population - Age 80+": data.get("Population - Age 80+", 0),
            "Population - Age 0-4": data.get("Population - Age 0-4", 0),
            "Population - Age 5+": data.get("Population - Age 5+", 0),
            "Population - Age 18+": data.get("Population - Age 18+", 0),
            "Population - Age 65+": data.get("Population - Age 65+", 0),
            "Population - Female": data.get("Population - Female", 0),
            "Population - Male": data.get("Population - Male", 0),
            "Population - Latinx": data.get("Population - Latinx", 0),
            "Population - White Non-Latinx": data.get(
                "Population - White Non-Latinx", 0
            ),
        }
        writer.writerow(row)
