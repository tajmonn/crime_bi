import psycopg2
from datetime import datetime
import csv

# Connect to your PostgreSQL database
conn = psycopg2.connect(
    dbname="postgres", user="postgres", password="crime", host="localhost"
)

# Create a cursor object
cur = conn.cursor()

# Query the database
cur.execute("""SELECT id FROM Przestepstwo WHERE id_typ = 1;""")
rows = cur.fetchall()

with open("raw/crime.tsv", mode="r", encoding="utf-8") as file:
    reader = csv.DictReader(file, delimiter="\t")
    i = 0
    for row in reader:
        if row["arrest"] == "TRUE" and row["domestic"] == "TRUE":
            i += 1
print("csv", i)
print(len(rows))
# Close the cursor and connection
cur.close()
conn.close()
