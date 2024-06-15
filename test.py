import psycopg2
from datetime import datetime

# Connect to your PostgreSQL database
conn = psycopg2.connect(
    dbname="postgres", user="postgres", password="crime", host="localhost"
)

# Create a cursor object
cur = conn.cursor()

# Query the database
cur.execute("""SELECT id FROM Przychod WHERE "Hardship index" = %s;""", (None,))
print(cur.fetchone())
# cur.execute("""SELECT * FROM Sasiedztwo""")
# rows = cur.fetchall()
# for row in rows:
#     print(row)
# print(len(rows))
# Close the cursor and connection
cur.close()
conn.close()
