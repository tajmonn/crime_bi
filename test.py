import psycopg2
from datetime import datetime

# Connect to your PostgreSQL database
conn = psycopg2.connect(
    dbname="postgres", user="postgres", password="crime", host="localhost"
)

# Create a cursor object
cur = conn.cursor()

cur.execute("SELECT * FROM przestepstwo;")
a = cur.fetchall()
# for i in a:
#     print(i)
print(len(a))
cur.close()
conn.close()
