#to connect to  postgres db
import psycopg2
#to import data from csv file
import csv

import os
print(os.getcwd())


csv_data = csv.reader(open('IN.csv'))
conn = psycopg2.connect(dbname="postgres", user = "postgres", password = "password", host = "127.0.0.1", port = "5432")
cur = conn.cursor()


cur.execute("COPY apitest FROM '/home/pi/apitest/IN.csv',' CSV;")
print ("Data imported from csv file")

conn.commit()
cur.close()
