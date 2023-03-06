import sqlite3
import csv
import datetime
import os
import time
import pandas as pd

queries = [
    "SELECT * FROM names ORDER BY Id LIMIT 5;",
    "SELECT * FROM table1 ORDER BY Id LIMIT 5;"
]

def createDataFrames():
    # create dataframes
    global df
    global df2
    df = pd.read_csv("names.csv")
    df2 = pd.read_csv("table1.csv")


def insertIntoTables():
    df.to_sql('names', con=con, if_exists='replace')
    df2.to_sql('table1', con=con, if_exists='replace')


def runQueries():
    for query in queries:
        start = datetime.datetime.now()
        result = cursor.execute(query)
        time.sleep(5)#can be removed. just added artificial timing to it.
        end = datetime.datetime.now()
        queryResult = result.fetchmany(5)
        time_elapsed = end - start
        file.write("Current query is: \"" + query + "\"\n")
        file.write("Time elapsed for the query is " + str(time_elapsed) + ":\n")
        for row in queryResult:
            file.write(str(row) + "\n")
        file.write("\n")




con = sqlite3.connect("amazonReviews4380.db")
cursor = con.cursor()
file = open("output.txt", "w")
file.write("Database Query Results:\n\n")
createDataFrames()
insertIntoTables()
runQueries()
con.close()
if os.path.exists("amazonReviews4380.db"):
    # remove the database file
    os.remove("amazonReviews4380.db")
print("Done executing")