import sqlite3
import datetime
import os
import time
import pandas as pd

files = [
    "names.csv",
    "table1.csv",
    "table2.csv"
]

queries = [
    "SELECT * FROM names ORDER BY Id",
    "SELECT * FROM tableName ORDER BY Id"
]


def populateTables():
    dfNames = pd.read_csv("names.csv")
    dfNames.to_sql('names', con=con, if_exists='replace')

    dfTable = pd.read_csv("table1.csv")
    dfTable.to_sql('tableName', con=con, if_exists='replace')
    dfTable = pd.read_csv("table2.csv")
    dfTable.to_sql('tableName', con=con, if_exists='append')

def runQueries():
    for query in queries:
        start = datetime.datetime.now()
        print("executing query: " + query)
        result = cursor.execute(query)
        time.sleep(1)#can be removed. just added artificial timing to it.
        end = datetime.datetime.now()
        #queryResult = result.fetchmany(5)
        queryResult = result.fetchall()
        time_elapsed = end - start
        file.write("Current query is: \"" + query + "\"\n")
        file.write("Time elapsed for the query is " + str(time_elapsed) + ":\n")
        for row in queryResult:
            file.write(str(row) + "\n")
        file.write("\n")



try:
    if os.path.exists("amazonReviews4380.db"):
        # remove the database file if it exists so we are definitely getting a clean start
        os.remove("amazonReviews4380.db")
    con = sqlite3.connect("amazonReviews4380.db")
    cursor = con.cursor()
    file = open("output.txt", "w")
    file.write("Database Query Results:\n\n")
    populateTables()
    runQueries()
    print("Done executing")

finally:
    con.close()