import sqlite3
import csv
import datetime
import os
import time


def createTables():
    cursor.execute("CREATE TABLE names(id, name, word)")
    cursor.execute("CREATE TABLE table1(id, letter, newWord)")
    con.commit()


def insertIntoTables():
    with open('names.csv','r') as file:
        namesReader = csv.DictReader(file) # comma is default delimiter
        to_db = [(i['Id'], i['Name'], i['Word']) for i in namesReader]

        cursor.executemany("INSERT INTO names (Id, Name, Word) VALUES (?, ?, ?);", to_db)
        con.commit()


    with open('table1.csv','r') as file:
        table1Reader = csv.DictReader(file) # comma is default delimiter
        to_db = [(i['Id'], i['letter'], i['newWord']) for i in table1Reader]

        cursor.executemany("INSERT INTO table1 (Id, letter, newWord) VALUES (?, ?, ?);", to_db)
        con.commit()


def runQueries():
    query = "SELECT * FROM names ORDER BY Id LIMIT 5;"
    start = datetime.datetime.now()
    result = cursor.execute(query)
    time.sleep(5)#can be removed. just added artificial timing to it.
    end = datetime.datetime.now()
    queryResult = result.fetchall()
    time_elapsed = end - start
    file.write("Current query is: \"" + query + "\"\n")
    file.write("Time elapsed for the query is " + str(time_elapsed) + ":\n")
    #print("Time elapsed is " + str(time_elapsed) + ":")
    #print("names result:")
    for row in queryResult:
        file.write(str(row) + "\n")
        #print(row)

    file.write("\n")

    query = "SELECT * FROM table1 ORDER BY Id LIMIT 5;"
    start = datetime.datetime.now()
    result = cursor.execute(query)
    time.sleep(5)#can be removed. just added artificial timing to it.
    end = datetime.datetime.now()
    queryResult = result.fetchall()
    time_elapsed = end - start
    file.write("Current query is: \"" + query + "\"\n")
    file.write("Time elapsed for the query is " + str(time_elapsed) + ":\n")
    #print("Time elapsed is " + str(time_elapsed) + ":")
    #print("table1:")
    for row in queryResult:
        file.write(str(row) + "\n")
        #print(row)




con = sqlite3.connect("amazonReviews4380.db")
cursor = con.cursor()
file = open("output.txt", "w")
file.write("Database Query Results:\n\n")
createTables()
insertIntoTables()
runQueries()
con.close()
if os.path.exists("amazonReviews4380.db"):
    # remove the database file
    os.remove("amazonReviews4380.db")
else:
    print(f"The file amazonReviews4380.db does not exist.")
print("Done executing")