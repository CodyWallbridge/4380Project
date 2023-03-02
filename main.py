import sqlite3

def createFiles():
    cursor.execute("CREATE TABLE movie(title, year, score)")
    con.commit()


def insertIntoFiles():
    cursor.execute("""
    INSERT INTO movie VALUES
        ('Monty Python and the Holy Grail', 1975, 8.2),
        ('And Now for Something Completely Different', 1971, 7.5)
""")
    con.commit()

def runQueries():
    result = cursor.execute("SELECT score FROM movie")
    result.fetchall()




con = sqlite3.connect("amazonReviews4380.db")
createFiles()
insertIntoFiles()
runQueries()


cursor = con.cursor()
result = cursor.execute("SELECT ....") # run whatever query you want
cursor.execute("UPDATE ....") # update whatever table you want
con.commit() # commits are necessary for every change

# BATCH COMMIT 
data = [
    ("Monty Python Live at the Hollywood Bowl", 1982, 7.9),
    ("Monty Python's The Meaning of Life", 1983, 7.5),
    ("Monty Python's Life of Brian", 1979, 8.0),
]
cur.executemany("INSERT INTO movie VALUES(?, ?, ?)", data)
con.commit()  # Remember to commit the transaction after executing INSERT.
