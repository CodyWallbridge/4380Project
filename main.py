import sqlite3
import datetime
import os
import pandas as pd
import locale

queries = [
    "SELECT * FROM brands;",
    "SELECT * FROM categories;",
    "SELECT * FROM customers;",
    "SELECT * FROM products;",
    "SELECT * FROM reviews;"
]

def remove_comma_or_period(x):
    if isinstance(x, str):
        x = x.replace(',', '')
        locale.setlocale(locale.LC_NUMERIC, 'en_US.UTF-8')
        x = locale.atof(x)
        return int(x)
    else:
        return x

def populateTables():
    start = datetime.datetime.now()

    data_types = {
        'brand_id': 'INTEGER PRIMARY KEY',
        'brand_name': 'TEXT'
    }
    print("handling brands")
    dfBrands = pd.read_csv("complete_data_36gb/brands.csv")
    dfBrands = dfBrands.reset_index(drop=True)
    dfBrands.to_sql('brands', con=con, if_exists='replace', index=False, dtype=data_types)

    data_types = {
        'category_id': 'INTEGER PRIMARY KEY',
        'category': 'TEXT'
    }
    print("handling categories")
    dfCategories = pd.read_csv("complete_data_36gb/categories.csv")
    dfCategories = dfCategories.reset_index(drop=True)
    dfCategories.to_sql('categories', con=con, if_exists='replace', index=False, dtype=data_types)
    
    data_types = {
        'reviewerID': 'TEXT PRIMARY KEY',
        'reviewerName': 'TEXT'
    }
    print("handling customers")
    dfCustomers = pd.read_csv("complete_data_36gb/customers.csv")

    dfCustomers = dfCustomers.drop_duplicates(subset=['reviewerID'], keep='first')
    dfCustomers = dfCustomers.reset_index(drop=True)
    dfCustomers.to_sql('customers', con=con, if_exists='replace', index=False, dtype=data_types)
    
    data_types = {
        'asin': 'TEXT PRIMARY KEY',
        'category_id': 'INTEGER',
        'title': 'TEXT',
        'description': 'TEXT',
        'price': 'REAL',
        'brand_id': 'INTEGER'
    }
    print("handling products")
    dfProducts = pd.read_csv("complete_data_36gb/products.csv")
    dfProducts = dfProducts.drop_duplicates(subset=['asin'], keep='first')
    dfProducts = dfProducts.reset_index(drop=True)
    dfProducts.to_sql('products', con=con, if_exists='replace', index=False, dtype=data_types)

    data_types = {
        'asin': 'TEXT',
        'reviewerID': 'TEXT',
        'unixReviewTime': 'INTEGER',
        'reviewText': 'TEXT',
        'summary': 'TEXT',
        'overall': 'INTEGER',
        'vote': 'TEXT'
    }
    primary_key = ['asin', 'reviewerID']
    for i in range(0,32):
        print("handling reviews" + str(i))
        dfReviews = pd.read_csv("complete_data_36gb/reviews" + str(i) + ".csv", dtype={'asin': 'string', 'reviewerID': 'string', 'unixReviewTime': 'int64', 'reviewText': 'string', 'summary': 'string', 'overall': 'int64', 'vote': 'string'})
        dfReviews['vote'] = dfReviews['vote'].apply(remove_comma_or_period)
        dfReviews = dfReviews.reset_index(drop=True)

        if i == 0:
            dfReviews.to_sql('reviews', con=con, if_exists='replace', index=False, dtype=data_types, index_label=primary_key)
        else:
            dfReviews.to_sql('reviews', con=con, if_exists='append', index=False, dtype=data_types, index_label=primary_key)

    end = datetime.datetime.now()
    time_elapsed = end - start
    print("\n\n\nTime elapsed for the setup is " + str(time_elapsed))  
     
    result = cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = result.fetchall()
    for table in tables:
        table_name = table[0]
        result = cursor.execute(f"PRAGMA table_info('{table_name}');")
        columns = result.fetchall()
        column_names = [col[1] for col in columns]

        result = cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        row_count = result.fetchone()[0]

        print(f"Table: {table_name}, Columns: {' '.join(column_names)}, Rows: {row_count}")


def runQueries():
    for query in queries:
        start = datetime.datetime.now()
        print("executing query: " + query)
        result = cursor.execute(query)
        queryResult = result.fetchall()
        end = datetime.datetime.now()
        time_elapsed = end - start
        file.write("Current query is: \"" + query + "\"\n")
        file.write("Time elapsed for the query is " + str(time_elapsed) + ":\n\n")
        # this line can be used for the later report where we print the 5 rows to output.txt
        # for row in queryResult:
        #     file.write(str(row) + "\n")
        # file.write("\n")


dbExists = True
if os.path.exists("amazonReviews4380.db") is False:
    print("db dont exist")
    dbExists = False
con = sqlite3.connect("amazonReviews4380.db")
cursor = con.cursor()
if dbExists is False:
    populateTables()
file = open("output.txt", "w")
file.write("Database Query Results:\n\n")
runQueries()
con.close()
print("Done executing")