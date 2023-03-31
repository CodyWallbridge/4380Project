import sqlite3
import datetime
import os
import pandas as pd
import locale

verificationQueries = [
    "SELECT * FROM brands WHERE brand_name IS NULL OR brand_id IS NULL;",
    "SELECT * FROM categories WHERE category IS NULL OR category_id IS NULL;",
    "SELECT * FROM products WHERE asin IS NULL OR category_id IS NULL;",
    "SELECT * FROM products WHERE category_id NOT IN (SELECT category_id FROM categories) OR brand_id NOT IN (SELECT brand_id FROM brands);",
    "SELECT * FROM customers WHERE reviewerID IS NULL;"#,
    #"SELECT * FROM reviews WHERE reviewerID NOT IN (SELECT reviewerID FROM customers);"
]

codyQueries = [
    # 1 Find all customers who have reviewed products across at least 15 amazon product areas
    # (3:10)
    # add index on reviews.asin (done in 3, 4, 9)
    """SELECT r.reviewerID, COUNT(DISTINCT p.category_id) AS num_categories_reviewed
        FROM reviews r
        INNER JOIN products p ON r.asin = p.asin
        GROUP BY r.reviewerID
        HAVING COUNT(DISTINCT p.category_id) >= 15
        LIMIT 10;""",
    # 2 Find people who have reviewed at least 20 items in the tools_and_home_improvement area
    # (2:45)
    # add index on reviews.reviewerID (alreadyt done in 3 and 5)
    """SELECT r.reviewerID, COUNT(DISTINCT p.category_id) AS num_categories_reviewed
        FROM reviews r
        INNER JOIN products p ON r.asin = p.asin
        WHERE p.category_id = (SELECT category_id FROM categories WHERE category = 'tools_and_home_improvement')
        GROUP BY r.reviewerID
        HAVING COUNT(DISTINCT p.asin) >= 20
        LIMIT 10;""",
    # 3 Find products which haven't been bought by a specific customer, which were positively reviewed by other
    # customers who share an 80% review similarity score with that customer
    # (11 minutes)
    # add index on reviews.reviewerID and reviews.asin (both already done in query 5 and 4/9)
    """  SELECT other_reviewers.asin FROM reviews other_reviewers 
        WHERE other_reviewers.overall >= 3.0
        AND other_reviewers.reviewerID in (
        SELECT r2.reviewerID FROM reviews r1, reviews r2 
        WHERE r1.reviewerID = 'A1V6B6TNIC10QE' 
        AND r1.reviewerID <> r2.reviewerID 
        AND r1.asin = r2.asin 
            GROUP BY r2.reviewerID HAVING AVG(ABS(r1.overall - r2.overall)) <= 1.5)
            AND other_reviewers.asin IN(
                SELECT p.asin FROM products p 
                LEFT JOIN reviews r on p.asin = r.asin 
                WHERE r.reviewerID <> 'A1V6B6TNIC10QE' OR r.reviewerID is NULL)
        LIMIT 10;""",
    # 4 Get the 10 brands that have the highest average rating on their products
    # (6 minutes)
    # index on reviews.asin, sort reviews by price
    # (asin is already done in query 9. maybe just sort with analyzing change in other queries is enough)
    """SELECT b.brand_name, AVG(r.overall) AS avg_rating
        FROM brands b
        INNER JOIN products p ON b.brand_id = p.brand_id
        INNER JOIN reviews r ON p.asin = r.asin
        GROUP BY b.brand_name
        ORDER BY avg_rating ASC
        LIMIT 10;""",
    # 5 Find the most negative reviewers in the electronics category with at least 10 reviews
    # (45 mins reviews_1)
    # add index on reviews.reviewerId, sort reviews on average rating
    # (analyze whether the sorting changes any other queries)
    """SELECT b.brand_name, COUNT(p.asin) * 100.0 / (SELECT COUNT(*) FROM products WHERE overall > 4) AS percentage_above_4_star
    FROM brands b 
    JOIN products p ON b.brand_id = p.brand_id
    JOIN reviews r ON r.asin = p.asin
    WHERE r.overall > 4
    GROUP BY b.brand_name
    ORDER BY percentage_above_4_star DESC
    LIMIT 1;""",
    # 6 Find the top brand that has the highest proportion of reviews on their products as 5 stars
    # (43 mins reviews_1)
    # add index on reviews.overall, can analyze difference between B+ and hash
    """SELECT b.brand_name,
        COUNT(p.asin) * 100.0 / (SELECT COUNT(*) FROM products WHERE overall > 4) AS percentage_above_4_star
        FROM brands b
        JOIN products p ON b.brand_id = p.brand_id
        JOIN reviews r ON r.asin = p.asin
        WHERE r.overall > 4
        GROUP BY b.brand_name
        ORDER BY percentage_above_4_star DESC
        LIMIT 1;""",
    # 7 Find the 10 brands who have the highest standard deviation in their pricing
    # (26 seconds)
    # index on products.brand_id
    """SELECT b.brand_name, ROUND(SQRT(AVG((p.price - avg_price) * (p.price - avg_price))), 2) AS price_stdev
        FROM brands b
        JOIN products p ON b.brand_id = p.brand_id
        JOIN (SELECT brand_id, AVG(price) AS avg_price FROM products GROUP BY brand_id) AS avg_prices ON p.brand_id = avg_prices.brand_id
        GROUP BY b.brand_name
        ORDER BY price_stdev DESC
        LIMIT 10;""",
    # 8 Find the average price of the 5 most reviewed items in each product category
    # (4.5 days with 1 review did not complete)
    """SELECT c.category, AVG(p.price) as avg_price_top_5
        FROM categories c JOIN products p ON c.category_id = p.category_id
        WHERE p.asin IN (
            SELECT asin
            FROM reviews
            WHERE asin IN (SELECT asin FROM products WHERE category_id = p.category_id)
            GROUP BY asin
            ORDER BY count (*) DESC
            LIMIT 5)
        GROUP BY c.category;""",
    # 9 Get the average number of reviews for products in each product category on Amazon
    # (4 mins, index on reviews.asin and products.asin)
    """SELECT c.category, AVG(reviews_per_product) AS avg_reviews_per_product
        FROM categories c
        JOIN (
        SELECT category_id, p.asin, COUNT(*) AS reviews_per_product
        FROM products p
        JOIN reviews r ON r.asin = p.asin
        GROUP BY category_id, p.asin
        ) p ON c.category_id = p.category_id
        GROUP BY c.category;""",
    # 10 Get a weighted average of average product rating per each category (weighted on number of reviews in that category)
    # (1.75 days on 1 reviews and did not complete)
    """SELECT c.category, SUM(r.overall * r.weight) / SUM(r.weight) as avg_weighted_rating
        FROM categories c
        JOIN (
            SELECT asin, overall, COUNT(*) as weight
            FROM reviews
            GROUP BY asin, overall
        ) r ON asin IN (SELECT asin FROM products WHERE category_id = c.category_id)
        GROUP BY c.category;""",
    # 11 Get 10 brands with the highest proportion of items with price costing less than 90% of other products
    # (15 seconds)
    # sort products by price
    """SELECT b.brand_name,
        COUNT(p.asin) * 100.0 / (SELECT COUNT(*) FROM products) AS
        percentage_bottom_10_percent
        FROM brands b
        JOIN products p ON b.brand_id = p.brand_id
        WHERE p.price <= (SELECT price FROM products ORDER BY price LIMIT 1 OFFSET
            (SELECT COUNT(*) FROM products) / 10)
        GROUP BY b.brand_name
        ORDER BY percentage_bottom_10_percent DESC
        LIMIT 10;""",
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
    for i in range(0,1):

    #for i in range(0,6):

    #for i in range(0,32):
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

def testOptimizations():
    start = datetime.datetime.now()
    allTop5 = []
    print("starting runs")
    for i in range(0,29):
        innerQuery = """SELECT asin
                FROM reviews
                WHERE asin IN (SELECT asin FROM products WHERE category_id = """ + str(i) + """)
                GROUP BY asin
                ORDER BY count (*) DESC
                LIMIT 5;"""
        cursor.execute(innerQuery)
        result = cursor.fetchall()
        allTop5.append(result)
    for innerThing in allTop5:
        #print(innerThing)
        smallSet_str = "'" + "','".join(str(x[0]) for x in innerThing) + "'"
        query = """SELECT c.category, AVG(p.price) as avg_price_top_5
            FROM categories c JOIN products p ON c.category_id = p.category_id
            WHERE p.asin IN ({})
            GROUP BY c.category;""".format(smallSet_str)
        cursor.execute(query)
        queryResult = cursor.fetchall()
        for row in queryResult:
            print(str(row) + "\n")
    
    end = datetime.datetime.now()
    time_elapsed = end - start
    print("Query took " + str(time_elapsed) + " to execute")

    


def runQueries():
    # (1 indexed)
    # reviews1: 6, 8, 10
    # reviews5: 5
    
    # for full tables
    #indexes = [0,1,2,3,6,8,10]

    # for 5 tables
    #indexes = []

    # for 1 table
    #indexes = [4,5,7,9]
    indexes = [4,5]
    ogStart = datetime.datetime.now()
    print("Doing queries")
    for i in indexes:
        query = codyQueries[i]
        start = datetime.datetime.now()
        result = cursor.execute(query)
        queryResult = result.fetchall()
        end = datetime.datetime.now()
        time_elapsed = end - start
        print("Current query index is " + str(i) + " is: \"" + query + "\"\n")
        print("Time elapsed for the query is " + str(time_elapsed) + ":\n\n")
        # file.write("Current query index is " + str(i) + "is: \"" + query + "\"\n")
        # file.write("Time elapsed for the query is " + str(time_elapsed) + ":\n\n")
        # this line can be used for the later report where we print the 5 rows to output.txt
        # for row in queryResult:
        #     file.write(str(row) + "\n")
        # file.write("\n")
    actuallyDone = datetime.datetime.now()
    allTime = actuallyDone - ogStart
    print("All queries together took " + str(allTime))

def showTables():
    #prints the tables, their columns and the row count (just to be safe for now)
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

    #prints the tables and their columns
    # result = cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    # tables = result.fetchall()
    # for table in tables:
    #     table_name = table[0]
    #     result = cursor.execute(f"PRAGMA table_info('{table_name}');")
    #     columns = result.fetchall()
    #     column_names = [col[1] for col in columns]
    #     print(f"Table: {table_name}, {' '.join(column_names)}")

def checkData():
    print("Doing verification queries")
    i = 1
    for query in verificationQueries:
        start = datetime.datetime.now()
        print("executing query: " + str(i))
        result = cursor.execute(query)
        queryResult = result.fetchall()
        end = datetime.datetime.now()
        time_elapsed = end - start
        file.write("Current query is: \"" + query + "\"\n")
        file.write("Time elapsed for the query is " + str(time_elapsed) + ":\n\n")
        # this line can be used for the later report where we print the 5 rows to output.txt
        for row in queryResult:
            file.write(str(row) + "\n")
        file.write("\n")
        i += 1

def checkSelectStar():
    selectQueries = [
        "SELECT * FROM brands;",
        "SELECT * FROM categories;",
        "SELECT * FROM customers;",
        "SELECT * FROM products;",
        "SELECT * FROM reviews;"
    ]
    print("Doing select * queries")
    i = 1
    for query in selectQueries:
        start = datetime.datetime.now()
        print("executing query: " + str(i))
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
        i += 1

dbExists = True
if os.path.exists("amazonReviews4380.db") is False:
    dbExists = False
con = sqlite3.connect("amazonReviews4380.db")
cursor = con.cursor()
if dbExists is False:
    populateTables()
file = open("output.txt", "w")
file.write("Database Query Results:\n\n")
#showTables()

#checkData()
#checkSelectStar()

#testOptimizations()
runQueries()
con.close()
print("Done executing")