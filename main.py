import sqlite3
import datetime
import os
import pandas as pd
import locale

codyQueries = [
    #find all customers who have reviewed 10 different kinds of a category such as tols_and_home_improvement
    """SELECT r.reviewerID, COUNT(DISTINCT r.category_id) AS num_categories_reviewed
    FROM reviews r
    INNER JOIN products p ON r.asin = p.asin
    WHERE p.category_id = (SELECT category_id FROM categories WHERE category = 'tools_and_home_improvement')
    GROUP BY r.reviewerID
    HAVING COUNT(DISTINCT r.category_id) >= 10;""",
    #find products which haven't been bought by a customer which was positively reviewed by other customers who share
    # an 80% review similarity score with customer X, sorted on customer count
    """SELECT p.asin, p.title, COUNT(DISTINCT r.reviewerID) AS num_customers
    FROM products p
    INNER JOIN reviews r ON p.asin = r.asin
    WHERE p.asin NOT IN (
    SELECT p2.asin
    FROM products p2
    INNER JOIN reviews r2 ON p2.asin = r2.asin
    WHERE r2.reviewerID = 'X' AND r2.overall > 3
    )
    AND r.reviewerID IN (
    SELECT r2.reviewerID
    FROM reviews r2
    WHERE r2.reviewerID != 'X' AND r2.overall > 3
    GROUP BY r2.reviewerID
    HAVING SIMILARITY(r.reviewText, (SELECT reviewText FROM reviews WHERE reviewerID = 'X' AND overall > 3)) >= 0.8
    )
    GROUP BY p.asin, p.title
    ORDER BY num_customers DESC;""",
    #Which brand has the highest average star rating for its products?
    """SELECT b.brand_name, AVG(r.overall) AS avg_rating
    FROM brands b
    INNER JOIN products p ON b.brand_id = p.brand_id
    INNER JOIN reviews r ON p.asin = r.asin
    GROUP BY b.brand_name
    ORDER BY avg_rating DESC
    LIMIT 1;""",
    #Which customers have given the highest average star rating to products in a specific category?
    """SELECT r.reviewerID, AVG(r.overall) AS avg_rating
    FROM reviews r
    INNER JOIN products p ON r.asin = p.asin
    WHERE p.category_id = (SELECT category_id FROM categories WHERE category = 'specific_category')
    GROUP BY r.reviewerID
    ORDER BY avg_rating DESC
    LIMIT 1;""",
    # Which brand has the highest percentage of products with a star rating above 4?
    """SELECT b.brand_name, 
       COUNT(p.asin) * 100.0 / (SELECT COUNT(*) FROM products WHERE overall > 4) AS percentage_above_4_star
    FROM brands b 
    JOIN products p ON b.brand_id = p.brand_id
    JOIN reviews r ON r.asin = p.asin
    WHERE r.overall > 4
    GROUP BY b.brand_name
    ORDER BY percentage_above_4_star DESC
    LIMIT 1;""",
    # Which brand has the highest standard deviation in product prices?
    """SELECT b.brand_name, STDDEV(p.price) AS std_dev_price
    FROM brands b 
    JOIN products p ON b.brand_id = p.brand_id
    GROUP BY b.brand_name
    ORDER BY std_dev_price DESC
    LIMIT 1;""",
    # What is the average price of the top 5 most reviewed products in each category?
    """SELECT c.category, 
        AVG(p.price) as avg_price_top_5
    FROM categories c
    JOIN products p ON c.category_id = p.category_id
    WHERE p.asin IN (
    SELECT asin 
    FROM reviews 
    WHERE asin IN (SELECT asin FROM products WHERE category_id = p.category_id) 
    ORDER BY overall DESC 
    LIMIT 5
    )
    GROUP BY c.category;""",
    # For each category, what is the average number of reviews per product?
    """SELECT c.category, AVG(reviews_per_product) AS avg_reviews_per_product
    FROM categories c
    JOIN (
    SELECT category_id, asin, COUNT(*) AS reviews_per_product
    FROM products p
    JOIN reviews r ON r.asin = p.asin
    GROUP BY category_id, asin
    ) p ON c.category_id = p.category_id
    GROUP BY c.category;""",
    # What is the average rating of products in each category, weighted by the number of reviews?
        """SELECT c.category, 
        SUM(r.overall * r.weight) / SUM(r.weight) as avg_weighted_rating
    FROM categories c
    JOIN (
    SELECT asin, overall, COUNT(*) as weight
    FROM reviews 
    GROUP BY asin, overall
    ) r ON r.asin IN (SELECT asin FROM products WHERE category_id = c.category_id)
    GROUP BY c.category;""",
    # Which brand has the highest percentage of products with a price in the bottom 10% of all products?
    """SELECT b.brand_name, 
        COUNT(p.asin) * 100.0 / (SELECT COUNT(*) FROM products) AS percentage_bottom_10_percent
    FROM brands b 
    JOIN products p ON b.brand_id = p.brand_id
    WHERE p.price <= (
    SELECT PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY price) FROM products
    )
    GROUP BY b.brand_name
    ORDER BY percentage_bottom_10_percent DESC
    LIMIT 1;""",
    # What is the average helpful votes count for reviews in each category?
    """SELECT c.category, 
        SUM(p.price * r.weight) / SUM(r.weight) as avg_weighted_price
    FROM categories c
    JOIN (
    SELECT asin, price, COUNT(*) as weight
    FROM reviews 
    JOIN products p ON p.asin = reviews.asin
    GROUP BY asin, price
    ) r ON r.asin IN (SELECT asin FROM products WHERE category_id = c.category_id)
    JOIN products p ON p.asin = r.asin
    GROUP BY c.category;""",
    # What is the average price of products in each category, weighted by the number of reviews?
    """SELECT b.brand_name, c.category, 
        AVG(reviews_per_product) AS avg_reviews_per_product
    FROM brands b
    JOIN products p ON b.brand_id = p.brand_id
    JOIN categories c ON c.category_id = p.category_id
    JOIN (
    SELECT category_id, asin, COUNT(*) AS reviews_per_product
    FROM reviews r
    JOIN products p ON r.asin = p.asin
    GROUP BY category_id, asin
    ) p ON p.asin = r.asin AND p.category_id = c.category_id
    GROUP BY b.brand_name, c.category;"""
]

chrisQueries = [

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


def codyQueries():
    for query in codyQueries:
        start = datetime.datetime.now()
        print("executing query: " + query)
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


dbExists = True
if os.path.exists("amazonReviews4380.db") is False:
    dbExists = False
con = sqlite3.connect("amazonReviews4380.db")
cursor = con.cursor()
if dbExists is False:
    populateTables()
file = open("output.txt", "w")
file.write("Database Query Results:\n\n")
codyQueries()
chrisQueries()
con.close()
print("Done executing")