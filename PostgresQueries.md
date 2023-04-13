
Query 1:
SELECT r."reviewerID", COUNT(DISTINCT p."category_id") AS num_categories_reviewed
FROM "reviews" r
INNER JOIN "products" p ON r."asin" = p."asin"
GROUP BY r."reviewerID"
HAVING COUNT(DISTINCT p."category_id") >= 15
LIMIT 10;

Query 2: 
SELECT r."reviewerID", COUNT(DISTINCT p."category_id") AS "num_categories_reviewed"
FROM public.reviews r
INNER JOIN public.products p ON r."asin" = p."asin"
WHERE p."category_id" = (SELECT "category_id" FROM public.categories WHERE "category" = 'tools_and_home_improvement')
GROUP BY r."reviewerID"
HAVING COUNT(DISTINCT p."asin") >= 20
LIMIT 10;

Query 3:
SELECT other_reviewers."asin" 
FROM reviews other_reviewers 
WHERE other_reviewers."overall" >= 2.0 
AND other_reviewers."reviewerID" IN (
    SELECT r2."reviewerID" 
    FROM reviews r1, reviews r2 
    WHERE r1."reviewerID" = 'A1V6B6TNIC10QE' 
    AND r1."reviewerID" <> r2."reviewerID" 
    AND r1."asin" = r2."asin"
    GROUP BY r2."reviewerID" HAVING AVG(ABS(r1."overall" - r2."overall")) <= 2.5
)
AND other_reviewers."asin" IN (
    SELECT p."asin" 
    FROM products p 
    LEFT JOIN reviews r ON p."asin" = r."asin" 
    WHERE r."reviewerID" <> 'A1V6B6TNIC10QE' OR r."reviewerID" IS NULL
)
LIMIT 10;

Query 4:
SELECT b."brand_name", AVG(r.overall) AS avg_rating
FROM brands b
INNER JOIN products p ON b.brand_id = p.brand_id
INNER JOIN reviews r ON p.asin = r.asin
GROUP BY b."brand_name"
ORDER BY avg_rating ASC
LIMIT 10;


Query 5:
SELECT r."reviewerID", AVG(r."overall") AS avg_rating
    FROM reviews r
    INNER JOIN products p ON r."asin" = p."asin"
    WHERE p."category_id" = (SELECT "category_id" FROM categories WHERE "category" = 'electronics')
    GROUP BY r."reviewerID" HAVING count(*) >= 10
    ORDER BY avg_rating ASC
    LIMIT 10;


Query 6:
SELECT b.brand_name,
        COUNT(p.asin) * 100.0 / (SELECT COUNT(*) FROM products) AS percentage_above_4_star
        FROM brands b
        JOIN products p ON b.brand_id = p.brand_id
        JOIN reviews r ON r.asin = p.asin
        WHERE r.overall > 4
        GROUP BY b.brand_name
        ORDER BY percentage_above_4_star DESC
        LIMIT 1;


Query 7:
SELECT b.brand_name, ROUND(SQRT(AVG(CAST((p.price - avg_prices.avg_price) * (p.price - avg_prices.avg_price) AS NUMERIC))), 2) AS price_stdev
        FROM brands b
        JOIN products p ON b.brand_id = p.brand_id
        JOIN (SELECT brand_id, AVG(price) AS avg_price FROM products GROUP BY brand_id) AS avg_prices ON p.brand_id = avg_prices.brand_id
        GROUP BY b.brand_name
        ORDER BY price_stdev DESC
        LIMIT 10;

Query 9:
SELECT c.category, AVG(reviews_per_product) AS avg_reviews_per_product
FROM categories c
JOIN (
    SELECT category_id, p.asin, COUNT(*) AS reviews_per_product
    FROM products p
    JOIN reviews r ON r.asin = p.asin
    GROUP BY category_id, p.asin
) p ON c.category_id = p.category_id
GROUP BY c.category;


Query 11:
SELECT b."brand_name",
       COUNT(p."asin") * 100.0 / (SELECT COUNT(*) FROM products) AS percentage_bottom_10_percent
FROM brands b
JOIN products p ON b."brand_id" = p."brand_id"
WHERE p."price" <= (SELECT "price" FROM products ORDER BY "price" LIMIT 1 OFFSET (SELECT COUNT(*) FROM products) / 10)
GROUP BY b."brand_name"
ORDER BY percentage_bottom_10_percent DESC
LIMIT 10;

