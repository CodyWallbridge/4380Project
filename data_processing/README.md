NOTE: Data processing code made some use of Github Copilot under the permission of Adam Pazdor.  

# Stage 1
Read through the raw data, parse it into tables.  
Things slow down as the tables get large, create a table for every 5. 

# Stage 2 
Aggregate individual tables (not reviews, they are too large and completed), create primary keys, and link tables together.  

# Stage 3
Expand the products table using the json data.  
Create brand table relevant to products.  
