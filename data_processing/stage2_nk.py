import pandas as pd
import os 


def process_customers():
    folder_path = os.path.abspath("E:/Amazon_Review_Dataset/stage_1_dfs_nk")
    print(os.listdir(folder_path))

    customers_form = {
        'reviewerID': [], # replace with c_id
        'reviewerName': []
    }
    customers_df = pd.DataFrame(customers_form)
    for file in os.listdir(folder_path):
        if 'customers' in file:
            print("Processing file: {}".format(file))
            df = pd.read_csv(os.path.join(folder_path, file))
            # append df to customers_df, remove duplicates  
            customers_df = pd.concat([customers_df, df])
    customers_df.drop_duplicates(inplace=True)
    customers_df.to_csv("E:/Amazon_Review_Dataset/stage_2_dfs_nk/customers.csv", index=False)


def process_products_fill():
    folder_path = os.path.abspath("E:/Amazon_Review_Dataset/stage_1_dfs_nk")
    print(os.listdir(folder_path))

    products_form = {
        'asin': [], # replace with asin
        'category_id': [],
        # 'brand_id': [],
        # 'marketplace': [], # replace with marketplace_id
        # 'product_title': [],
        # 'price': [],
        # 'product_category': [],    
    }
    product_df = pd.DataFrame(products_form)
    for file in os.listdir(folder_path):
        if 'product' in file:
            print("Processing file: {}".format(file))
            df = pd.read_csv(os.path.join(folder_path, file))
            # append df to customers_df, remove duplicates  
            product_df = pd.concat([product_df, df])
    product_df.drop_duplicates(inplace=True)
    product_df.to_csv("E:/Amazon_Review_Dataset/stage_2_dfs_nk/products.csv", index=False)


def main():
    process_customers()
    process_products_fill()
    # process_reviews()




if __name__ == '__main__':
    main()

