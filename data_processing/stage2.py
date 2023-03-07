import pandas as pd
import os 


def process_customers():
    folder_path = os.path.abspath("E:/Amazon_Review_Dataset/stage_1_dfs")
    print(os.listdir(folder_path))

    customers_form = {
        'customer_id': [], # replace with c_id
    }
    customers_df = pd.DataFrame(customers_form)
    for file in os.listdir(folder_path):
        if 'customers' in file:
            print("Processing file: {}".format(file))
            df = pd.read_csv(os.path.join(folder_path, file))
            # append df to customers_df, remove duplicates  
            customers_df = pd.concat([customers_df, df])
    customers_df.drop_duplicates(inplace=True)
    customers_df.to_csv("E:/Amazon_Review_Dataset/stage_2_dfs/customers.csv", index=False)


def process_marketplace():
    folder_path = os.path.abspath("E:/Amazon_Review_Dataset/stage_1_dfs")
    print(os.listdir(folder_path))

    marketplace_form = {
        'market_id': [],
        'marketplace': [], # name
    }
    marketplace_df = pd.DataFrame(marketplace_form)
    for file in os.listdir(folder_path):
        if 'marketplace' in file:
            print("Processing file: {}".format(file))
            df = pd.read_csv(os.path.join(folder_path, file))
            # append df to customers_df, remove duplicates  
            marketplace_df = pd.concat([marketplace_df, df])
    marketplace_df.drop_duplicates(inplace=True)
    for idx, _ in marketplace_df.iterrows():
        marketplace_df.at[idx, 'market_id'] = idx
    marketplace_df.to_csv("E:/Amazon_Review_Dataset/stage_2_dfs/marketplaces.csv", index=False)


def process_marketplace():
    folder_path = os.path.abspath("E:/Amazon_Review_Dataset/stage_1_dfs")
    print(os.listdir(folder_path))

    marketplace_form = {
        'market_id': [],
        'marketplace': [], # name
    }
    marketplace_df = pd.DataFrame(marketplace_form)
    for file in os.listdir(folder_path):
        if 'marketplace' in file:
            print("Processing file: {}".format(file))
            df = pd.read_csv(os.path.join(folder_path, file))
            # append df to customers_df, remove duplicates  
            marketplace_df = pd.concat([marketplace_df, df])
    marketplace_df.drop_duplicates(inplace=True)
    for idx, _ in marketplace_df.iterrows():
        marketplace_df.at[idx, 'market_id'] = idx
    marketplace_df.to_csv("E:/Amazon_Review_Dataset/stage_2_dfs/marketplaces.csv", index=False)


def process_products_fill():
    folder_path = os.path.abspath("E:/Amazon_Review_Dataset/stage_1_dfs")
    print(os.listdir(folder_path))

    products_form = {
        'product_id': [], # replace with asin
        'product_parent': [], 
        # 'brand_id': [],
        'marketplace': [], # replace with marketplace_id
        'product_title': [],
        # 'price': [],
        'product_category': [],    
    }
    product_df = pd.DataFrame(products_form)
    for file in os.listdir(folder_path):
        if 'product' in file:
            print("Processing file: {}".format(file))
            df = pd.read_csv(os.path.join(folder_path, file))
            # append df to customers_df, remove duplicates  
            product_df = pd.concat([product_df, df])
    product_df.drop_duplicates(inplace=True)
    categories_df = product_df['product_category'].unique()
    categories_df = pd.DataFrame(categories_df)
    categories_df['category_id'] = [i for i in range(len(categories_df))]
    categories_df.columns = ['category', 'category_id']
    categories_df.to_csv("E:/Amazon_Review_Dataset/stage_2_dfs/categories.csv", index=False)

    merged_df = pd.merge(product_df, categories_df, how='left', left_on='product_category', right_on='category')
    merged_df['product_category'] = merged_df['product_id']
    merged_df.drop(['product_category', 'category'], axis=1, inplace=True)
    
    marketplaces_df = pd.read_csv("E:/Amazon_Review_Dataset/stage_2_dfs/marketplaces.csv")
    merged_df = pd.merge(merged_df, marketplaces_df, how='left', left_on='marketplace', right_on='marketplace')
    merged_df['marketplace'] = merged_df['market_id']
    merged_df.drop(['marketplace'], axis=1, inplace=True)
    merged_df.to_csv("E:/Amazon_Review_Dataset/stage_2_dfs/products.csv", index=False)

def process_reviews():
    folder_path = os.path.abspath("E:/Amazon_Review_Dataset/stage_1_dfs")
    print(os.listdir(folder_path))

    reviews_form = {
        'product_id': [],
        'customer_id': [],
        # 'm_id': [],
        'review_date': [],
        'review_body': [],
        'star_rating': [],
        'helpful_votes': [],
        'total_votes': [],
        'verified_purchase': [],
        # 'typo_count': [],
        # 'sentiment': [],
    }
    reviews_df = pd.DataFrame(reviews_form)

    for file in os.listdir(folder_path):
        if 'review' in file:
            print("Processing file: {}".format(file))
            df = pd.read_csv(os.path.join(folder_path, file))
            # append df to customers_df, remove duplicates  
            reviews_df = pd.concat([reviews_df, df])
    reviews_df.drop_duplicates(inplace=True)
    reviews_df.to_csv("E:/Amazon_Review_Dataset/stage_2_dfs/reviews.csv", index=False)

def main():
    # process_customers()
    # process_marketplace()
    process_products_fill()
    # process_reviews()




if __name__ == '__main__':
    main()

