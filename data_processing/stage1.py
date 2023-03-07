import pandas as pd
import os
import numpy as np
from tqdm import tqdm

CHUNKSIZE = 100000 # Max read 100k rows at a time


def save_df(df, path):
    df = df.drop_duplicates()
    df.to_csv(path, index=False)


def main():

    marketplace_form = {
        # 'id': [],
        'marketplace': [], # name
    }
    marketplace_df = pd.DataFrame(marketplace_form)

    '''
    brand_df = {
        'id': [],
        'name': [],
    }
    '''

    customers_form = {
        'customer_id': [], # replace with c_id
    }
    customers_df = pd.DataFrame(customers_form)

    products_form = {
        'product_id': [], # replace with asin
        # 'brand_id': [],
        'marketplace': [], # replace with marketplace_id
        'product_title': [],
        # 'price': [],
        'product_category': [],    
    }
    products_df = pd.DataFrame(products_form)

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

    to_skip = ['amazon_reviews_multilingual_US_v1_00.tsv']

    batch_size = 5

    folder_path = os.path.abspath("E:/Amazon_Review_Dataset/amazon review")
    print(os.listdir(folder_path))
    i = 0
    for file in os.listdir(folder_path):
        if file in to_skip:
            continue
        print("Processing file: {}".format(file))
        csv_reader = pd.read_table(os.path.join(folder_path, file), delimiter='\t', quoting=3, chunksize=CHUNKSIZE, low_memory=False)
        for csv_chunk in tqdm(csv_reader):
            marketplace_df = pd.concat([marketplace_df, csv_chunk[['marketplace']]])
            customers_df = pd.concat([customers_df, csv_chunk[['customer_id']]])
            products_df = pd.concat([products_df, csv_chunk[['product_id', 'product_parent', 'marketplace', 'product_title', 'product_category']]])
            reviews_df = pd.concat([reviews_df, csv_chunk[['product_id', 'customer_id', 'marketplace', 'review_date', 'review_body', 'star_rating', 'helpful_votes', 'total_votes', 'verified_purchase']]])
        if i%batch_size == 0 and i != 0:
            save_df(marketplace_df, 'E:/Amazon_Review_Dataset/stage_1_dfs/marketplace' + str(i) + '.csv')
            save_df(customers_df, 'E:/Amazon_Review_Dataset/stage_1_dfs/customers' + str(i) + '.csv')
            save_df(products_df, 'E:/Amazon_Review_Dataset/stage_1_dfs/products' + str(i) + '.csv')
            save_df(reviews_df, 'E:/Amazon_Review_Dataset/stage_1_dfs/reviews' + str(i) + '.csv')
            marketplace_df = pd.DataFrame(marketplace_form)
            customers_df = pd.DataFrame(customers_form)
            products_df = pd.DataFrame(products_form)
            reviews_df = pd.DataFrame(reviews_form)
        i+=1

    marketplace_df.to_csv('E:/Amazon_Review_Dataset/stage_1_dfs/marketplace' + str(i) + '.csv' , index=False)
    customers_df.to_csv('E:/Amazon_Review_Dataset/stage_1_dfs/customers' + str(i) + '.csv', index=False)
    products_df.to_csv('E:/Amazon_Review_Dataset/stage_1_dfs/products' + str(i) + '.csv', index=False)
    reviews_df.to_csv('E:/Amazon_Review_Dataset/stage_1_dfs/reviews' + str(i) + '.csv', index=False)
    
if __name__ == '__main__':
    main()
