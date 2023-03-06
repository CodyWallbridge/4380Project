import pandas as pd
import os
import numpy as np
from tqdm import tqdm

CHUNKSIZE = 100000 # Max read 100k rows at a time


def save_df(df, path):
    df = df.drop_duplicates()
    df = df.replace('"', '\'', regex=True)
    df = df.replace('\\', '|')
    df.to_csv(path, index=False, escapechar='\\')


def main():

    # marketplace_form = {
    #     # 'id': [],
    #     'marketplace': [], # name
    # }
    # marketplace_df = pd.DataFrame(marketplace_form)

    '''
    brand_df = {
        'id': [],
        'name': [],
    }
    '''

    customers_form = {
        'reviewerID': [], # replace with c_id
        'reviewerName': []
    }
    customers_df = pd.DataFrame(customers_form)

    products_form = {
        'asin': [], # replace with asin
        'category_id': [],
        # 'brand_id': [],
        # 'marketplace': [], # replace with marketplace_id
        # 'product_title': [],
        # 'price': [],
        # 'product_category': [],    
    }
    products_df = pd.DataFrame(products_form)

    reviews_form = {
        'asin': [],
        'reviewerID': [],
        # 'm_id': [],
        'unixReviewTime': [],
        'reviewText': [],
        'summary': [],
        'overall': [],
        'vote': [],
        
        # 'total_votes': [],
        # 'verified_purchase': [],
        # 'typo_count': [],
        # 'sentiment': [],
    }
    reviews_df = pd.DataFrame(reviews_form)

    to_skip = ['amazon_reviews_multilingual_US_v1_00.tsv']

    batch_size = 1
    runs_per_save = 50
    save_count = 0

    folder_path = os.path.abspath("E:/Amazon_Review_Dataset/Amazon Review Online")
    print(os.listdir(folder_path))
    i = 1

    file_num = 0
    category_names = []

    for file in os.listdir(folder_path):
        cat_name = file.split('.')[0].lower()
        if '_5' in cat_name:
            cat_name = cat_name[:-2]
        category_names.append(cat_name)
        if file in to_skip:
            continue
        print("Processing file: {}".format(file))
        csv_reader = pd.read_json(os.path.join(folder_path, file), lines=True, chunksize=CHUNKSIZE)
        for csv_chunk in tqdm(csv_reader):
            # marketplace_df = pd.concat([marketplace_df, csv_chunk[['marketplace']]])
            csv_chunk['category_id'] = file_num
            customers_df = pd.concat([customers_df, csv_chunk[['reviewerID', 'reviewerName']]])
            products_df = pd.concat([products_df, csv_chunk[['asin', 'category_id']]])
            reviews_df = pd.concat([reviews_df, csv_chunk[['asin', 'reviewerID', 'unixReviewTime', 'reviewText', 'summary', 'overall', 'vote']]])

            if i%runs_per_save == 0:
                save_df(customers_df, 'E:/Amazon_Review_Dataset/stage_1_dfs_nk/customers' + str(save_count) + '.csv')
                save_df(products_df, 'E:/Amazon_Review_Dataset/stage_1_dfs_nk/products' + str(save_count) + '.csv')
                reviews_df['vote'].fillna(0, inplace=True)
                save_df(reviews_df, 'E:/Amazon_Review_Dataset/stage_1_dfs_nk/reviews' + str(save_count) + '.csv')
                customers_df = pd.DataFrame(customers_form)
                products_df = pd.DataFrame(products_form)
                reviews_df = pd.DataFrame(reviews_form)
                save_count+=1
            i+=1
        file_num += 1

    categories_df = pd.DataFrame({'category_id': range(0, len(category_names)), 'category': category_names})
    # marketplace_df.to_csv('E:/Amazon_Review_Dataset/stage_1_dfs/marketplace' + str(i) + '.csv' , index=False)
    customers_df.to_csv('E:/Amazon_Review_Dataset/stage_1_dfs_nk/customers' + str(save_count) + '.csv', index=False)
    products_df.to_csv('E:/Amazon_Review_Dataset/stage_1_dfs_nk/products' + str(save_count) + '.csv', index=False)
    reviews_df['vote'].fillna(0, inplace=True)
    reviews_df.to_csv('E:/Amazon_Review_Dataset/stage_1_dfs_nk/reviews' + str(save_count) + '.csv', index=False)
    categories_df.to_csv('E:/Amazon_Review_Dataset/stage_1_dfs_nk/categories.csv', index=False)
    
if __name__ == '__main__':
    main()
