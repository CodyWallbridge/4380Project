# ChatGPT really helped out with this data cleaning code. 

import pandas as pd
import json
from tqdm import tqdm
import math
import os 
import polars as pl


# Define a function to read the metadata JSON file in chunks and extract the desired attributes
def read_metadata(filename, chunksize, desired_values):
    progress_bar = tqdm()
    with open(filename, 'r') as f:
        while True:
            progress_bar.update(1)
            data = []
            for i in range(chunksize):
                line = f.readline()
                if not line:
                    break
                item = json.loads(line)
                product_id = item.get('asin')
                brand = item.get('brand')
                price = item.get('price')
                if product_id and brand and price:
                    data.append({'product_id': product_id, 'brand': brand, 'price': price})
            if not data:
                progress_bar.close()
                break
            yield pd.DataFrame(data)

# Define a function to join the review DataFrame with the metadata DataFrame
def join_reviews_metadata(reviews_df, metadata_df):
    return pd.merge(reviews_df, metadata_df, on='product_id', how='left')


def main():
    # Load the Amazon review dataset into a DataFrame
    products_df = pd.read_csv('E:/Amazon_Review_Dataset/stage_2_dfs/products.csv')

    # Iterate over the chunks of the metadata file, extract the desired attributes, and join them with the review DataFrame
    metadata_cols = ['product_id', 'brand', 'price']
    for metadata_df_chunk in read_metadata("E:/Amazon_Review_Dataset/All_Amazon_Meta.json/All_Amazon_Meta.json", chunksize=10000):
        metadata_df_chunk = metadata_df_chunk[metadata_cols]
        products_df = join_reviews_metadata(products_df, metadata_df_chunk)

    # Save the result to a CSV file
    products_df.to_csv('E:/Amazon_Review_Dataset/stage_3_dfs/products.csv', index=False)


def main_2():
    metadata = {}
    print("Opening products.csv")
    products_df = pd.read_csv('E:/Amazon_Review_Dataset/stage_2_dfs/products.csv')
    desired_values = products_df['product_id'].values
    total = len(desired_values)
    progress_bar = tqdm(total=total)
    print("Opening large file")
    with open('E:/Amazon_Review_Dataset/All_Amazon_Meta.json/All_Amazon_Meta.json', 'r') as meta_data:
        for line in meta_data:
            item = json.loads(line)
            if item['asin'] in desired_values:
                progress_bar.update(1)
                metadata[item['asin']] = {'brand': item['brand'], 'price': item['price']}
    progress_bar.close()
    print("Merging in results")
    products_df['brand'] = products_df['product_id'].apply(lambda x: metadata[x]['brand'] if x in metadata else '')
    products_df['price'] = products_df['product_id'].apply(lambda x: metadata[x]['price'] if x in metadata else '')
    print("Saving results to csv")
    products_df.to_csv('E:/Amazon_Review_Dataset/stage_3_dfs/products.csv', index=False)

def merge_reviews_metadata(products_df, metadata_df):
    metadata_cols = ['asin', 'brand', 'price']
    merged_df = pd.merge(products_df, metadata_df[metadata_cols], left_on='product_id', right_on='asin', how='inner')
    merged_df.drop('asin', axis=1, inplace=True)
    return merged_df


def main_3(): #  ~ 1 hour run time
    print("Opening products.csv")
    products_df = pd.read_csv('E:/Amazon_Review_Dataset/stage_2_dfs/products.csv')
    chunksize = 100000
    merged_df = pd.DataFrame()
    expected = len(products_df)
    print("Beginning read loop")
    prev_len = 0
    progress_bar = tqdm(total=expected)
    for metadata_chunk in pd.read_json('E:/Amazon_Review_Dataset/All_Amazon_Meta.json/All_Amazon_Meta.json', lines=False, chunksize=chunksize):
        merged_df = pd.concat([merged_df, merge_reviews_metadata(products_df, metadata_chunk)])
        cur_len = len(merged_df)
        if cur_len != prev_len:
            progress_bar.update(cur_len-prev_len)
            prev_len = cur_len
            if cur_len == len(products_df):
                break
    progress_bar.close()
    # Save the result to a CSV file
    print("saving results")
    merged_df.to_csv('E:/Amazon_Review_Dataset/stage_3_dfs/products.csv', index=False)


if __name__ == '__main__':
    main_4()
