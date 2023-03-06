# ChatGPT really helped out with this data cleaning code. 

import pandas as pd
import json
from tqdm import tqdm
import math
import os 
import polars as pl


def main():
    print("Opening products.csv")
    products_df = pd.read_csv('E:/Amazon_Review_Dataset/stage_3_dfs_nk/products.csv')
    products_df = products_df.dropna() # drop rows with missing values
    products_df['price'] = products_df['price'].str.replace('$', '', regex=False)
    products_df['price'] = products_df['price'].str.split('-', expand=True)[0].str.strip()
    products_df = products_df[products_df['price'].str[0] != '<']
    products_df['price'] = pd.to_numeric(products_df['price'], errors='coerce')
    products_df = products_df.dropna() # drop rows with missing values
    
    brand_df = products_df['brand'].unique()
    brand_df = pd.DataFrame(brand_df)
    brand_df['brand_id'] = [i for i in range(len(brand_df))]
    brand_df.columns = ['brand_name', 'brand_id']
    brand_df['brand_name'] = brand_df['brand_name'].str.replace('[\t\n]+', '', regex=True)
    brand_df.to_csv("E:/Amazon_Review_Dataset/stage_4_dfs_nk/brands.csv", index=False)

    merged_df = pd.merge(products_df, brand_df, how='left', left_on='brand', right_on='brand_name')
    merged_df['brand'] = merged_df['brand_id']
    merged_df.drop(['brand', 'brand_name'], axis=1, inplace=True)
    merged_df.to_csv('E:/Amazon_Review_Dataset/stage_4_dfs_nk/products.csv', index=False)



if __name__ == '__main__':
    main()
