# Inner join the reviews and product tabes, save as the new review tables. Also add new category_id value and generate category table at the end. 

import pandas as pd
import json
from tqdm import tqdm
import math
import os 
import polars as pl


def main():
    print("Opening products.csv")
    products_df = pd.read_csv('E:/Amazon_Review_Dataset/stage_4_dfs_nk/products.csv')
    asins = products_df['asin'].unique()

    for file in os.listdir('E:/Amazon_Review_Dataset/stage_1_dfs_nk'):
        if 'review' in file:
            print("Processing file: " + file)
            df = pd.read_csv(os.path.join('E:/Amazon_Review_Dataset/stage_1_dfs_nk', file), low_memory=False)
            df = df[df['asin'].isin(asins)]
            df['vote'] = df['vote'].fillna(0)
            df.to_csv('E:/Amazon_Review_Dataset/stage_5_dfs_nk/' + file, index=False)

if __name__ == '__main__':
    main()
