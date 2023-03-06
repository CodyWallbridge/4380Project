# Recreate customer table given reviewerID found in reviews tables 

import pandas as pd
import json
from tqdm import tqdm
import math
import os 
import polars as pl


def main():
    all_customer_ids = [] 
    
    for file in os.listdir('E:/Amazon_Review_Dataset/stage_5_dfs_nk'):
        if 'review' in file:
            print("Processing file: " + file)
            review_df = pd.read_csv(os.path.join('E:/Amazon_Review_Dataset/stage_5_dfs_nk', file), low_memory=False)
            all_customer_ids += list(review_df['reviewerID'].unique())
    all_customer_ids = pd.Series(all_customer_ids).unique()
    customers_df = pd.read_csv('E:/Amazon_Review_Dataset/stage_2_dfs_nk/customers.csv')
    customers_df = customers_df[customers_df['reviewerID'].isin(all_customer_ids)]
    customers_df.to_csv('E:/Amazon_Review_Dataset/stage_6_dfs_nk/customers.csv', index=False)

if __name__ == '__main__':
    main()
