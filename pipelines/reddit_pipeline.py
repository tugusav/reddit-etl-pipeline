from praw import Reddit
from utils.constants import REDDIT_SECRET, REDDIT_CLIENT_ID
from etls.reddit_etl import connect_to_reddit, extract_posts, transform_data, load_data_to_csv
from utils.constants import OUTPUT_PATH
import pandas as pd


def reddit_pipeline(file_name: str, subreddit: str, time_filter='day', limit=None):
    # connect to reddit instance

    instance = connect_to_reddit(REDDIT_CLIENT_ID, REDDIT_SECRET, 'tugusavcloud')

    # extract data from reddit
    posts = extract_posts(instance, subreddit, time_filter, limit)

    posts_df = pd.DataFrame(posts)
    # transform data
    posts_transformed = transform_data(posts_df)

    # load data to csv
    file_path = f'{OUTPUT_PATH}/{file_name}'
    load_data_to_csv(posts_transformed, file_path)

    return file_path



    