from praw import Reddit
import sys
from utils.constants import POST_FIELDS
import pandas as pd
import numpy as np

def connect_to_reddit(client_id, client_secret, user_agent) -> Reddit:
    try:
        reddit = Reddit(client_id=client_id,
                        client_secret=client_secret,
                        user_agent=user_agent)
        print("Connected to Reddit")
    except Exception as e:
        print(e)
        sys.exit(1)
    return reddit

def extract_posts(reddit_instance, subreddit, time_filter, limit):
    try:
        posts = reddit_instance.subreddit(subreddit).top(time_filter, limit=limit)

        posts_list = []
        for p in posts:
            post_dict = vars(p)
            post = {key: post_dict[key] for key in POST_FIELDS}
            print(post)
            for key, value in post.items():
                print(key, value)
            posts_list.append(post)
    except Exception as e:
        print(e)
        sys.exit(1)
    return posts_list

def transform_data(post_df):
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    post_df['over_18'] = np.where((post_df['over_18'] == True), True, False)
    post_df['author'] = post_df['author'].astype(str)
    edited_mode = post_df['edited'].mode()
    post_df['edited'] = np.where(post_df['edited'].isin([True, False]),
                                 post_df['edited'], edited_mode).astype(bool)
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['upvote_ratio'] = post_df['upvote_ratio'].astype(float)
    post_df['score'] = post_df['score'].astype(int)
    post_df['selftext'] = post_df['selftext'].astype(str)
    post_df['title'] = post_df['title'].astype(str)
    # Remove any newline characters from the title
    post_df['title'] = post_df['title'].str.replace('\n', ' ')
    # Remove any newline characters from the selftext
    post_df['selftext'] = post_df['selftext'].str.replace('\n', ' ')
    return post_df

def load_data_to_csv(df, path):
    df.to_csv(path, index=False)
    print(f"Data saved to {path}")

# [2024-02-19, 12:07:28 UTC] {logging_mixin.py:188} INFO - 
# {'id': '1au4hps', 
#  'title': 'Klinsmann rejects South Korean FA complaints: We were a success', 
#  'selftext': '', 
#  'score': 48, 
#  'num_comments': 6, 
#  'author': Redditor(name='Majano57'), 
#  'created_utc': 1708289502.0, 
#  'url': 'https://www.tribalfootball.com/articles/klinsmann-rejects-south-korean-fa-complaints-we-were-a-success-4486251', 
#  'upvote_ratio': 0.92, 
#  'over_18': False, 
#  'edited': False, 
#  'spoiler': False, 
#  'stickied': False, 
#  'subreddit': Subreddit(display_name='football')}
