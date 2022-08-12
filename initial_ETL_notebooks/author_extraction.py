# %%
import praw
import  sys
from praw.models import MoreComments
import pandas as pd
import pprint
from datetime import datetime , timedelta ,date ,time
import pytz
from pathlib import Path
import configparser
import os

# %%
#! pip install praw

# %%

path = Path(__file__)
ROOT_DIR = path.parent.parent.absolute()
config_path = os.path.join(ROOT_DIR, "configuration.conf")

config = configparser.ConfigParser()
config.read(config_path)
output_name = sys.argv[1]

# %%
print(config.get("reddit config","client_id"))

# %%
reddit = praw.Reddit(
        client_id= config.get("reddit config","client_id"),
        client_secret=config.get("reddit config","client_secret"),
        user_agent="Collect to data visualize r/singapore",#  description
        password=config.get("reddit config","password"),            
        username=config.get("reddit config","username") # TODO make creds secure
        ,check_for_async=False)

# %%
print(reddit.read_only)

# %%
singaporesub = reddit.subreddit("singapore")
author_details_list = []

# %%

for post in singaporesub.new(limit=200):
    
    if post.author is None or  post.author.id is None:
       continue
    else:        
       #posts.append([post.title,post.selftext,post.id,post.score,post.upvote_ratio,post.link_flair_text,post.created_utc,post.num_comments,post.author.id,post.url,post.distinguished,post.is_original_content])
       author_details_list.append([post.author.id,post.author.name,post.author.link_karma,post.author.comment_karma,post.author.created_utc,post.author.is_gold,post.author.is_mod,post.author.is_employee])


redditor_details = pd.DataFrame(author_details_list,columns=['author_id', 'author_name' ,'link_karma', 'comment_karma', 'acc_creation_date', 'is_gold', 'is_mod','is_employee'])
print(len(redditor_details))

    # print(submission.title)|
    # print(submission.selftext)
    # # Output: the submission's title
    # print(submission.score)
    # print(submission.upvote_ratio)
    # # Output: the submission's score
    # print(submission.id)
    # # Output: the submission's ID
    # print(submission.url)
    # print(submission.link_flair_text)
    # print(submission.num_comments)
    # print(submission.created_utc)

    # print(submission.author)
    # print(submission.author.link_karma)
    # print(submission.author.is_gold)
    # print(submission.author.is_mod)
    # print(submission.author.is_employee)
    # submission.comment_sort = "confidence"
    # comments = submission.comments.list()
    # for comm in comments:
    #     if isinstance(comm, MoreComments):
    #         continue
    #     print(comm.body)
    #     print(comm.score)
    #     print(comm.link_id)
    #     print(comm.id)
       
    
  
    
    
    #    TODO   FIGURE OUT COMMENT FOREST
    # TODO comment sorting
    # TODO does the script need argumen for date ?
    # TODO  can anythin be done with the author 

    
  


# %%
print(redditor_details.head())

# %%
redditor_details['acc_creation_date']=(pd.to_datetime(redditor_details['acc_creation_date'],unit='s',utc=True))

# %%
redditor_details["acc_creation_date"] = redditor_details["acc_creation_date"].dt.tz_convert('Asia/Singapore')

# %%
redditor_details.head()

# %%
filepath = Path(f'/media/user/volume2/students/s121md210_02/singaporesub/Data/{output_name}_author.csv')
# %%
redditor_details.to_csv(filepath,index=False)

# %%


# %%



