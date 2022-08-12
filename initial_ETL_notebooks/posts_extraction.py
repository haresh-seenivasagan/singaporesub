# %%
import praw
import  sys
from praw.models import MoreComments
import pandas as pd
import pprint
from datetime import datetime , timedelta ,date ,time
import pytz
from pathlib import Path
import os
import configparser

# %%
#! pip install praw

# %%
path = Path(__file__)
ROOT_DIR = path.parent.parent.absolute()
config_path = os.path.join(ROOT_DIR, "configuration.conf")
output_name = sys.argv[1]
config = configparser.ConfigParser()
config.read(config_path)

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
posts = []
author_details = []

# %%

for post in singaporesub.new(limit=200):
    
    if post.author is None or post.author.id is None:
       continue
    else:        
       posts.append([post.title,post.selftext,post.id,post.score,post.upvote_ratio,post.link_flair_text,post.created_utc,post.num_comments,post.author.id,post.url,post.distinguished,post.is_original_content,post.over_18])
      # author_details.append([post.author.id,post.id,post.author,post.author.link_karma,post.author.comment_karma,post.author.created_utc,post.author.is_gold,post.author.is_mod,post.author.is_employee,post.author.is_suspended])


posts = pd.DataFrame(posts,columns=['title', 'body', 'post_id' ,'score', 'upvote_ratio', 'flair', 'created_time', 'num_comments','author_id' ,'url','distinguished','is_original_content','over_18'])
print(len(posts))

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
print(posts.head())

# %%
posts['created_time']=(pd.to_datetime(posts['created_time'],unit='s',utc=True))
posts["created_time"] = posts["created_time"].dt.tz_convert('Asia/Singapore')

# %%
posts.body = posts.body.replace(r'^\s*$', 'NULL', regex=True)

# %%
posts.head()

# %%
filepath = Path(f'/media/user/volume2/students/s121md210_02/singaporesub/Data/{output_name}_posts.csv')

# %%
posts.to_csv(filepath,index=False)

# %%
posts.nunique() 

# %%


# # %%
# posts.created_time

# # %%
# type(posts.created_time)

# # %%
# tz = pytz.timezone("Asia/Singapore")

# # %%
# today = date.today()
# midnight_local = datetime.combine(today, time())
# sing_midnight = tz.localize(midnight_local)
# print(today)
# print(midnight_local)
# yesterday = sing_midnight - timedelta(days=1)
# begining = sing_midnight


# # %%
# print(yesterday)

# # %%
# mask =  (posts['created_time'] > yesterday) & (posts['created_time'] <= begining)

# # %%


# # %%
# past_24_posts = posts[mask]

# # %%
# past_24_posts

# # %%
# posts.to_csv('test2to3.csv')

# # %%
# posts['created_time']

# # %%
# filepath=Path('C:/Users/hares/singaporesub/Data/init_Posts_NoTransform.csv')

# # %%
# yo=pd.read_csv(filepath)

# # %%
# yo.nunique()

# # %%
# yo['created_time']=(pd.to_datetime(yo['created_time'],unit='s',utc=True))
# yo["created_time"] = yo["created_time"].dt.tz_convert('Asia/Singapore')

# # %%
# yo.to_csv('testlmao.csv')

# # %%



