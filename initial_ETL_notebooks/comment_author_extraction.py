# %%
import praw
import  sys
from praw.models import MoreComments
import pandas as pd
import pprint
from datetime import datetime , timedelta ,date ,time
import pytz
from  pathlib import Path
import os
import configparser

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
comment_authors = []
counter = 0


for post in singaporesub.new(limit=200):
    
    if post.author.id == None:
       continue
    else: 
      print(post.id, " this is a post")
      counter = 0       
      #posts.append([post.title,post.selftext,post.id,post.score,post.upvote_ratio,post.link_flair_text,post.created_utc,post.num_comments,post.author.id,post.url,post.distinguished,post.is_original_content])
      #author_details.append([post.author.id,post.id,post.author,post.author.link_karma,post.author.comment_karma,post.author.created_utc,post.author.is_gold,post.author.is_mod,post.author.is_employee,post.author.is_suspended])
      post.comment_sort = "top"
      comments = post.comments
      for comm in comments:
         
         if counter == 20:
            break
         
         
         print(comm)
         print(type(comm))
         
         if isinstance(comm, (MoreComments)) or isinstance(comm,type(None)) :
   
            continue
         if comm.author is not None:
            comment_authors.append([comm.author.id,comm.id,post.id,comm.author.name,comm.author.link_karma,comm.author.comment_karma,comm.author.created_utc,comm.author.is_gold,comm.author.is_mod,comm.author.is_employee])
            counter+=1
    #     print(comm.body)
    #     print(comm.score)
    #     print(comm.link_id)
    #     print(comm.id)

comment_author_details = pd.DataFrame(comment_authors,columns=['author_id', 'comment_id', 'post_id','author_name' ,'link_karma', 'comment_karma', 'acc_creation_date', 'is_gold', 'is_mod','is_employee'])


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
    
       
    
  
    
    
    # TODO   FIGURE OUT COMMENT FOREST
    # TODO comment sorting
    # TODO does the script need argumen for date ?
    # TODO  can anythin be done with the author 

    
  


# %%
print(comment_author_details.head(50))
print(len(comment_author_details))

# %%
comment_author_details['acc_creation_date']=(pd.to_datetime(comment_author_details['acc_creation_date'],unit='s',utc=True))
comment_author_details["acc_creation_date"] =comment_author_details["acc_creation_date"].dt.tz_convert('Asia/Singapore')

# %%
filepath = Path(f'/media/user/volume2/students/s121md210_02/singaporesub/Data/{output_name}_commentAuthor.csv')

# %%
comment_author_details.to_csv(filepath,index=False)

# %%


# %%



