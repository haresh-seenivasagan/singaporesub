import praw

import  sys
SUBREDDIT = 'singapore'

LIMIT = None
 

def main():
    reddit_instance = connect_api()
    
    print(reddit_instance.read_only)
    subreddit_posts_object  = subreddit_posts(reddit_instance)
    extracted_data = extract_data(subreddit_posts_object)

def connect_api():
    """connect to reddit api   https://www.reddit.com/dev/api/ """
        
    try:
        instance = praw.Reddit(
        client_id="Mf3cdHYsO1pTQ8SXMJWF6Q",
        client_secret="_5qEJwsVcGp1HUneUvTPOQfeNvWeKQ",
        user_agent="Collect to data visualize r/singapore",#  description
        password="Haresh_2255",            
        username="Apart_climate_8516" # TODO make creds secure
        )
        return instance
    except Exception as e:
        print(f"Unable to connect to API. Error: {e}")
        sys.exit(1)

def subreddit_posts(reddit_instance):
    ''' create posts object for reddit instance'''
    try:
        subreddit = reddit_instance.subreddit(SUBREDDIT)
        posts = subreddit.new(limit = LIMIT)
        return posts
    except Exception as e:
        print(f"There's been an issue. Error: {e}")
        sys.exit(1)

def extract_data(posts):
  """Extract Data to Pandas DataFrame object"""
  list_of_items = []
  try:
    for submission in posts:
      to_dict = vars(submission)
      sub_dict = {field : to_dict[field] for field in POST_FIELDS}
      list_of_items.append(sub_dict)
      extracted_data_df = pd.DataFrame(list_of_items)
  except Exception as e:
    print(f'There has been an issue. Error {e}')
    sys.exit(1)

  return extracted_data_df        


if __name__ == "__main__":
  main()

