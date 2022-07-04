import requests
url= 'https://oauth.reddit.com/singapore/new'
res = requests.get(url).json()
print(res)