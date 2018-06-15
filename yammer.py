##############################################################################
# Author            : Amber Zaratsian
# Creation Date     : 13AUG2017
# Description       : Python code to query Yammer to collect information from a 
#                     particular Yammer group. Part 1: Creates a csv with user 
#                     information. Part 2: Creates a csv with yammer posts 
#                     after a specific datetime.
# Python Version    : 3.5.4
# Anaconda Version  : 5.0.1
# Operating System  : Windows 10
# Notes             : API Documentation https://developer.yammer.com/docs/
##############################################################################

import requests
from datetime import datetime
import pandas as pd

#client_id = <> # <- Insert your client_id
#client_secret = <> # <- Insert your client_secret

access_token = <> # <- Insert your Access Token
headers = {"Authorization":"Bearer abcDefGhi"} # <- Insert your Bearer Token (example provided)

# Yammer Group id
group_id = '123456' # <- Insert the yammer group id

##############################################################################
#
# Part 1: Users 
#                
##############################################################################

users = requests.get('https://www.yammer.com/api/v1/users/in_group/' + group_id + '.json', headers=headers).json()

# Create a list of users
all_users = users['users']

# Will return only the first 50 members of a group
# if more than 50 members then the more_avialable parameter will be true 
# add page condition to return next 50 members
more_available = users['more_available']

page = 2
while more_available == True:
    next_users = requests.get('https://www.yammer.com/api/v1/users/in_group/' + group_id + '.json?page=' + str(page), headers=headers).json()
    more_available = next_users['more_available']
    all_users = all_users + next_users['users']
    print(more_available)
    print(page)
    page+=1
                

# Extract features from JSON and create a list for each user
users_ls = []
for i in all_users:    
    activation_timestamp = i['activated_at']
    email = i['email']
    name = i['full_name']
    user_id = i['id']
    job_title = i['job_title']
    followers = i['stats']['followers']
    following = i['stats']['following']
    users_ls.append([activation_timestamp, email, name, user_id, job_title, followers, following])

# Convert list to Pandas DataFrame
users_df = pd.DataFrame(users_ls, columns=['activation_timestamp', 'email', 'name', 'user_id', 'job_title', 'followers', 'following']) 

# Convert 'activated_at' to datetime format  
users_df['activation_timestamp'] = pd.to_datetime(users_df['activation_timestamp'])

# Today's date
now = datetime.now()
today = now.strftime("%m-%d-%Y")

# Save path
path = 'yammer_users_' + today + '.csv'

users_df.to_csv(path, index=False, encoding='utf-8')


##############################################################################
#
# Part 2: Messages 
#                
##############################################################################
# Time last script ran
datenow = datetime.strptime('2017/05/14 03:30:00', '%Y/%m/%d %H:%M:%S')

# Query Yammer for messages posted in the particular group of interest
messages = requests.get('https://www.yammer.com/api/v1/messages/in_group/' + group_id + '.json', headers=headers).json()

# Convert created_at from string to datetime
for i in messages['messages']:
    i['created_at'] = datetime.strptime(i['created_at'][:19], '%Y/%m/%d %H:%M:%S')

# Keep messages if they were posted after the script ran last
newest_messages = []
for i in messages['messages']:
    if i['created_at'] > datenow :
        newest_messages.append(i)

# Find out whether the group has more than 20 messages by looking for older_available=True
older_available = messages['meta']['older_available']
oldest_id = str(messages['messages'][(len(messages['messages']) - 1)]['id'])

# Length of newest_messages
message_no = len(newest_messages)

# Query Yammer -  limitations with the Yammer API mean that it returns a maximum of 20 messages per query. 
# So, if more than 20 messages have been posted to the group you’ve selected for export, you’ll have to
# repeat the query more than once. 
# Loop through to query older messages if available
all_messages = newest_messages
counter = 0
while older_available == True and message_no == 20:
    counter = counter + 1
    print(counter) 
    messages = requests.get('https://www.yammer.com/api/v1/messages/in_group/' + group_id + '.json?older_than=' + oldest_id, headers=headers).json()
    older_messages = []
    for i in messages['messages']:
        i['created_at'] = datetime.strptime(i['created_at'][:19], '%Y/%m/%d %H:%M:%S')
        if i['created_at'] > datenow :
            older_messages.append(i)
    all_messages = all_messages + older_messages
    message_no = len(older_messages)
    oldest_id = str(messages['messages'][(len(messages['messages']) - 1)]['id'])
    older_available = messages['meta']['older_available']
    print('older_available ' + str(older_available) + ' ' + oldest_id)
    print('number of older messages ' + str(message_no))  
    #time.sleep(4)
    

# Extract features from JSON and create a list for each message
messages_ls = []
for i in all_messages:    
    replied_to_id = i['replied_to_id']
    thread_id = i['thread_id']
    created_at = i['created_at']
    likes_count = i['liked_by']['count']
    user_id = i['sender_id']
    language = i['language']
    message_type = i['message_type']
    posting = i['body']['plain'].replace('\n', ' ')
    if len(i['attachments']) == 1:
        attachment = i['attachments'][0]['web_url']
    else:
        attachment = ''
    messages_ls.append([replied_to_id, thread_id, created_at, likes_count, user_id, language, message_type, posting, attachment])

# Convert list to Pandas DataFrame
messages_df = pd.DataFrame(messages_ls, columns=['replied_to_id', 'thread_id', 'created_at', 'likes_count', 'user_id', 'language', 'message_type', 'posting', 'attachment']) 

# Filter for messages written in English
#messages_df = messages_df[messages_df['language']=='en']

# Convert 'created_at' to datetime format  
messages_df['created_at'] = pd.to_datetime(messages_df['created_at'])

# Replace NaNs in 'replied_to_id' with -1 and convert column from float to int
messages_df['replied_to_id'] = messages_df['replied_to_id'].fillna(-1).astype(int)

messages_df.sort_values(['thread_id', 'replied_to_id', 'created_at'], ascending=[False, True, True], inplace=True)

messages_df = messages_df.merge(users_df[['user_id', 'name']], how='left', on='user_id')

# Rearrange columns
messages_df = messages_df[['thread_id', 'replied_to_id', 'created_at', 'name', 'user_id', 'likes_count', 'posting', 'attachment', 'message_type', 'language']]

messages_df.to_csv('yammer_messages.csv', index=False, encoding='utf-8')
