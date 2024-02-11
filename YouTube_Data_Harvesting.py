import googleapiclient.discovery

from pymongo import MongoClient

import mysql.connector

import pandas as pd

import streamlit as st


# API config
def API_config():

    API_ = "AIzaSyDwmNRjjLj6mot3OAMMJ0Wn856obUPYjWI"

    api_service_name = "youtube"
    api_version = "v3"
    youtube_ = googleapiclient.discovery.build(api_service_name, api_version, developerKey= API_)

    return youtube_

youtube = API_config()


# Channels

# Needed content from channel

def channel_info(Channel_id):
   
    request = youtube.channels().list(
      part="snippet,contentDetails,statistics",
      id=Channel_id
   )
    
    response = request.execute()

    for i in response["items"]:
        channel_detials={
            "Channel_id":i["id"],
            "Channel_name":i["snippet"]["title"],
            "Channel_description":i["snippet"]["description"],
            "Playlist_id":i["contentDetails"]["relatedPlaylists"]["uploads"],
            "Subscribers" : i["statistics"].get("subscriberCount"),
            "Views":i["statistics"].get("viewCount"),
            "Total_videos": i["statistics"]["videoCount"],
            }
    return channel_detials


#  Playlist Items
# 
# List of video ID's

def playlist_items():
  request = youtube.playlistItems().list(
      part="snippet",
      maxResults=50,
      playlistId=channel_info(Channel_Id)['Playlist_id']
  )
  response = request.execute()

  video_ids = [response["items"][i]["snippet"]["resourceId"]["videoId"] for i in range(len(response["items"]))]
  return video_ids


# Videos

def video_info(video_id):
  list_of_video_detials = []

  for i in video_id:
    request = youtube.videos().list(
        part = "snippet,contentDetails,statistics",
        id = i # videoId
    )
    response = request.execute()

    for j in response["items"]:

      s = j['contentDetails']['duration']
      l=[]
      f =''
      for i in s:
          if i.isnumeric():
              f = f+i
          else:
              if f:
                  l.append(f)
                  f=''
      if 'H' not in s:
          l.insert(0,'00')
      if 'M' not in s:
          l.insert(1,'00')
      if 'S' not in s:
          l.insert(-1,'00')  
        
      duration = ':'.join(l)
      result = duration.split(':')
      for i in range(0,3,1):
         if len(result[i]) == 1:
            value = "0" + result[i]
            result.remove(result[i])
            result.insert(i, value)
         final_result = ":".join(result)
        #  print(final_result)

      dict_of_video_detials ={
        "channel_name" : j['snippet']['channelTitle'],
        "channel_id" : j['snippet']['channelId'],
        "video_id" : j['id'],
        "title": j['snippet']['title'],
        "tags" : j['snippet'].get('tags'),
        "thumbnails" : j['snippet']['thumbnails']['default']['url'],
        "description" : j['snippet']['description'],
        "published_date" : j['snippet']['publishedAt'],
        "video_duration" : final_result,
        "caption" : j['contentDetails']['caption'],
        "total_views" : j['statistics']['viewCount'],
        "likes" : j['statistics'].get('likeCount'),
        "total_comment" : j['statistics'].get('commentCount'),
        "favorite_Count" : j['statistics']['favoriteCount'],
    }

      list_of_video_detials.append(dict_of_video_detials)

  return list_of_video_detials


# Comment Threads

def comments_info(Video_ids):

  list_of_comment_detials = []

  try:
    for i in Video_ids:
      request = youtube.commentThreads().list(
          part="snippet,replies",
          videoId = i, # videoId
          maxResults = 50
      )
      response = request.execute()

      for j in response["items"]:
          dict_of_comment_detials = {
              "channel_id": j["snippet"]["channelId"],
              "channel_name" : channel_info(Channel_Id)['Channel_name'],
              "video_id" : j["snippet"]["videoId"],
              "comment_id" : j["snippet"]["topLevelComment"]["id"],
              "comment_text": j["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
              "comment_person" : j["snippet"]["topLevelComment"]["snippet"]['authorDisplayName'],
              "comment_date" : j["snippet"]["topLevelComment"]["snippet"]['publishedAt'],
          }

          list_of_comment_detials.append(dict_of_comment_detials)

  except:
    pass
  return list_of_comment_detials


# channel_playlist

def channel_playlist(channel_id):

  list_of_playlist_detials = []

  request = youtube.playlists().list(
      part="snippet,contentDetails",
      channelId=channel_id,
      maxResults=100
  )
  response = request.execute()

  for i in response['items']:
    dict_of_playlist_detials = {
        "channel_id" : i['snippet']['channelId'],
        "channel_name": i['snippet']['channelTitle'],
        "playlist_id" : i['id'],
        "playlist_name": i['snippet']['title'],
        "published_date" : i['snippet']['publishedAt'],
        "video_count": i['contentDetails']['itemCount']
        }

    list_of_playlist_detials.append(dict_of_playlist_detials)
  return list_of_playlist_detials


# MongoDB connection

client = MongoClient('mongodb://localhost:27017/')
db = client['Youtube_data_harvesting']
collection = db['collection']

def main():
    list_of_channel_detials = channel_info(Channel_Id)
    playlist_det = channel_playlist(Channel_Id)
    Video_id_list = playlist_items()
    video_detials_list = video_info(Video_id_list)
    comments = comments_info(Video_id_list)

    main_channel_detials = {
        "channel_detial" : list_of_channel_detials,
        "playlist_detials" : playlist_det,
        "video_detials" : video_detials_list,
        "comment__detials": comments
    }

    insert = collection.insert_one(main_channel_detials)
    return st.success(":white_check_mark: Data inserted successfully")
    


# MySQL

config = {
    'user':'root', 'password':'gokul',
    'host':'127.0.0.1', 'database':'youtube_database'
}

connection = mysql.connector.connect(**config)

cursor = connection.cursor()
print(cursor)


# **Channel Table**

def channel_table(channel_id):
  
  Create_Query = """Create table if not exists channels(Channel_name VARCHAR(100) ,  Channel_ID VARCHAR(50),
                  Subscribers INT, Views INT, Total_videos INT, Channel_description TEXT,
                  Playlist_ID VARCHAR(50), PRIMARY KEY (Channel_ID));"""
  cursor.execute(Create_Query)
  connection.commit()

  db = client['Youtube_data_harvesting']
  collection = db['collection']

  d = collection.find_one({'channel_detial.Channel_id':channel_id})
  ch_list=d['channel_detial']
  df=pd.DataFrame(ch_list,index=[0])

  for index,row in df.iterrows():
    insert_query = '''insert into channels(Channel_name,
                                          Channel_ID,
                                          Subscribers,
                                          Views,
                                          Total_videos,
                                          Channel_description,
                                          Playlist_ID)
                                          values(%s,%s,%s,%s,%s,%s,%s)'''
    values=(row['Channel_name'],
            row['Channel_id'],
            row['Subscribers'],
            row['Views'],
            row['Total_videos'],
            row['Channel_description'],
            row['Playlist_id'])
    

    cursor.execute(insert_query,values)
    connection.commit()


# **playlist_table**


def playlist_table(channel_id):

  Create_Query = """Create table if not exists playlists(Playlist_Id VARCHAR(100) ,  Title VARCHAR(100),
                  Channel_Id VARCHAR(100), Channel_Name VARCHAR(100), PublishedAt VARCHAR(100), Video_Count INT,
                  PRIMARY KEY (Playlist_Id));"""
  cursor.execute(Create_Query)
  connection.commit()
  
  db = client['Youtube_data_harvesting']
  collection = db['collection']

  d = collection.find_one({'channel_detial.Channel_id':channel_id})
  pl_list=d['playlist_detials']
  df1=pd.DataFrame(pl_list)
  df1.published_date = df1.published_date.str.replace('T',',')
  df1.published_date = df1.published_date.str.replace('Z','')

  for index,row in df1.iterrows():
    insert_query = '''insert into playlists(Playlist_Id,
                                            Title,
                                            Channel_Id,
                                            Channel_Name,
                                            PublishedAt,
                                            Video_Count)
                                            values(%s,%s,%s,%s,%s,%s)'''
    values=(row['playlist_id'],
            row['playlist_name'],
            row['channel_id'],
            row['channel_name'],
            row['published_date'],
            row['video_count'])
    
    cursor.execute(insert_query,values)
    connection.commit()


# **video_table**

def video_table(channel_id):
  Create_Query = """create table if not exists videos(
                Channel_Name varchar(100),
                Channel_ID varchar(100),
                Video_Id varchar(100),
                Video_Title varchar(200),
                Video_Description text,
                Tags text,
                PublishedAt varchar(200),
                View_Count int,
                Like_Count int,
                Favorite_Count int,
                Comment_Count int,
                Duration TIME,
                Thumbnail varchar(200),
                Caption_Status varchar(100)
              );"""
  
  cursor.execute(Create_Query)
  connection.commit()
    
  db = client['Youtube_data_harvesting']
  collection = db['collection']

  d = collection.find_one({'channel_detial.Channel_id':channel_id})
  vi_list=d['video_detials']
  df2=pd.DataFrame(vi_list)
  df2.published_date = df2.published_date.str.replace('T',',')
  df2.published_date = df2.published_date.str.replace('Z','')

  for index,row in df2.iterrows():
    insert_query = '''insert into videos(Channel_Name,
                                        Channel_ID,
                                        Video_Id,
                                        Video_Title,
                                        Video_Description,
                                        Tags,
                                        PublishedAt,
                                        View_Count, 
                                        Like_Count,
                                        Favorite_Count,
                                        Comment_Count,
                                        Duration,
                                        Thumbnail,
                                        Caption_Status)
                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    row['tags'] = str(row['tags'])
    values=(row['channel_name'],
            row['channel_id'],
            row['video_id'],
            row['title'],
            row['description'],
            row['tags'],
            row['published_date'],
            row['total_views'],
            row['likes'],
            row['favorite_Count'],
            row['total_comment'],
            row['video_duration'],
            row['thumbnails'],
            row['caption'])
    
    cursor.execute(insert_query,values)
    connection.commit()


# **comment_table**

def comment_table(channel_id):
  Create_Query = """create table if not exists comments(
                channel_Id varchar(50),
                channel_name varchar(50),
                Comment_Id varchar(100),
                Comment_Text text,
                Comment_Author varchar(100),
                Comment_Published timestamp
              );"""
  cursor.execute(Create_Query)
  connection.commit()
    
  db = client['Youtube_data_harvesting']
  collection = db['collection']

  d = collection.find_one({'channel_detial.Channel_id':channel_id})
  cmt_list=d['comment__detials']
  df3=pd.DataFrame(cmt_list)

  df3.comment_date = df3.comment_date.str.replace('T',',')
  df3.comment_date = df3.comment_date.str.replace('Z','')

  for index,row in df3.iterrows():
    insert_query = '''insert into comments(
                                        channel_Id,
                                        channel_name,
                                        Comment_Id,
                                        Comment_Text,
                                        Comment_Author,
                                        Comment_Published)
                                        values(%s,%s,%s,%s,%s,%s)'''
    values =(
            row['channel_id'],
            row['channel_name'],
            row['comment_id'],
            row['comment_text'],
            row['comment_person'],
            row['comment_date'])
    
    cursor.execute(insert_query,values)
    connection.commit()


# SQL tables

def main_table(channel_id):
  channel_table(channel_id)
  playlist_table(channel_id)
  video_table(channel_id)
  comment_table(channel_id)

  return st.success(":white_check_mark: Data Successfully Migrated to SQL")

# ----------------------------------------------------------------------------------


def ch_names():
  channel_list =[]
  for i in collection.find({}):
    channel_list.append(i['channel_detial']['Channel_name'])
  return channel_list

def ch_ids(option):
  id = collection.find_one({'channel_detial.Channel_name':option})
  id = id['channel_detial']['Channel_id']
  return id


# ------------------ Streamlit ------------------#

st.set_page_config(page_title="Data Harvesting and Warehoushing", page_icon=":alien:")
name = f"<p style='font-size:25px;'>Created By: Gokulvasan V</p>"
st.markdown(f"{name}", unsafe_allow_html=True,) 
st.write("**Project GitHub:** [github.com/Gokulvasan-V](https://github.com/Gokulvasan-V/YouTube-Data-Harvesting-and-Warehousing)")
st.write("**LinkedIn:** [linkedin.com/in/gokulvasan-v](https://www.linkedin.com/in/gokulvasan-v-5760141b3/)")

background_image = """
<style>
[data-testid="stAppViewContainer"] > .main {
    background-image: url("https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTsEKJjDv-RepyLwiOhgXVVJe2k8Y-ThuM2qw&s");
    background-size: 100vw 100vh;
    background-position: center;  
    background-repeat: no-repeat;
}
</style>
"""

st.markdown(background_image, unsafe_allow_html=True)

st.title(":red[Youtube Data Harvesting and Warehoushing]")
r = f"<p style='font-size:25px;'>using MySQL, MongoDB & Streamlit</p>"
st.markdown(f"**{r}**", unsafe_allow_html=True,)
Channel_Id=st.text_input("Enter the channel id:",)
button=st.button("Extract Data :cinema:")
if button:
  main()

st.write("Select the channel to transform data to MySQL")
option = st.selectbox('Select Channel',ch_names(),index=None,placeholder='Select Channel')
if st.button('Migrate to SQL :arrows_counterclockwise:'):
  channel_ = ch_ids(option)
  main_table(channel_)

# _______________ Questions______________________# 

def query1():
    query = """select Channel_Name,Video_Title from videos;"""
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result,columns=['Channel Name','Video Name'])
    return df

def query2():
    query = """select Channel_name,Total_videos from channels order by Total_videos desc;"""
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result,columns=['Channel Name','No.Videos'])
    return df

def query3():
    query = """select Video_Title, Channel_Name,View_Count from videos order by View_Count desc limit 10;"""
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result,columns=['Video Name','Channel Name','Views'])
    return df

def query4():
    query = """select Video_Title, Comment_Count from videos order by Comment_Count desc;"""
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result,columns=['Video Name','Comment Count'])
    return df

def query5():
    query = """select Video_Title, Channel_Name, Like_Count from videos order by Like_Count desc;"""
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result,columns=['Video Name','Channel Name','Like Count'])
    return df

def query6():
    query = """select Video_Title, Like_Count from videos order by Like_Count desc;"""
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result,columns=['Video Name','Like Count'])
    return df

def query7():
    query = """select Channel_name, Views from channels order by Views desc;"""
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result,columns=['Channel Name','Views'])
    return df

def query8():
    query = """select Channel_Name,Video_Title,date (PublishedAt) from videos where year(PublishedAt)=2022;"""
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result,columns=['Channel Name','Video Name','Published At'])
    return df

def query9():
    query = """select Channel_Name, sec_to_time(avg(time_to_sec(Duration))) as Avg_Duration from videos group by Channel_Name order by Avg_Duration ;"""
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result,columns=['Channel Name','Average Duration'])
    return df

def query10():
    query = """select Video_Title,Channel_Name,Comment_Count from videos order by Comment_Count desc;"""
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result,columns=['Video Name','Channnel Name','Comment Count'])
    return df    



st.title(':red[SQL Query]')


   
options = ['1. What are the names of all the videos and their corresponding channels?',
           '2. Which channels have the most number of videos, and how many videos do they have?',
           '3. What are the top 10 most viewed videos and their respective channels?',
           '4. How many comments were made on each video, and what are their corresponding video names?',
           '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
           '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
           '7. What is the total number of views for each channel, and what are their corresponding channel names?',
           '8. What are the names of all the channels that have published videos in the year 2022?',
           '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
           '10. Which videos have the highest number of comments, and what are their corresponding channel names?'
           ]
try:
    select_question = st.selectbox("Select the Question",options,index = None,placeholder='Tap to Select')
    if select_question == '1. What are the names of all the videos and their corresponding channels?':
        st.dataframe(query1())

    if select_question == '2. Which channels have the most number of videos, and how many videos do they have?':
        st.dataframe(query2())

    if select_question == '3. What are the top 10 most viewed videos and their respective channels?':
        st.dataframe(query3())    

    if select_question == '4. How many comments were made on each video, and what are their corresponding video names?':
        st.dataframe(query4())

    if select_question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        st.dataframe(query5())    

    if select_question == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        st.dataframe(query6())     

    if select_question == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        st.dataframe(query7())     

    if select_question == '8. What are the names of all the channels that have published videos in the year 2022?':
        st.dataframe(query8())    

    if select_question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        st.dataframe(query9())     

    if select_question == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        st.dataframe(query10())     
except:
    st.success(":x: Please add atleast one channel to MySQL database :x:")        