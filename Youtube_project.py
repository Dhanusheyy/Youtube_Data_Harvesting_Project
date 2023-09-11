import streamlit as st
import pandas as pd
import mysql.connector
from pymongo import MongoClient
import pandas as pd
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
import googleapiclient.errors
from googleapiclient.errors import HttpError
import requests
import re

API_KEY = 'AIzaSyAmYgtIxoV_G-8FaCrqUb0UNq7DFh4tMwc'

#%%

def get_channel_id(channel_link):
    
    if "@" in channel_link:
        channel_url = channel_link+"/about"
    
        response = requests.get(channel_url) # Send a GET request to the channel page
    
        channel_id_match = re.search(r'"channelId":"([A-Za-z0-9_-]+)"', response.text) # Extract the channel ID using regular expressions
    
        if channel_id_match:
            channel_id = channel_id_match.group(1)
    return  channel_id



def get_Channel_Information(all_id):
    
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    
    cha_res = youtube.channels().list(
                    part='contentDetails,snippet,statistics',
                    id=all_id).execute()
    ch_ti1 = []
    for i in cha_res['items']:
            ch_id = i['id']
            channel_link = f"https://www.youtube.com/channel/{ch_id}"
            ch_title = i['snippet']['title']
            ch_playlist = i['contentDetails']['relatedPlaylists']['uploads']
            CreatedAt = i['snippet']['publishedAt']
            Subcount = i['statistics']['subscriberCount']
            TotalViews = i['statistics']['viewCount']
            TotalVideos = i['statistics']['videoCount']
            ch_logo = i['snippet']['thumbnails']['medium']['url']
            ch_ti1.append({"Channel_id": ch_id,"Channel_Name": ch_title,"Playlist_id": ch_playlist,"Created_Date":CreatedAt,
                                                    "Subcribers":Subcount,"TotalViews":TotalViews,"TotalVideos":TotalVideos,
                                                    "Thumbnail":ch_logo,"Channel_link":channel_link})
        
    
    return ch_ti1[0]


def get_video_ids(playlist_id):
    
    youtube = build('youtube', 'v3', developerKey=API_KEY)

    video_ids = []

    try:
        request = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=playlist_id,
            maxResults=50  )

        counter = 0  # Counter for tracking the number of video IDs
        while request and counter < 100:  # Stop when 100 video IDs are obtained
            response = request.execute()
            for item in response['items']:
                video_ids.append(item['contentDetails']['videoId'])
                counter += 1
            request = youtube.playlistItems().list_next(request, response)

    except HttpError as e:
        error_message = e.content.decode("utf-8")
        print("An error occurred:", error_message)

    return video_ids[:50] 



def get_comments_data(vid_lis):
    
    youtube = build('youtube', 'v3', developerKey=API_KEY)
        
    comments = []
    for vids in vid_lis:
        try:
            ch_response = youtube.videos().list(
                part='snippet',
                id=vids).execute()

            for video in ch_response['items']:
                ch_id = video['snippet']['channelId']
                vid_title = video['snippet']["title"]
                Channel_title = video['snippet']["channelTitle"]

            response = youtube.commentThreads().list(
                part='snippet,replies',
                videoId=vids,
                maxResults=30,
            ).execute()
            
            video_comments = []
            for item in response['items']:
                
                comment = item['snippet']['topLevelComment']['snippet']['textOriginal']                
                
                repl = []
                if 'replies' in item:
                    replies = item['replies']['comments']
                    for reply in replies:
                        reply_text = reply['snippet']['textOriginal']
                        repl.append(reply_text)

                else:
                    repl = ["No reply"]
                
                video_comments.append({"Comments":comment,"Replies": repl})
            comments.append({"Channel_id":ch_id,"Video_id": vids,"Video_title":vid_title,"Comments":video_comments})

        except HttpError as e:
            if e.resp.status == 403:
                pass

    return comments


def get_video_info(video_ids):
    
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    #video_ids = video_ids[:100]
    all_video_stats = []
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part='snippet,statistics',
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute()

        for video in response['items']:
            ch_id = video['snippet']['channelId']
            Video_Id = video['id']
            Title = video['snippet']['title']
            Published_date = video['snippet']['publishedAt']
            
            try:
                Views = video['statistics']['viewCount']
                Likes = video['statistics']['likeCount']
                Comments = video['statistics']['commentCount']
                
                all_video_stats.append({"Channel_id": ch_id,"Video_id":Video_Id,"Video_Title":Title,"Uploaded_Date":Published_date,
                                                           "Total_Views":Views,"Total_Likes":Likes,
                                                                  "Total_Comments":Comments})
            except KeyError:
                continue
    
    return all_video_stats


def convert_comment_df(data):
    re_data = []
    for d in data:
        for i in d['Comments']:
            dt = dict()
            dt.update({'Channel_id':d['Channel_id'],'Video_id':d['Video_id'],
                      'Video_title':d['Video_title']})
            dt.update({'Comments':i['Comments'],'Replies':i['Replies'][0]})
            re_data.append(dt)
    return re_data


def mdb_insert(keyword,channel_Data,video_Data,comments_data):

    client = MongoClient("localhost", 27017)

    db = client['DataSyncPro_2']
    collection_names = db.list_collection_names('youtube')

    # Find the first available name by appending a number
    new_collection_name = keyword
    counter = 1
    while new_collection_name in collection_names:
        new_collection_name = f"{keyword}{counter}"
        counter += 1 
        
    # Create the collection with the new name
    collection = db[new_collection_name]
    Channel_Data = {"_id":f"{new_collection_name}-Channel","Channels_Data":channel_Data}
    collection.insert_one(Channel_Data)
    
    
    
    def update_mysql(channel_data,video_df,comment_df):
    
    channel_id = channel_data['Channel_id']
    
    video_df = pd.DataFrame(video_df)
    comment_df = pd.DataFrame(comment_df)
    
    mydb = mysql.connector.connect(host='localhost',user='root',
                                   password='Dhanush@2003',database ='DataSyncPro_2')
    
    cursor = mydb.cursor()
    
    ch_schema = """
        CREATE TABLE IF NOT EXISTS Channel_Table (
            Channel_Id VARCHAR(30),
            Channel_Name VARCHAR(40),
            Playlist_Id VARCHAR(50),
            Created_Date DATETIME,
            Subscribers BIGINT,
            Total_Views BIGINT,
            Total_Videos BIGINT
        )
    """
    
    vi_schema = """
        CREATE TABLE IF NOT EXISTS Videos_Table (
            Channel_Id VARCHAR(30),
            Video_Id VARCHAR(30),
            Video_Title VARCHAR(100),
            Uploaded_Date DATETIME,
            Total_Views BIGINT,
            Total_Likes BIGINT,
            Total_Comments BIGINT
        )
    """
    
    comm_schema = """
        CREATE TABLE IF NOT EXISTS Comments_Table (
            Channel_Id VARCHAR(30),
            Video_Id VARCHAR(30),
            Video_Title VARCHAR(100),
            Comments VARCHAR(200),
            Replies VARCHAR(200)
        )
    """
    
    cursor.execute(ch_schema)
    cursor.execute(vi_schema)
    cursor.execute(comm_schema)
    
    cursor.execute("""
        INSERT IGNORE INTO Channel_Table (Channel_Id, Channel_Name, Playlist_Id, Created_Date, Subscribers, Total_Views, Total_Videos)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        channel_data['Channel_id'],
        channel_data['Channel_Name'],
        channel_data['Playlist_id'],
        channel_data['Created_Date'],
        channel_data['Subcribers'],
        channel_data['TotalViews'],
        channel_data['TotalVideos']
    ))
    

    
    for vindex, vrow in video_df.iterrows():
        cursor.execute("""
            INSERT IGNORE INTO Videos_Table (Channel_Id, Video_Id, Video_Title, Uploaded_Date, Total_Views, Total_Likes, Total_Comments)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            channel_id,
            vrow['Video_id'],
            vrow['Video_Title'],
            vrow['Uploaded_Date'],
            vrow['Total_Views'],
            vrow['Total_Likes'],
            vrow['Total_Comments']
        ))
    
        
        video_id = vrow['Video_id']
        
        for cindex, crow in comment_df[(comment_df['Channel_id'] == channel_id) & (comment_df['Video_id'] == vrow['Video_id']) ].iterrows():
            cursor.execute("""
                INSERT IGNORE INTO Comments_Table (Channel_Id, Video_Id, Video_Title, Comments, Replies)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                channel_id,
                video_id,
                crow['Video_title'],
                crow['Comments'],
                crow['Replies']
            ))
    
    

    mydb.commit()
    cursor.close()
    mydb.close()
    
    
    st.set_page_config(page_title = 'Youtube Data Harvesting' , layout='wide')

colT1,colT2 = st.columns([10,28])
with colT2:
    st.title(' :blue[Youtube Data Harvesting]') 
    
st.markdown("<h3 style='text-align: center;'>&#x1F680 This app was created for harvest the data from the youbetube &#x1F680; </h3>", unsafe_allow_html=True)

colS1,colS2 = st.columns([1,5])
with colS2:
    link = st.text_input(" :orange[Enter The Youtube Channel link]")
    
colB1,colB2 = st.columns([10,10])
with colB2:
    searc = st.button('search')

if searc:
    if link:
        channel_id = get_channel_id(link)
        channel_data = get_Channel_Information(channel_id)
        Channel_link = channel_data['Channel_link']
        
        video_id = get_video_ids(channel_data['Playlist_id']) 

        comments_data = get_comments_data(video_id)
        comment_df = convert_comment_df(comments_data)

        get_video_info = get_video_info(video_id)


        st.markdown("---")
        
        colc1,colc2,colc3,colc4 = st.columns([7,7,7,10])
    
        with colc4:
            st.image(channel_data['Thumbnail'], width=200)
        
        with colc1:
            st.subheader(" :orange[Channel Name]")
            st.markdown(f"##### {channel_data['Channel_Name']}")
            
            st.subheader(' :orange[Channel link]' )
            link_html = f"<div style='text-align: left;'><a href={Channel_link} target='_blank'>Channel Link</a></div>"
            st.markdown(link_html, unsafe_allow_html=True)
            
        with colc3:
            st.subheader(" :orange[Total Subcribers]")
            st.markdown(f"##### {channel_data['Subcribers']}")
            
            st.subheader(" :orange[Total Views]")
            st.markdown(f"##### {channel_data['TotalViews']}")
            
        with colc2:
            st.subheader(" :orange[Created Date]")
            cre = channel_data['Created_Date'].split("T")[0]
            st.markdown(f"##### {cre}")
            
            st.subheader(" :orange[Total Videos]")
            st.markdown(f"##### {channel_data['TotalVideos']}")
        
        st.markdown("---")
        
        st.dataframe(pd.DataFrame(get_video_info).drop(['Channel_id','Video_id'],axis=1), width=1200, height=200)
        st.dataframe(pd.DataFrame(comment_df).drop(['Channel_id','Video_id'],axis=1), width=1200, height=200)
        
        colB1,colB2,colB3 = st.columns([5,7,10])
        
        with colB3:
            mang_bt = st.button(' :green[ upload to mangodb ]',on_click = mdb_insert(channel_data['Channel_Name'],channel_data,pd.DataFrame(get_video_info),pd.DataFrame(comment_df)))

        with colB2:
            sql_bt = st.button(' :green[ upload to sqldb ]',on_click=update_mysql(channel_data,get_video_info,comment_df))
        
        
        st.markdown("---")