from flask import Flask, render_template, request
from flask_cors import cross_origin
from googleapiclient.discovery import build
from bs4 import BeautifulSoup
import requests
import re
import json
import os
import pymongo
import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from pytube import YouTube
import boto3
import time
import concurrent.futures

app = Flask(__name__)


@app.route('/', methods=['GET'])  # route to display the home page
@cross_origin()
def homePage():
    return render_template("index.html")


@app.route('/review', methods=['POST', 'GET'])  # route to show the review comments in a web UI
@cross_origin()
def index():
    if request.method == 'POST':

        t01 = time.perf_counter()

        api_key = os.environ.get("API_KEY")

        url = request.form['content']
        soup = BeautifulSoup(requests.get(url).content, "html.parser")
        data = re.search(r"var ytInitialData = ({.*});", str(soup.prettify())).group(1)
        json_data = json.loads(data)

        channel_id = json_data['header']['c4TabbedHeaderRenderer']['channelId']

        youtube = build('youtube', 'v3', developerKey=api_key)

        def get_playlist_id(youtube, channel_id):
            try:
                request = youtube.channels().list(
                    part='snippet,content_details',
                    id=channel_id
                )
                response = request.execute()

                playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                channelname = response['items'][0]['snippet']['title']

                return playlist_id, channelname

            except Exception as e:
                print("Unable to get playlist, Error:" , e)

        playlist_id = get_playlist_id(youtube, channel_id)[0]
        channelname = get_playlist_id(youtube, channel_id)[1]

        def get_video_ids(youtube, playlist_id):
            try:
                request = youtube.playlistItems().list(
                    part='contentDetails',
                    playlistId=playlist_id,
                    maxResults=50)

                response = request.execute()

                video_ids = []

                for i in range(len(response['items'])):
                    video_ids.append(response['items'][i]['contentDetails']['videoId'])

                return video_ids

            except Exception as e:
                print("Unable to get video IDs, Error:" , e)

        video_ids = get_video_ids(youtube, playlist_id)

        def get_video_data(youtube, video_ids):
            try:
                request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=",".join(video_ids))

                response = request.execute()

                all_video_details = []

                for video in response['items']:
                    video_details = dict(video_link='https://www.youtube.com/watch?v=' + str(video['id']),
                                         likes_count=video['statistics']["likeCount"],
                                         comments_count=video['statistics']["commentCount"],
                                         video_title=video['snippet']["title"],
                                         thumbnail_url=video['snippet']["thumbnails"]['high']['url'],
                                         channelname=channelname
                                         )

                    all_video_details.append(video_details)

                return all_video_details

            except Exception as e:
                print("Unable to process video data, Error:" , e)

        channel_video_data = get_video_data(youtube, video_ids)

        s3 = boto3.client(
            service_name='s3',
            region_name=os.environ.get("AWS_REGION"),
            aws_access_key_id=os.environ.get("AWS_S3_ACCESSID"),
            aws_secret_access_key=os.environ.get("AWS_S3_SECRET")
        )

        t1 = time.perf_counter()

        def download_and_upload_videos(vid):
            try:
                yt = YouTube("https://www.youtube.com/watch?v=" + vid)
                file = f"{yt.title}.mp4"
                stream = yt.streams.first()
                # uploading all files in a folder to s3 bucket
                s3.upload_file(stream.download('Downloads/'), 'youtube-scraper1', 'Videos/' + file)

                print(f"Video {vid} has been processed")
                link = s3.generate_presigned_url(ClientMethod='get_object',
                                                 Params={'Bucket': 'youtube-scraper1',
                                                         'Key': 'Videos/' + file}, ExpiresIn=3600)
                return link
            except Exception as e:
                print("Error occurred while downloading/uploading videos, Error:" , e)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(download_and_upload_videos, video_ids)

        url_list_internal = []
        for result in results:
            url_dict = dict(sharable_video_url=result)
            url_list_internal.append(url_dict)

        t2 = time.perf_counter()

        print(f"Finished video uploads in {t2 - t1} seconds")

        def get_comments_details_forsql(youtube, video_ids):
            all_comments = []
            for video in video_ids:
                try:
                    request = youtube.commentThreads().list(part="snippet",
                                                            order="relevance",
                                                            maxResults=50,
                                                            videoId=video)

                    response = request.execute()
                    comments_in_video = [comment['snippet']['topLevelComment']['snippet']['textOriginal'] for comment in
                                         response['items']]
                    commentor_name = [comment['snippet']['topLevelComment']['snippet']['authorDisplayName'] for comment
                                      in response['items']]
                    comments_in_video_info = {'video_id': video, 'comments': comments_in_video,
                                              'commentor_name': commentor_name}

                    all_comments.append(comments_in_video_info)

                except:
                    # When error occurs - most likely because comments are disabled on a video
                    print('Could not get comments for video ' + video)
                    comments_in_video_info = {'video_id': video, 'comments': "No comment on this video",
                                              'commentor_name': None}
                    all_comments.append(comments_in_video_info)

            return all_comments

        all_comments = get_comments_details_forsql(youtube, video_ids)

        # combining all data into a single list of dictionaries
        combined_list = []
        for i, d1 in enumerate(channel_video_data):
            for j, d2 in enumerate(all_comments):
                for k, d3 in enumerate(url_list_internal):
                    if i == j & j == k:
                        d4 = {**d1, **d2, **d3}
                        combined_list.append(d4)

        def get_comments_details_mongodb(youtube, video_ids):
            all_comments_mongo = []
            for video in video_ids:
                try:
                    request = youtube.commentThreads().list(part="snippet",
                                                            order="relevance",
                                                            maxResults=50,
                                                            videoId=video)

                    response = request.execute()

                    for comment in response['items']:
                        comment_in_video = comment['snippet']['topLevelComment']['snippet']['textOriginal']
                        commentor_name = comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
                        commentor_thumnail_url = comment['snippet']['topLevelComment']['snippet'][
                            'authorProfileImageUrl']
                        comments_in_video_info = {'video_id': video, 'comments': comment_in_video,
                                                  'commentor_name': commentor_name,
                                                  'commenter_thumbnail_url': commentor_thumnail_url}

                        all_comments_mongo.append(comments_in_video_info)

                except:
                    # When error occurs - most likely because comments are disabled on a video
                    print('Could not get comments for video ' + video)
                    comments_in_video_info = {'video_id': video, 'comments': "No comment on this video",
                                              'commentor_name': None,
                                              'commenter_thumbnail_url': None}
                    all_comments_mongo.append(comments_in_video_info)

            return all_comments_mongo

        # Exporting all comments to MongoDB
        print("Starting Mongo Export...")
        try:
            client = pymongo.MongoClient(os.environ.get("MONGO_DB_LINK"))
            print("connection established with Mongo DB")
            records = get_comments_details_mongodb(youtube, video_ids)
            database = client['Youtuber_data']
            collection = database[channelname]
            collection.insert_many(records)
            print("Mongo Export Completed!")

        except Exception as e:
            print("Unable to export to MongoDB, Error:", e)

        # Exporting all video data to snowflake
        try:
            engine = create_engine(URL(
                account=os.environ.get('SNOWFLAKE_ACCOUNT'),
                user=os.environ.get('SNOWFLAKE_USERID'),
                password=os.environ.get('SNOWFLAKE_PWD'),
                database='YOUTUBE_SCRAPE_DATA',
                schema='PROJECT_SCHEMA',
                warehouse='youtube_data'))

            df = pd.DataFrame(combined_list)
            df['likes_count'] = pd.to_numeric(df['likes_count'])
            df['comments_count'] = pd.to_numeric(df['comments_count'])
            df = df.drop('comments', axis=1)
            df = df.drop('commentor_name', axis=1)
            df = df.drop('sharable_video_url',axis=1)

            connection = engine.connect()
            df.to_sql("data", con=engine, if_exists='replace', index=False, index_label=None)

            print("Data inserted into snowflake!")

            connection.close()
            engine.dispose()
            print("Engine and connection closed!")

        except Exception as e:
            print("Error:", e)

        t02 = time.perf_counter()
        print('Program run complete!')
        print(f'Finished whole run in {t02-t01} seconds')
        return render_template('results.html', combined_list=combined_list[0:(len(combined_list))])


if __name__ == "__main__":
    app.run()
