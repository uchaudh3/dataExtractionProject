# pip install --upgrade google-auth google-auth-httplib2 google-api-python-client
# ! pip install --upgrade google-auth google-auth-httplib2 google-api-python-client

import googleapiclient.discovery
import googleapiclient.errors

import pandas as pd
import time
from time import sleep
import pandas as pd
import os


API_KEY = "AIzaSyCOkXSnBaaENUhdYE2CFyVV8FHnmCbTDZU"

PRIMARY_FILE_LOCATION = "./YouTube/collectedData/"
DAILY_FILE_LOCATION = "./YouTube/collectedData/dailyData/"

channelIDs = ["UCheky_SBEExtbK_T0onuDwg","UCkT9AFJN7QIzApwhT825V7A","UCqECaJ8Gagnn7YCbPEzWH6g","UCPNxhDvTcytIdvwXWAm43cA","UCFFbwnve3yF62-tVXkTyHqg",]

CHANNEL_DETAILS_COLLECTION = "allChannelDetails"
VIDEO_IDS_COLLECTION = "allVideoIDs"
FIRST_DATA_COLLECTION = "firstDataCollection"

# To got channel ID, inspect the element and search for "externalId"
playlistIds = []    

videoDataframe = pd.DataFrame()
allYouTubeVideoIDs = pd.DataFrame()

api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=API_KEY)


def getChannelData(youtube, channelIDs):
    allData = []
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=",".join(channelIDs)
    )
    response = request.execute()
    for item in response['items']:
        # print(item)
        data = {
            'channelName' : item['snippet']['title'],
            'channelID' : item['id'],
            'subscriberCount' : item['statistics']['subscriberCount'],
            'totalViews' : item['statistics']['viewCount'],
            'totalVideos' : item['statistics']['videoCount'],
            'playlistID' : item['contentDetails']['relatedPlaylists']['uploads'],
        }
        playlistIds.append(item['contentDetails']['relatedPlaylists']['uploads'])
        allData.append(data)

    return(pd.DataFrame(allData))

def getallYouTubeVideoIDs(youtube, playlistId, nextPageToken, index, videoIds, channelID):
    request = youtube.playlistItems().list(part="snippet,contentDetails",playlistId=playlistId,maxResults=50,pageToken=nextPageToken)
    if index == 0:
        request = youtube.playlistItems().list(part="snippet,contentDetails",playlistId=playlistId,maxResults=50,)
    response = request.execute()
    nextPageToken = response.get('nextPageToken')
    if index == 0:
        channelID = response['items'][0]['snippet']['channelId']
    for item in response['items']:
        data = {}
        # print(item)
        data['channelID'] = item['snippet']['channelId']
        data['videoID'] = item['contentDetails']['videoId']
        data['channelTitle'] = item['snippet']['channelTitle']
        data['videoTitle'] = item['snippet']['title']
        data['uploadDate'] = item['contentDetails']['videoPublishedAt'][:-3].replace("T"," ") + "00"
        
        videoIds.append(data)
    index += 1
    if nextPageToken is None or index >= 2:
        return(pd.DataFrame(videoIds))
    getallYouTubeVideoIDs(youtube, playlistId, nextPageToken, index, videoIds, channelID)
    
    return(pd.DataFrame(videoIds))

def convertYouTubeTime(givenTime):
    numbers, temp, index = [0]*3, "", 2
    for i in range(len(givenTime)-2,1,-1):
        if givenTime[i].isnumeric():
            temp+=givenTime[i]
        else:
            numbers[index] = int(temp[::-1])
            index -= 1
            temp = ""
    numbers[index] = int(temp[::-1])
    # print(givenTime, numbers)
    totalSeconds = numbers[0]*60*60 + numbers[1]*60 + numbers[2]
    return totalSeconds

# convertYouTubeTime("PT10M48S")

def getVideoStatistics(youtube, video_ids):
    # Time string made with %M%S as 0000, set accordingly...
    timeStr = time.strftime("%Y-%m-%d %H:%M:00")
    all_video_info = []
    print(len(video_ids))
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute() 

        index = 0
        for video in response['items']:
            video_info = {}
            video_info['dataCollectionDate'] = timeStr
            video_info['channelID'] = video['snippet']['channelId']
            video_info['videoID'] = video_ids[i+index]
            stats_to_keep = {'snippet': ['tags',],
                             'statistics': ['viewCount', 'likeCount', 'commentCount',],
                             'contentDetails': ['duration',],
                            }

            for k in stats_to_keep.keys():
                for v in stats_to_keep[k]:
                    try:
                        video_info[v] = video[k][v]
                    except:
                        video_info[v] = None
            try:
                video_info['duration'] = convertYouTubeTime(video['contentDetails']['duration'])
            except:
                video_info['duration'] = 0

            index += 1

            all_video_info.append(video_info)
            
    return all_video_info

def youTubeDataFiltering(data):
    data = data[data['duration'] > 60]
    data = data[data['videoTitle'].str.contains("official", case=False)]
    data = data[~data['videoTitle'].str.contains("trailer|bts|visualizer", case=False)]
    print(data)
    return data


if __name__=='__main__':
    getChannelData(youtube, channelIDs).to_csv(PRIMARY_FILE_LOCATION + "{}.csv".format(CHANNEL_DETAILS_COLLECTION), index=False, encoding="utf-8")
    

    for playlistId in playlistIds:
        index = 0
        nextPageToken,channelID = "",""
        allYouTubeVideoIDs = pd.concat([allYouTubeVideoIDs, getallYouTubeVideoIDs(youtube, playlistId, nextPageToken, index, [], channelID)])
        # print(allYouTubeVideoIDs)
        

    allVideoInfo = getVideoStatistics(youtube, allYouTubeVideoIDs['videoID'].tolist())
    # print(allVideoInfo)

    allVideoInfo = pd.DataFrame(allVideoInfo)
    # print(allVideoInfo["channelId"])
    print(allVideoInfo["channelID"])
    allYouTubeVideoIDs = pd.merge(allYouTubeVideoIDs, allVideoInfo[["videoID","tags","duration"]])
    allVideoInfo.drop("tags", axis=1, inplace=True)
    allVideoInfo.drop("duration", axis=1, inplace=True)

    timeStr = time.strftime("%Y%m%d%H")



    # TOGGLE INDEX accordingly...
    finalAllYouTubeVideoIDs = youTubeDataFiltering(allYouTubeVideoIDs)
    finalAllYouTubeVideoIDs.to_csv(PRIMARY_FILE_LOCATION + "{}.csv".format(VIDEO_IDS_COLLECTION), index=False, encoding="utf-8")

    finalAllVideoInfo = finalAllYouTubeVideoIDs.merge(allVideoInfo, on="videoID", how='left')[["dataCollectionDate","videoID","viewCount","likeCount","commentCount"]].copy()
    finalAllVideoInfo.to_csv(PRIMARY_FILE_LOCATION + "{}.csv".format(FIRST_DATA_COLLECTION), index=False, encoding="utf-8")
    try:
        os.mkdir(DAILY_FILE_LOCATION)
    except:
        pass
# Tags to remove from Title
# Lyric, Live, Behind the scenes, 


