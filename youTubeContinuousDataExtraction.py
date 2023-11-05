# pip install --upgrade google-auth google-auth-httplib2 google-api-python-client
# ! pip install --upgrade google-auth google-auth-httplib2 google-api-python-client

import googleapiclient.discovery
import googleapiclient.errors

import pandas as pd
import time
from tqdm import tqdm
import math

API_KEY = "AIzaSyCOkXSnBaaENUhdYE2CFyVV8FHnmCbTDZU"

allVideoData = pd.DataFrame()


apiServiceName = "youtube"
apiVersion = "v3"

# Get credentials and create an API client
youtube = googleapiclient.discovery.build(apiServiceName, apiVersion, developerKey=API_KEY)

# ./YouTube/collectedData/youtubeVideoIds.csv
allVideoData = pd.read_csv('./YouTube/collectedData/allVideoIDs.csv')

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
    # print(len(video_ids))
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute() 

        index = 0
        print(f"\tPage: {(i//50)+1}/{math.ceil(len(video_ids)/50)}")
        # for video in response['items']:
        for _ in tqdm(range(len(response['items']))):
            video_info = {}
            video_info['dataCollectionDate'] = timeStr
            # video_info['channelID'] = video['snippet']['channelId']
            video_info['videoID'] = video_ids[i+index]
            stats_to_keep = {'statistics': ['viewCount', 'likeCount', 'commentCount',],
                            }

            for k in stats_to_keep.keys():
                for v in stats_to_keep[k]:
                    try:
                        video_info[v] = response['items'][_][k][v]
                    except:
                        video_info[v] = None

            index += 1

            all_video_info.append(video_info)
            
    return all_video_info

if __name__ == "__main__":
    allVideoData = allVideoData['videoID'].tolist()

    print("\nCollecting YOUTUBE Daily Data\n")
    allVideoInfo = getVideoStatistics(youtube, allVideoData)

    allVideoInfo = pd.DataFrame(allVideoInfo)
    timeStr = time.strftime("%Y%m%d-%H%M%S")
    # TOGGLE INDEX accordingly...
    allVideoInfo.to_csv("./YouTube/collectedData/dailyData/{}.csv".format(timeStr), index=False, encoding="utf-8")
