# pip install --upgrade google-auth google-auth-httplib2 google-api-python-client
# ! pip install --upgrade google-auth google-auth-httplib2 google-api-python-client

import pandas as pd
import time
import requests
from datetime import datetime, timedelta
import os
from tqdm import tqdm

homeDirectory = "/home/uno/Desktop/dataCollection/dataExtractionProject"

API_KEY = "AIzaSyCOkXSnBaaENUhdYE2CFyVV8FHnmCbTDZU"


baseURL = "https://www.googleapis.com/youtube/v3/"

today = datetime.today().strftime("%Y-%m-%d")
yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

# url = "https://www.googleapis.com/youtube/v3/commentThreads?part=snippet&maxResults=100&order=relevance&videoId=Tmz1lz0zcLQ&key=AIzaSyCOkXSnBaaENUhdYE2CFyVV8FHnmCbTDZU"

# data = requests.get(url).json()

# comments = []
# print(data)

# for i in range(len(data['items'])):
#   comments.append(data['items'][i]['snippet']['topLevelComment']['snippet']['textOriginal'])

# /YouTube/collectedData/youtubeVideoIds.csv
allVideoData = pd.read_csv(homeDirectory + '/YouTube/collectedData/allVideoIDs.csv')


def getComments(index, videoID, allData, pageToken):
    part="part=snippet"
    maxResults="maxResults=100"
    videoId="videoId="+videoID

    if index >= 1:
        nextPageToken = "pageToken="+pageToken
        url = baseURL + "commentThreads?" + '&'.join([part, maxResults, videoId, nextPageToken]) + "&key=" + API_KEY
    else:
        url = baseURL + "commentThreads?" + '&'.join([part, maxResults, videoId,]) + "&key=" + API_KEY
    response = requests.get(url).json()

    index += 1
    try:
        for i in range(len(response['items'])):
            if response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'].split("T")[0].strip() == yesterday:
                data = {"commentID": response['items'][i]['id'],
                        "videoID": response['items'][i]['snippet']['videoId'],
                        "comment": response['items'][i]['snippet']['topLevelComment']['snippet']['textOriginal'].strip(),
                        "publishedAt": response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'].replace("T"," ").replace("Z",""),}
                allData.append(data)
    except:
        print("Catch")
        pass

    if index >= 5 or 'nextPageToken' not in response:
        return allData
    else:
        pageToken = response['nextPageToken']
        getComments(index, videoID, allData, pageToken)
    return allData


if __name__ == "__main__":
    allVideoData = allVideoData['videoID'].tolist()
    allVideoDataLength = len(allVideoData)
    timeStr = time.strftime("%Y%m%d-%H%M%S")
    try: os.mkdir(homeDirectory + "/YouTube/collectedData/comments/" + timeStr)
    except: print("In Except condition")
    print("\n\nCollecting YOUTUBE Comments")
    for _ in tqdm(range(allVideoDataLength)):
        allVideoInfo = getComments(0, allVideoData[_], [], "")
        if len(allVideoInfo) > 0:
            allVideoInfo = pd.DataFrame(allVideoInfo)
            # TOGGLE INDEX accordingly...
            allVideoInfo.to_csv(homeDirectory + "/YouTube/collectedData/comments/{}/{}Comments.csv".format(timeStr, ''.join(c for c in allVideoData[_] if c.isalpha())), index=False, encoding="utf-8")
        # print(f"{_} of {allVideoDataLength} Added")
