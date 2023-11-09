import requests
import requests.auth
import pandas as pd
from datetime import datetime
import time
from datetime import timedelta, date
import os
from tqdm import tqdm

homeDirectory = "/home/uno/Desktop/dataCollection/dataExtractionProject"

HOST = "localhost"
USER = "root"
PASSWORD = "root"
DATABASE_NAME = "dataScienceTestDatabase"

PRIMARY_FILE_LOCATION = "/Reddit/collectedData/"
DAILY_FILE_LOCATION = "/Reddit/collectedData/dailyData/"

SUBSCRIBER_COUNT = "redditSubscriberCount"


CLIENT_ID = 'UeN31barzQmS-CROFvlqkQ'
SECRET_KEY = "73q3fyrrOR9CPvSqVTON0zkyAmM4cw"

clientAuth = requests.auth.HTTPBasicAuth(CLIENT_ID, SECRET_KEY)

dataPost = {"grant_type": "password", "username": "New_Lengthiness9629", "password": "Obstacle@53"}
headers = {"User-Agent": "MyAPP"}
response = requests.post("https://www.reddit.com/api/v1/access_token", auth=clientAuth, data=dataPost, headers=headers)
TOKEN = response.json()['access_token']
headers = {**headers, **{'Authorization': f'bearer {TOKEN}'}}

redditFilters = ["subreddit_id","subreddit","subreddit_subscribers","created_utc","id","author_fullname","title","upvote_ratio","ups", "num_comments","url",]
redditFiltersName = ["subredditID","subreddit","subredditSubscribers","created","postID","userID","title","upvoteRatio","ups", "totalComments","url",]
# redditFilters = ["subreddit_id", "subreddit","subreddit_subscribers","created_utc","id","author_fullname","title","over_18","upvote_ratio","content_categories","ups", "num_comments","selftext","url",]

afterPage = ""
allCurrentData = pd.DataFrame()

nameIDandSubscribers = []


def scrapeData(subReddit, category, totalPages, afterPage, limit, timeLimit, index, tempCurrentData):
    requestURL = "https://oauth.reddit.com/r/{}/{}.json?after={}".format(subReddit, category, afterPage)
    res = requests.get(requestURL, headers=headers)
    totalPages += 1
    elseLimit = 20
    timeStr = time.strftime("%Y%m%d-%H0000")

    try:
        afterPage = res.json()['data']['after']
    except:
        afterPage = None

    allData = res.json()['data']['children']
    for i in range(len(allData)):
        currentData = {}
        if allData[i]['data']['created_utc'] > timeLimit:
            currentData['dataCollectionDate'] = datetime.strptime(timeStr, "%Y%m%d-%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
            for j in range(len(redditFilters)):
                try:
                    if redditFilters[j] == "created_utc":
                        currentData["publishedAt"] = datetime.fromtimestamp(allData[i]['data'][redditFilters[j]]).strftime('%Y-%m-%d %H:%M:%S') 
                    else:
                        currentData[redditFiltersName[j]] = allData[i]['data'][redditFilters[j]]
                except:
                    currentData[redditFiltersName[j]] = "NA"

            tempCurrentData.append(currentData)
        else:
            elseLimit -= 1
            if elseLimit <= 0:
                # print("All data from the given date has been collected, TERMINATING the PROCESS\n")
                return pd.DataFrame(tempCurrentData)


    # print("Collected {} reddit post data, turning to NEXT PAGE".format((totalPages)*25))
    if totalPages*25 >= limit or afterPage == None:
        return pd.DataFrame(tempCurrentData)
    

    
    scrapeData(subReddit, category, totalPages, afterPage, limit, timeLimit, index, tempCurrentData)
    return pd.DataFrame(tempCurrentData)


totalPages = 0

limit = 1000
index = 0


subReddits = ["Kanye", "gorillaz", "KendrickLamar", "FrankOcean", "beatles", "pinkfloyd", "DaftPunk", "Eminem", "TaylorSwift", "deathgrips", "gratefuldead", "ToolBand", "brockhampton", "OFWGKTA", "Metallica", "lanadelrey", "TheWeeknd", "XXXTENTACION", "Coldplay", "LadyGaga", "FallOutBoy", "KidCudi", "DavidBowie", "PRINCE", "MichaelJackson", "rollingstones", "FleetwoodMac", "ACDC", "Blink182", "ChanceTheRapper", "arcticmonkeys", "twentyonepilots",]
# subReddits = ["Kanye", "gorillaz", "KendrickLamar", "FrankOcean", "beatles"]

aliasNames = ["Kanye", "gorillaz", "KendrickLamar", "FrankOcean", "beatles", "pinkfloyd", "DaftPunk", "Eminem", "TaylorSwift", "deathgrips", "gratefuldead", "ToolBand", "brockhampton", "OFWGKTA", "Metallica", "lanadelrey", "TheWeeknd", "XXXTENTACION", "Coldplay", "LadyGaga", "FallOutBoy", "KidCudi", "DavidBowie", "PRINCE", "MichaelJackson", "rollingstones", "FleetwoodMac", "ACDC", "Blink182", "ChanceTheRapper", "arcticmonkeys", "twentyonepilots",]


newDate = date.today() - timedelta(days=1)
newDate = str(newDate).strip().split("-")
print(newDate)
newDate = datetime(int(newDate[0]), int(newDate[1]), int(newDate[2]))
timeLimit = (time.mktime(newDate.timetuple()))
timestr = time.strftime("%Y%m%d-%H%M%S")
print(timeLimit)

os.mkdir(os.path.join(homeDirectory + PRIMARY_FILE_LOCATION, timestr))


newDF = pd.DataFrame()
column_names=["dataCollectionDate", "subredditID", "subreddit", "subredditSubscribers"]


if __name__ == "__main__":
    if len(subReddits) == len(aliasNames):
        print("\n\nCollecting Daily REDDIT Data")
        for i in tqdm(range(len(subReddits))):
            allCurrentData = pd.concat([scrapeData(subReddits[i], "hot", totalPages, afterPage, limit, timeLimit, index, [])])
            # print(allCurrentData.drop_duplicates(subset="title"))

            try:
                temp = allCurrentData[["dataCollectionDate", "subredditID", "subreddit", "subredditSubscribers"]].iloc[0]
            except Exception as e:
                temp = {"dataCollectionDate": "NA", "subredditID": "NA", "subreddit": "NA", "subredditSubscribers": "NA"}

            temp_df = pd.DataFrame([temp])
            newDF = pd.concat([newDF, temp_df], ignore_index=True)

            try:
                allCurrentData.drop(["subredditID", "subreddit", "subredditSubscribers"], axis=1, inplace=True)

                allCurrentData.to_csv(homeDirectory + "/Reddit/collectedData/{}/{}.csv".format(timestr, aliasNames[i].lower()), index=False, encoding="utf-8")
            except Exception as e:
                pass
        print(newDF)
        newDF.to_csv(homeDirectory + "/Reddit/collectedData/{}/{}.csv".format(timestr, SUBSCRIBER_COUNT), index=False, encoding="utf-8")



