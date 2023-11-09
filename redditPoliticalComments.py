import requests
import requests.auth
import pandas as pd
import time
from datetime import datetime, timedelta, date
import os
from tqdm import tqdm

homeDirectory = "/home/uno/Desktop/dataCollection/dataExtractionProject"

HOST = "localhost"
USER = "root"
PASSWORD = "root"
DATABASE_NAME = "dataScienceTestDatabase"


CLIENT_ID = 'UeN31barzQmS-CROFvlqkQ'
SECRET_KEY = "73q3fyrrOR9CPvSqVTON0zkyAmM4cw"

clientAuth = requests.auth.HTTPBasicAuth(CLIENT_ID, SECRET_KEY)

dataPost = {"grant_type": "password", "username": "New_Lengthiness9629", "password": "Obstacle@53"}
headers = {"User-Agent": "MyAPP"}
response = requests.post("https://www.reddit.com/api/v1/access_token", auth=clientAuth, data=dataPost, headers=headers)
TOKEN = response.json()['access_token']
headers = {**headers, **{'Authorization': f'bearer {TOKEN}'}}

redditFilters = ["author_fullname","link_title","body",]
redditFiltersName = ["userID","title","body",]

def scrapeData(subReddit, category, totalPages, afterPage, limit, timeLimit, index, tempCurrentData):
    requestURL = f"https://oauth.reddit.com/r/{subReddit}/{category}.json?after={afterPage}"
    res = requests.get(requestURL, headers=headers)
    totalPages += 1
    elseLimit = 20

    try:
        afterPage = res.json()['data']['after']
    except:
        afterPage = None

    allData = res.json()['data']['children']
    for i in range(len(allData)):
        currentData = {}
        if allData[i]['data']['created_utc'] > timeLimit:
            for j in range(len(redditFilters)):
                try:
                    currentData[redditFiltersName[j]] = allData[i]['data'][redditFilters[j]].strip()
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


current_time = datetime.now()
new_time = current_time - timedelta(minutes=10)
timeLimit = new_time.timestamp()

timeStr = time.strftime("%Y%m%d-%H%M%S")



if __name__ == "__main__":
    print("\nCollecting Daily REDDIT Data")
    allCurrentData = pd.concat([scrapeData("politics", "comments", totalPages, "", limit, timeLimit, index, [])])
    # print(allCurrentData.drop_duplicates(subset="title"))

    allCurrentData.to_csv(homeDirectory + f"/Reddit/collectedData/politics/comments/{timeStr}_{allCurrentData.shape[0]}.csv", index=False, encoding="utf-8")
    