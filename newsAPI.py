import requests
import pandas as pd
import time
from time import sleep
from datetime import datetime, timedelta
from tqdm import tqdm

homeDirectory = "/home/uno/Desktop/dataCollection/dataExtractionProject"

API_KEY = "24ad8975b73e43529bac14562c9f9ad1"
# API_KEY = "1655e6c7c4884394bec1b847d94c0568"

today = datetime.today().strftime("%Y-%m-%d")
yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
print(yesterday, today)
timeStr = time.strftime("%Y-%m-%d %H:%M:00")


def getNews(totalMessages, pageNumber, allData, celebName):
    url = f'https://newsapi.org/v2/everything?q={celebName.replace(" ","+")}&from={yesterday}&sortBy=popularity&language=en&page={pageNumber}&apiKey={API_KEY}'
    # print(url)
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        # print(len(data['articles']))
        for i in range(len(data['articles'])):
            if data['articles'][i]['description'] != None:
                if data['articles'][i]['title'] != "[Removed]" and any(_ in data['articles'][i]['description'] for _ in celebName.split(" ")):
                    tempData = {"dataCollectionDate" : timeStr,
                                "celebName" : celebName,
                                "sourceName" : data['articles'][i]['source']['name'].strip(),
                                "title" : data['articles'][i]['title'].strip(),
                                "description" : data['articles'][i]['description'].strip(),
                                "content" : data['articles'][i]['content'].strip(),
                                "publishedAt" : data['articles'][i]['publishedAt'].replace("T"," ").replace("Z",""),}
                    allData.append(tempData)
        totalMessages += len(data['articles'])
        if totalMessages >= data['totalResults']: return allData
        else: getNews(totalMessages, pageNumber+1, allData, celebName)
    else:
        print(f'Error: Unable to fetch data. Status code: {response.status_code}')
    return allData

celebNames = ["Kanye West", "gorillaz", "Kendrick Lamar", "Frank Ocean", "Beatles", "Pink Floyd", "Daft Punk", "Eminem", "Taylor Swift", "Death Grips", "GratefulDead", "ToolBand", "Brockhampton", "OFWGKTA", "Metallica", "Lanadelrey", "The Weeknd", "XXXTENTACION", "Coldplay", "Lady Gaga", "FallOutBoy", "Kid Cudi", "David Bowie", "PRINCE", "Michael Jackson", "rollingstones", "Fleetwood Mac", "ACDC", "Blink182", "Chance The Rapper", "Arctic Monkeys", "twenty one pilots",]


if __name__ == "__main__":
    finalNews = pd.DataFrame()
    print("\n\nCollecting Data from NEWS API")
    for _ in tqdm(range(len(celebNames))):
        currentData = getNews(0, 1, [], celebNames[_])
        if currentData != []:
            finalNews = pd.concat([finalNews, pd.DataFrame(currentData)], ignore_index=True)
    finalNews.to_csv(homeDirectory + "/News/collectedData/" + time.strftime("%Y%m%d-%H%M%S") + ".csv", index=False)
        

