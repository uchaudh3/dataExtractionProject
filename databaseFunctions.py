import databaseFunctions
import pandas as pd
import mysql.connector
import datetime
from time import sleep
import pandas as pd
import csv
from os import listdir


HOST = "localhost"
USER = "root"
PASSWORD = "root"
DATABASE_NAME = "dataScienceTestDatabase"

YT_COLLECTED_DATA_LOCATION = "./YouTube/collectedData/"
YT_DAILY_FILE_LOCATION = "./YouTube/collectedData/dailyData/"

YT_COMMENTS_FILE_LOCATION = "./YouTube/collectedData/comments/"

REDDIT_COLLECTED_DATA_LOCATION = "./Reddit/collectedData/"


NEWS_DAILY_FILE_LOCATION = "./News/collectedData/"

CHANNEL_DETAILS_COLLECTION = "allChannelDetails"
VIDEO_IDS_COLLECTION = "allVideoIDs"
FIRST_DATA_COLLECTION = "firstDataCollection"

youTubeTableStructure = {CHANNEL_DETAILS_COLLECTION: "(id INT AUTO_INCREMENT PRIMARY KEY, channelName VARCHAR(50), channelID VARCHAR(30), subscriberCount BIGINT UNSIGNED, totalViews BIGINT UNSIGNED, totalVideos INT UNSIGNED, playlistID VARCHAR(50));", 
                  VIDEO_IDS_COLLECTION: "(channelID VARCHAR(30), videoID VARCHAR(50) NOT NULL PRIMARY KEY, channelTitle VARCHAR(50), videoTitle TEXT, uploadDate DATE, tags MEDIUMTEXT , duration SMALLINT UNSIGNED);", 
                  FIRST_DATA_COLLECTION: "(dataCollectionDate DATE, videoID VARCHAR(50) PRIMARY KEY, viewCount BIGINT UNSIGNED, likeCount INT UNSIGNED, commentCount INT UNSIGNED);",
                  "dailyData": "(id INT AUTO_INCREMENT PRIMARY KEY, dataCollectionDate DATE, videoID VARCHAR(50), viewCount BIGINT UNSIGNED, likeCount INT UNSIGNED, commentCount INT UNSIGNED);",}

youTubeDataStructure = {CHANNEL_DETAILS_COLLECTION: "(channelName, channelID, subscriberCount, totalViews, totalVideos, playlistID) VALUES (%s,%s,%s,%s,%s,%s);", VIDEO_IDS_COLLECTION: "(channelID, videoID, channelTitle, videoTitle, uploadDate, tags, duration) VALUES (%s,%s,%s,%s,%s,%s,%s);", FIRST_DATA_COLLECTION: "(dataCollectionDate, videoID, viewCount, likeCount, commentCount) VALUES (%s,%s,%s,%s,%s);", "dailyData": "(dataCollectionDate, videoID, viewCount, likeCount, commentCount) VALUES (%s,%s,%s,%s,%s);","comment": "(commentID, videoID, comment, publishedAt) VALUES (%s,%s,%s,%s);",}

redditTableStructure = {"reddit": "(id INT AUTO_INCREMENT PRIMARY KEY, dataCollectionDate DATE, publishedAt DATE, postID VARCHAR(50), userID VARCHAR(50), title LONGTEXT, upvoteRatio FLOAT UNSIGNED, ups INT UNSIGNED, totalComments INT UNSIGNED, url TEXT);",
                        "redditSubscriberCount" : "(id INT AUTO_INCREMENT PRIMARY KEY, dataCollectionDate DATE, subredditID VARCHAR(50), subreddit VARCHAR(50), subredditSubscribers BIGINT UNSIGNED);",}

redditDataStructure = {"reddit":  "(dataCollectionDate, publishedAt ,postID, userID, title, upvoteRatio, ups, totalComments, url) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);",
                       "redditSubscriberCount": "(dataCollectionDate, subredditID, subreddit, subredditSubscribers) VALUES (%s,%s,%s,%s);"}

newsTableStructure = {"news": "(id INT AUTO_INCREMENT PRIMARY KEY, dataCollectionDate DATE, celebName VARCHAR(50), sourceName VARCHAR(50), title LONGTEXT, description LONGTEXT, content LONGTEXT, publishedAt DATE);",}

newsDataStructure = {"news": "(dataCollectionDate, celebName, sourceName, title, description, content, publishedAt) VALUES (%s,%s,%s,%s,%s,%s,%s);",}

def createTables(tableName, myCursor, inputStructure):
    try:
        # databaseFunctions.deleteTable(channelID, myCursor)
        myCursor.execute("CREATE TABLE " + tableName + inputStructure)
        print("Created table {} Successfully.\n".format(tableName))
        
    except:
        print("Error while creating {} Table.\n".format(tableName))
        # databaseFunctions.deleteTable(channelID, myCursor)

def deleteTables(tableName, myCursor):
    try:
        myCursor.execute("DROP TABLE " + tableName + ";")
        print("Dropped table {} Successfully.\n".format(tableName))
        
    except:
        print("Error while deleting {} Table.\n".format(tableName))

def getAllData(tableName, myCursor):
    counter = 0
    thisData = []
    myCursor.execute("SELECT * FROM " + tableName + ";")
    for _ in myCursor: 
        print(_)
        thisData.append(_)
        counter += 1
    print("\nTotal {} rows of data is in the {}.\n".format(counter, tableName))
    return thisData

def createChannelTables(allChannelIDs, myCursor):
    for channelID in allChannelIDs:
        channelID = ''.join(c for c in channelID if c.isalpha())
        try:
            # databaseFunctions.deleteTable(channelID, myCursor)
            myCursor.execute("CREATE TABLE " + ''.join(c for c in channelID if c.isalpha()) + " (id INT AUTO_INCREMENT PRIMARY KEY, dataCollectionDate DATE, videoID VARCHAR(50), viewCount BIGINT UNSIGNED, likeCount INT UNSIGNED, commentCount INT UNSIGNED);")
            print("Created table {} Successfully.\n".format(channelID))
            
        except:
            print("Error while creating {} Table.\n".format(channelID))
            # databaseFunctions.deleteTable(channelID, myCursor)

def deleteChannelTables(allChannelIDs, myCursor):
    for channelID in allChannelIDs:
        channelID = ''.join(c for c in channelID if c.isalpha())
        try:
            myCursor.execute("DROP TABLE " + channelID + ";")
            print("Dropped table {} Successfully.\n".format(channelID))
            
        except:
            print("Error while deleting {} Table.\n".format(channelID))
            # databaseFunctions.deleteTable(channelID, myCursor)

def createCommentsTables(allVideoIds, myCursor):
    for videoID in allVideoIds:
        videoID = ''.join(c for c in videoID if c.isalpha()) + "Comments"
        try:
            # databaseFunctions.deleteTable(videoID, myCursor)
            myCursor.execute("CREATE TABLE " + ''.join(c for c in videoID if c.isalpha()) + " (id INT AUTO_INCREMENT PRIMARY KEY, commentID VARCHAR(50), videoID VARCHAR(50), comment LONGTEXT, publishedAt DATE);")
            print("Created table {} Successfully.\n".format(videoID))
            
        except:
            print("Error while creating {} Table.\n".format(videoID))
            # databaseFunctions.deleteTable(videoID, myCursor)
def deleteCommentsTables(allVideoIds, myCursor):
    for videoID in allVideoIds:
        videoID = ''.join(c for c in videoID if c.isalpha()) + "Comments"
        try:
            myCursor.execute("DROP TABLE " + videoID + ";")
            print("Dropped table {} Successfully.\n".format(videoID))
            
        except:
            print("Error while deleting {} Table.\n".format(videoID))
            # databaseFunctions.deleteTable(videoID, myCursor)

def addSingleData(tableName, myCursor, data, dataStructure):
    myCursor.execute("INSERT INTO " + tableName + dataStructure , data)

def appendCSVData(fileName, tableName, myCursor, youTubeTableStructure):
    appended = 0
    print("Appending data from {} to {}".format(fileName.split("/")[-1], tableName))
    with open(fileName, newline='', encoding="utf-8") as csvfile:
        csvData = csv.reader(csvfile)
        next(csvData, None)

        for row in csvData:
            # print(row)
            addSingleData(tableName, myCursor, row, youTubeTableStructure)
            appended += 1
    print("Appended {} rows of data from {} to {}, SUCCESSFULLY.\n".format(appended, fileName.split("/")[-1], tableName))

def getLastFile(directory):
    allFiles = listdir(directory)
    csvFiles = list(filter(lambda f: f.endswith('.csv'), allFiles))
    csvFiles.sort()
    return csvFiles

def getRedditLastFiles():
    lastUpdatedFiles = listdir(REDDIT_COLLECTED_DATA_LOCATION)
    lastUpdatedFiles.sort()
    lastUpdatedFilesName = lastUpdatedFiles[-1]
    return listdir(REDDIT_COLLECTED_DATA_LOCATION + lastUpdatedFilesName), lastUpdatedFilesName

def getCommentLastFiles():
    lastUpdatedFiles = listdir(YT_COMMENTS_FILE_LOCATION)
    lastUpdatedFiles.sort()
    lastUpdatedFilesName = lastUpdatedFiles[-1]
    return listdir(YT_COMMENTS_FILE_LOCATION + lastUpdatedFilesName), lastUpdatedFilesName

def updateYouTubeDailyData(youTubeDataStructure):
    allVideoIDs = pd.read_csv(YT_COLLECTED_DATA_LOCATION + VIDEO_IDS_COLLECTION + ".csv")

    with open(YT_DAILY_FILE_LOCATION + getLastFile(YT_DAILY_FILE_LOCATION)[-1], newline='', encoding="utf-8") as csvfile:
        csvData = csv.reader(csvfile)
        next(csvData, None)

        for row in csvData:
            tempChannelID = allVideoIDs[allVideoIDs["videoID"] == row[1]].channelID.item().strip()
            addSingleData(''.join(c for c in tempChannelID if c.isalpha()), myCursor, row, youTubeDataStructure)
    print("Updated Daily Data to the Database")

def updateYouTubeCommentData(youTubeDataStructure):
    lastUpdatedFiles, lastUpdatedFilesName = getCommentLastFiles()
    for data in lastUpdatedFiles:
        tableName = data.replace(".csv", "")
        with open(YT_COMMENTS_FILE_LOCATION + lastUpdatedFilesName + "/" + data, newline='', encoding="utf-8") as csvfile:
            csvData = csv.reader(csvfile)
            next(csvData, None)
            for row in csvData:
                addSingleData(tableName, myCursor, row, youTubeDataStructure)
        print("Updated Daily Data to the Database")

def updateRedditDailyData():
    lastUpdatedFiles, lastUpdatedFilesName = getRedditLastFiles()
    for data in lastUpdatedFiles:
        tableName = data.replace(".csv", "")
        with open(REDDIT_COLLECTED_DATA_LOCATION + lastUpdatedFilesName + "/" + data, newline='', encoding="utf-8") as csvfile:
            csvData = csv.reader(csvfile)
            next(csvData, None)

            for row in csvData:
                if tableName == "redditSubscriberCount":
                    addSingleData(tableName, myCursor, row, redditDataStructure["redditSubscriberCount"])
                else:
                    addSingleData(tableName, myCursor, row, redditDataStructure["reddit"])
        print("Updated Daily Data to the Database")

def updateDailyNewsData():
    # print(getLastFile(NEWS_DAILY_FILE_LOCATION))
    currentFile = getLastFile(NEWS_DAILY_FILE_LOCATION)[-1]
    with open(NEWS_DAILY_FILE_LOCATION + currentFile, newline='', encoding="utf-8") as csvfile:
        csvData = csv.reader(csvfile)
        next(csvData, None)

        for row in csvData:
            addSingleData("news", myCursor, row, newsDataStructure["news"])   
    print("Updated Daily Data to the Database")
        

        

def createAllYouTubeTables():
    createChannelTables(allChannelIDs, myCursor)
    createTables(FIRST_DATA_COLLECTION, myCursor, youTubeTableStructure[FIRST_DATA_COLLECTION])
    createTables(CHANNEL_DETAILS_COLLECTION, myCursor, youTubeTableStructure[CHANNEL_DETAILS_COLLECTION])
    createTables(VIDEO_IDS_COLLECTION, myCursor, youTubeTableStructure[VIDEO_IDS_COLLECTION])
    createTables("dailyData", myCursor, youTubeTableStructure["dailyData"])

def deleteAllYouTubeTables():
    deleteTables(FIRST_DATA_COLLECTION, myCursor)
    deleteTables(CHANNEL_DETAILS_COLLECTION, myCursor)
    deleteTables(VIDEO_IDS_COLLECTION, myCursor)
    deleteChannelTables(allChannelIDs, myCursor)
    deleteTables("dailyData", myCursor)

def createAllRedditTables():
    lastUpdatedFiles, lastUpdatedFilesName = getRedditLastFiles()
    for data in lastUpdatedFiles:
        tableName = data.replace(".csv", "")
        if tableName == "redditSubscriberCount":
            createTables(tableName, myCursor, redditTableStructure["redditSubscriberCount"])
        else:
            createTables(tableName, myCursor, redditTableStructure["reddit"])
            
def deleteAllRedditTables():
    lastUpdatedFiles, lastUpdatedFilesName = getRedditLastFiles()
    for data in lastUpdatedFiles:
        tableName = data.replace(".csv", "")
        deleteTables(tableName, myCursor)
        

if __name__=='__main__':
    allChannelIDs = pd.read_csv(YT_COLLECTED_DATA_LOCATION + '{}.csv'.format(CHANNEL_DETAILS_COLLECTION))
    allChannelIDs = allChannelIDs["channelID"].values.tolist()

    database = mysql.connector.connect(host=HOST, user=USER, password=PASSWORD, database=DATABASE_NAME)
    if database.is_connected():
        print("Database connection is Successful.\n")
    myCursor = database.cursor()


    # myCursor.execute(f"CREATE DATABASE {DATABASE_NAME};")


    # # CREATE & DELETE TABLES FOR YOUTUBE COMMENTS
    # allVideoData = pd.read_csv('./YouTube/collectedData/allVideoIDs.csv')
    # allVideoData = allVideoData['videoID'].tolist()
    # deleteCommentsTables(allVideoData, myCursor)
    # createCommentsTables(allVideoData, myCursor)

    # # # ADD COMMENTS TO DATABASE
    # updateYouTubeCommentData(youTubeDataStructure["comment"])



    # # CREATE & DELETE TABLES FOR YOUTUBE
    # deleteAllYouTubeTables()
    # createAllYouTubeTables()


    # # ADD INITIAL YOUTUBE DATA TO THE TABLE
    # appendCSVData(YT_COLLECTED_DATA_LOCATION + CHANNEL_DETAILS_COLLECTION + '.csv', CHANNEL_DETAILS_COLLECTION, myCursor, youTubeDataStructure[CHANNEL_DETAILS_COLLECTION])
    # appendCSVData(YT_COLLECTED_DATA_LOCATION + VIDEO_IDS_COLLECTION + '.csv', VIDEO_IDS_COLLECTION, myCursor, youTubeDataStructure[VIDEO_IDS_COLLECTION])
    # appendCSVData(YT_COLLECTED_DATA_LOCATION + FIRST_DATA_COLLECTION + '.csv', FIRST_DATA_COLLECTION, myCursor, youTubeDataStructure[FIRST_DATA_COLLECTION])

    
    # # UPDATE YOUTUBE DAILY SCRAPED DATA
    # updateYouTubeDailyData(youTubeDataStructure["dailyData"])

    # database.commit()

    # # # # ----------------------------------------------------------------- #


    # # CREATE & DELETE TABLES FOR REDDIT
    # deleteAllRedditTables()
    # createAllRedditTables()


    # # UPDATE REDDIT DAILY SCRAPED DATA
    # updateRedditDailyData()
        

    # # # # ----------------------------------------------------------------- #
    
    # # CREATE & DELETE TABLES FOR NEWS
    # deleteTables("news", myCursor)
    # createTables("news", myCursor, newsTableStructure['news'])

    # # UPDATE DAILY NEWS DATA
    # updateDailyNewsData()

    

    database.commit()
    database.close()
