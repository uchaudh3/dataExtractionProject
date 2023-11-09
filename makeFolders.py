import os

homeDirectory = "/home/uno/Desktop/dataCollection/dataExtractionProject"


def makeAllFolders():
    folderNames = ["/YouTube", "/YouTube/collectedData", "/YouTube/collectedData/comments", "/YouTube/collectedData/dailyData", "/Reddit", "/Reddit/collectedData", "/Reddit/collectedData/comments", "/Reddit/collectedData/politics", "/Reddit/collectedData/politics/posts", "/Reddit/collectedData/politics/comments", "/News", "/News/collectedData"]
    for folderName in folderNames:
        try:
            os.mkdir(folderName)
        except:
            print(f"Already Exists {folderName}")




makeAllFolders()


# subReddits = ["Kanye", "gorillaz", "KendrickLamar", "FrankOcean", "beatles", "pinkfloyd", "DaftPunk", "Eminem", "TaylorSwift", "deathgrips", "gratefuldead", "ToolBand", "brockhampton", "OFWGKTA", "Metallica", "lanadelrey", "TheWeeknd", "XXXTENTACION", "Coldplay", "LadyGaga", "FallOutBoy", "KidCudi", "DavidBowie", "PRINCE", "MichaelJackson", "rollingstones", "FleetwoodMac", "ACDC", "Blink182", "ChanceTheRapper", "arcticmonkeys", "twentyonepilots",]

# celebNames = ["Kanye West", "gorillaz", "Kendrick Lamar", "Frank Ocean", "Beatles", "Pink Floyd", "Daft Punk", "Eminem", "Taylor Swift", "Death Grips", "GratefulDead", "ToolBand", "Brockhampton", "OFWGKTA", "Metallica", "Lanadelrey", "The Weeknd", "XXXTENTACION", "Coldplay", "Lady Gaga", "FallOutBoy", "Kid Cudi", "David Bowie", "PRINCE", "Michael Jackson", "rollingstones", "Fleetwood Mac", "ACDC", "Blink182", "Chance The Rapper", "Arctic Monkeys", "twenty one pilots",]

# channelIDs = ["UCs6eXM7s8Vl5WcECcRHc2qQ", "UCfIXdjDQH9Fau7y99_Orpjw", "UCZwYLLsXM2rBtixxFAdYR1A", "UCqf-kTp9ERV5T1rPayno7LA", "UCc4K7bAqpdBP8jh1j9XZAww", "UCY2qt3dw2TQJxvBrDiYGHdQ", "UC_kRDKYrUlrbtrSiyu5Tflg", "UCfM3zsQsOnfWNUppiycmBuw", "UCqECaJ8Gagnn7YCbPEzWH6g", "UCuq1H-HXWoW4JL-hX5bWxzw", "UCPuuuhmMW7jh6roOrIV9yRw", "UC1wUo-29zS7m_Jp-U_xYcFQ", "UCFLnwFhuJeBSCjIJewxSqKw", "UC7V34pJZN9v7J1eLp4uq9Jg", "UCbulh9WdLtEXiooRcYK7SWw", "UCqk3CdGN_j8IR9z4uBbVPSg", "UC0WP5P-ufpRfjbNrmOWwLBQ", "UCM9r1xn6s30OnlJWb-jc3Sw", "UCDPM_n1atn2ijUwHd0NNRQw", "UCNL1ZadSjHpjm4q9j2sVtOA", "UC2qWxZHgnlwDvcmLqP23jrA", "UCoNPsL8j28yfKRu6e7YUhPA", "UC8YgWcDKi1rLbQ1OtrOHeDw", "UCv3mNSNjuWldihk1DUdnGtw", "UC5OrDvL9DscpcAstz7JnQGA", "UCB_Z6rBg3WW3NL4-QimhC2A", "UCAb60rVrvVQVfSgrX1UWb0g", "UCB0JSO6d5ysH2Mmqz5I9rIw", "UCdvlHk5SZWwr9HjUcwtu8ng", "UCeXp3EC97_rUl_e2vgM3gLg", "UC-KTRBl9_6AX10-Y7IKwKdw", "UCBQZwaNPFfJ1gZ1fLZpAEGw",]


# print(len(subReddits))
# print(len(celebNames))
# print(len(channelIDs))