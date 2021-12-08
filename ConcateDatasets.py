#%% - Imports
import os 
import pandas as pd


# Instantiate Master dfs
dfp = pd.DataFrame(columns=['title', 'score', 'id', 'subreddit_name', 'url', 'number_comments',
                            'ups', 'downs', 'upvote_ratio', 'date_created', 'body', 'keyword'])
dfc = pd.DataFrame(columns=['comment_id', 'parent_id', 'comment_body', 'comment_link_id'])


# Read in Sep CSVs
for file in os.listdir("ScrapedFiles"): #moved all files into separate dir (were previously in same folder as .py)
    # Only Data files
    if ".csv" in file: 
        # Create sep Posts and Comments dfs
        if "Posts" in file:
            df = pd.read_csv(file)
            df.drop("Unnamed: 0", axis=1, inplace=True)
            
            # Add Topic Column (as a categorical)
            keywords = file.split("-")
            df['keyword'] = keywords[2][:-4]
            dfp = dfp.append(df)

        
        elif "Comments" in file:
            df = pd.read_csv(file)
            df.drop("Unnamed: 0", axis=1, inplace=True)

            # Add Topic Column (as a categorical)
            keywords = file.split("-")
            df['keyword'] = keywords[2][:-4]
            dfc = dfc.append(df)


print("done")


# Write out Master dfs in Concated Dir
#dfp.drop("downs", axis=1, inplace=True) #only zeroes for all dataframes

# NEED SPECIFY DIR TO CONCATED FOLDER WITH ABSOLUTE PATH NOT RELATIVE PATH
dfp.to_csv("Concated/Posts.csv", index=False)
dfc.to_csv("Concated/Comments.csv", index=False)
# %%
dfp[dfp["id"] =="pg87gt"] #try removing t3 from ids (in comments df) to match with posts
# %%
import pandas as pd 
dfp = pd.read_csv("/Volumes/GoogleDrive/My Drive/Colab/IDS 561 - Big Data/Data/Reddit/Concated/Posts.csv")
dfc = pd.read_csv("/Volumes/GoogleDrive/My Drive/Colab/IDS 561 - Big Data/Data/Reddit/Concated/Comments.csv")

print("dfp", dfp.shape)
print("dfc", dfc.shape)
# %% – Bitstamp Data

import pandas as pd 
file_path = "/Volumes/GoogleDrive/My Drive/Colab/IDS 561 - Big Data/Data/bitstampUSD_1-min_data_2012-01-01_to_2021-03-31.csv"
df = pd.read_csv(file_path)

print("df", df.shape)
# %% – CBECI Data
import pandas as pd 

data_dir = "/Volumes/GoogleDrive/My Drive/Colab/IDS 561 - Big Data/Data/CBECI"
for file in os.listdir(data_dir):
    
    path = os.path.join(data_dir, file)
    df = pd.read_csv(path)
    print(file, "-", df.shape)

    # min/max date times for each
    if 'Date and Time' in df.columns:
        print(df["Date and Time"].min(), "to", df["Date and Time"].max())
    elif 'Date' in df.columns:
        print(df["Date"].min(), "to", df["Date"].max())
    elif 'date' in df.columns:
        print(df["date"].min(), "to", df["date"].max())
# %%

# %%
