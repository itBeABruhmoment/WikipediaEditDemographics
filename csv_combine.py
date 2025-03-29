import pandas as pd
import os
import re
import numpy as np

# https://superuser.com/questions/202818/what-regular-expression-can-i-use-to-match-an-ip-address
ipv4_match = re.compile("[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}")
ipv6_match = re.compile("(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))")

def list_files(directory_path):
    """
    Get a list of all file names in a specified directory.
    
    :param directory_path: Path to the directory
    :return: List of file names
    """
    try:
        # Use os.listdir() to get all entries in the directory
        files = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]
        return files
    except FileNotFoundError:
        print(f"Directory not found: {directory_path}")
        return []
    except PermissionError:
        print(f"Permission denied to access directory: {directory_path}")
        return []

# pd.read_csv()

# With empty lists and dtypes
df_per_page = pd.DataFrame({
    "url": pd.Series([], dtype=pd.StringDtype()),
    "num_contrib": pd.Series([], dtype=pd.Int64Dtype()),
    "time_diff_avg": pd.Series([], dtype="timedelta64[s]"),
    "anon_num": pd.Series([], dtype=pd.Int64Dtype()),
    "named_num": pd.Series([], dtype=pd.Int64Dtype()),
    "anon_contrib": pd.Series([], dtype=pd.Int64Dtype()),
    "named_contrib": pd.Series([], dtype=pd.Int64Dtype()),
    "anon_diff_avg": pd.Series([], dtype=pd.Float64Dtype()),
    "named_diff_avg": pd.Series([], dtype=pd.Float64Dtype()),
})

def process_and_save(file_name, df_per_page):
    # rev_id,timestamp,user,comment,size,tags
    print(file_name)
    df = pd.read_csv(file_name, dtype={
        "url":pd.StringDtype(),
        "rev_id":pd.Int64Dtype(),
        "timestamp":pd.StringDtype(),
        "user":pd.StringDtype(),
        "comment":pd.StringDtype(),
        "size":pd.Int64Dtype(),
        "tags":pd.StringDtype()
    })

    # diff stats
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["size_diff"] = df["size"] - df["size"].shift(-1)
    df["time_diff"] = df["timestamp"] - df["timestamp"].shift(-1)
    df = df.iloc[:-1]

    # match ip addresses
    df["is_anon"] = df["user"].str.match(ipv4_match) | df["user"].str.match(ipv6_match)

    # article summary stats
    df_by_anon = df[["is_anon", "size_diff"]]
    df_by_anon["size_diff"] = np.abs(df["size_diff"])
    df_by_anon["amount"] = 1
    df_by_anon = df_by_anon.groupby(["is_anon"]).sum()

    per_page_summary = {
        "url": df.iloc[0]["url"],
        "num_contrib": df.shape[0],
        "time_diff_avg": df["time_diff"].mean(),
        "anon_num": df_by_anon.loc[True, "amount"],
        "named_num":  df_by_anon.loc[False, "amount"],
        "anon_contrib": df_by_anon.loc[True, "size_diff"],
        "named_contrib": df_by_anon.loc[False, "size_diff"],
        "anon_diff_avg": df_by_anon.loc[True, "size_diff"] / df_by_anon.loc[True, "amount"],
        "named_diff_avg": df_by_anon.loc[False, "size_diff"] / df_by_anon.loc[False, "amount"],
    }

    # whois
    df = df[df["is_anon"] == True]

    return pd.DataFrame([per_page_summary])

    # print(df_by_anon.loc[True]["size_diff"])
    print(per_page_summary)
    # df.groupby(["is_anon"]).sum()
    # print(df)
    # df.to_csv("test.csv")

df_per_page = process_and_save("./wikipedia_histories/Knapsack_problem.csv", df_per_page)
print(df_per_page)
# print(list_files("./wikipedia_histories"))