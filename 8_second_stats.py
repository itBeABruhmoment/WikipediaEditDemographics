import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

df = pd.read_csv("./second.csv", dtype={
    "url": pd.StringDtype(),
    "rev_id": pd.Int64Dtype(),
    "timestamp": pd.StringDtype(),
    "user": pd.StringDtype(),
    "comment": pd.StringDtype(),
    "size": pd.Int64Dtype(),
    "tags": pd.StringDtype(),
    "size_diff": pd.Int16Dtype(),
    "time_diff": pd.StringDtype(),
    "is_anon": pd.BooleanDtype(),
    "country": pd.StringDtype(),
    "org": pd.StringDtype(),
    "inet": pd.StringDtype(),
    "desc": pd.StringDtype(),
    "file": pd.StringDtype(),
})


def extract_country_code(filename):
    if pd.isna(filename):
        return ""
    else:
        return str.split(filename, ".")[0].upper()

# make file names have same format as whois
df["file"] = df["file"].apply(extract_country_code)
df["country"] = df["country"].str.upper()
df[df["file"] == "CN"].to_csv("cn_file.csv")

print("agree", df[df["file"] == df["country"]].shape[0] / df.shape[0])

def countries_by_diff_file(df):
    df_contrib_by_county = df[["file", "size_diff"]].copy()
    df_contrib_by_county["count"] = 1
    df_contrib_by_county["size_diff"] = np.abs(df_contrib_by_county["size_diff"])
    df_contrib_by_county = df_contrib_by_county.groupby("file").sum()

    diff_sum = np.sum(df_contrib_by_county["size_diff"])
    count_sum = np.sum(df_contrib_by_county["count"])
    df_contrib_by_county["diff_percent"] = df_contrib_by_county["size_diff"] / diff_sum * 100
    df_contrib_by_county["count_percent"] = df_contrib_by_county["count"] / count_sum * 100

    df_contrib_by_county = df_contrib_by_county.sort_values(by=["size_diff"], ascending=False)

    df_top_diff = df_contrib_by_county.head(12)
    df_other_diff = df_contrib_by_county.iloc[12:].sum()

    labels_size = df_top_diff.index.tolist() + ["Other"]
    values_size = df_top_diff["size_diff"].tolist() + [df_other_diff["size_diff"]]

    plt.pie(values_size, labels=labels_size, autopct="%1.1f", startangle=140)
    plt.title("Countries by Bytes Changed")
    plt.savefig("countries_by_diff_file.png")
    plt.close()

def countries_by_count_file(df):
    df_contrib_by_county = df[["file", "size_diff"]].copy()
    df_contrib_by_county["count"] = 1
    df_contrib_by_county["size_diff"] = np.abs(df_contrib_by_county["size_diff"])
    df_contrib_by_county = df_contrib_by_county.groupby("file").sum()

    diff_sum = np.sum(df_contrib_by_county["size_diff"])
    count_sum = np.sum(df_contrib_by_county["count"])
    df_contrib_by_county["diff_percent"] = df_contrib_by_county["size_diff"] / diff_sum * 100
    df_contrib_by_county["count_percent"] = df_contrib_by_county["count"] / count_sum * 100

    df_contrib_by_county = df_contrib_by_county.sort_values(by=["count"], ascending=False)

    df_top_diff = df_contrib_by_county.head(12)
    df_other_diff = df_contrib_by_county.iloc[12:].sum()

    labels_size = df_top_diff.index.tolist() + ["Other"]
    values_size = df_top_diff["count"].tolist() + [df_other_diff["count"]]

    plt.pie(values_size, labels=labels_size, autopct="%1.1f", startangle=140)
    plt.title("Countries by Edit Count")
    plt.savefig("countries_by_count_file.png")
    plt.close()

countries_by_diff_file(df)
countries_by_count_file(df)