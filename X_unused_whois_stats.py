import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

df = pd.read_csv("./whois_results.csv", dtype={
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
})



df = df[pd.to_datetime(df["timestamp"]) >= "2020-01-01"]
# print(df[df["country"] == "CN"][["user", "timestamp"]])

df[df["country"] == "CN"].to_csv("cn.csv")

df_contrib_by_county = df[["country", "size_diff"]].copy()
df_contrib_by_county["count"] = 1
df_contrib_by_county["size_diff"] = np.abs(df_contrib_by_county["size_diff"])
df_contrib_by_county["country"] = df_contrib_by_county["country"].str.upper()

df_contrib_by_county = df_contrib_by_county.groupby("country").sum()
diff_sum = np.sum(df_contrib_by_county["size_diff"])
count_sum = np.sum(df_contrib_by_county["count"])
df_contrib_by_county["diff_percent"] = df_contrib_by_county["size_diff"] / diff_sum * 100
df_contrib_by_county["count_percent"] = df_contrib_by_county["count"] / count_sum * 100
df_contrib_by_county = df_contrib_by_county.sort_values(by=["size_diff"], ascending=False)
df_contrib_by_county.to_csv("by_country.csv", index=True)

# Create pie charts
# fig, axes = plt.subplots(1, 1, figsize=(14, 6))

df_top = df_contrib_by_county.head(10)
df_other = df_contrib_by_county.iloc[10:].sum()

labels_count = df_top.index.tolist() + ["Other"]
values_count = df_top["count"].tolist() + [df_other["count"]]

labels_size = df_top.index.tolist() + ["Other"]
values_size = df_top["size_diff"].tolist() + [df_other["size_diff"]]

plt.pie(values_size, labels=labels_size, autopct="%1.1f%%", startangle=140)
plt.title("Countries by Diff Whois")

# axes[1].pie(values_size, labels=labels_size, autopct="%1.1f%%", startangle=140)
# axes[1].set_title("Top 10 Countries by Size Difference")

plt.savefig("countries_by_diff.png")

df_mask = df[["inet", "size_diff"]].copy().dropna()
df_mask["size_diff"] = np.abs(df_mask["size_diff"])
df_mask["count"] = 1

df_mask = df_mask.groupby("inet").sum()
df_mask = df_mask.sort_values(by=["count"], ascending=False)
df_mask.to_csv("by_mask.csv", index=True)

df_ip = df[["user"]].copy().dropna()
df_ip["count"] = 1
df_ip = df_ip.groupby("user").count().sort_values(by=["count"], ascending=False)
df_ip.to_csv("by_ip.csv")
print(df_ip)
