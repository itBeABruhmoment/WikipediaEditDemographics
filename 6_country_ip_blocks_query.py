import subprocess
import pandas as pd
import re
import sys
import os
import ipaddress

# git all commits of the country-ip-blocks repo into a dataframe with timestamps
# generated by ChatGPT
def get_all_commits(repo_path):
    try:
        result = subprocess.run(
            ["git", "log", "--pretty=format:%H %s"],
            cwd=repo_path,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            return None
        
        commits = []
        for line in result.stdout.split('\n'):
            parts = line.split(" ", 1)
            if len(parts) == 2:
                commit_id, message = parts
                match = re.search(r"Update (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2})", message)
                if match:
                    timestamp = match.group(1)
                    commits.append((timestamp, commit_id))
        
        df = pd.DataFrame(commits, columns=["timestamp", "commit"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.sort_values("timestamp", ascending=False)
        return df
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# run git command to reroll git repo to a specified commit
def reroll(commit, repo_path):
    _ = subprocess.run(
        ["git", "checkout", commit, "--", "."],
        cwd=repo_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

# find the country of an IP address in country-ip-blocks
# generated by ChatGPT
def find_ip_in_cidr_files(ip, base_folder):
    try:
        target_ip = ipaddress.ip_address(ip)
        folder_type = "ipv4" if target_ip.version == 4 else "ipv6"
        folder_path = os.path.join(base_folder, folder_type)

        if not os.path.exists(folder_path):
            print(f"Error: Folder '{folder_path}' does not exist.")
            return None

        for file in os.listdir(folder_path):
            if file.endswith(".cidr"):
                file_path = os.path.join(folder_path, file)
                with open(file_path, "r") as f:
                    for line in f:
                        cidr = line.strip()
                        if cidr:
                            try:
                                if target_ip in ipaddress.ip_network(cidr, strict=False):
                                    # print(f"IP {ip} found in {file}")
                                    return file
                            except ValueError:
                                print(f"Skipping invalid CIDR: {cidr} in {file}")

        print(f"IP {ip} not found in any CIDR file.")
        return None

    except ValueError:
        print(f"Error: '{ip}' is not a valid IP address.")
        return None

# get the latest commit before a timestamp
def get_previous_commit(df, timestamp):
    timestamp = pd.to_datetime(timestamp)
    previous_commit = df[df["timestamp"] < timestamp].iloc[0]
    return previous_commit["commit"]

# path to the country-ip-blocks git repo, putting it in a temp fs is recommended
repo_path = "./mnt/country-ip-blocks"
ip_df = get_all_commits(repo_path)

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

counter = 0

def query(row):
    global counter
    counter += 1
    if counter % 100 == 0:
        print(counter)

    ip = row["user"]
    time = row["timestamp"]
    # print(time, ip)
    get_previous_commit(ip_df, time)
    reroll(time, repo_path)
    return str(find_ip_in_cidr_files(ip, repo_path))

df = df[pd.to_datetime(df["timestamp"]) >= "2020-03-01"]

df = df.sort_values(by="timestamp")

df["file"] = df.apply(query, axis=1)
df.to_csv("second.csv")