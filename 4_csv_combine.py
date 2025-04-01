# TODO: attempt to get state/province working
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd
import os
import re
import numpy as np
import whois
import subprocess
import sys

# https://superuser.com/questions/202818/what-regular-expression-can-i-use-to-match-an-ip-address
ipv4_match = re.compile("[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}")
ipv6_match = re.compile("(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))")

def run_whois(ip):
    """
    Runs the 'whois' command on the given IP address and parses the output into a dictionary.

    :param ip: The IP address to look up
    :return: A dictionary with parsed whois data
    """
    try:
        # Run the whois command
        result = subprocess.run(["whois", ip], capture_output=True, text=True, timeout=10)
        output = result.stdout
        
        # Parse output into a dictionary
        whois_data = {}
        for line in output.splitlines():
            if ":" in line:
                key, value = line.split(":", 1)
                key = str.lower(key.strip())
                
                if not (key in whois_data):
                    whois_data[key] = value.strip()

        country = None
        if "country" in whois_data:
            country = whois_data["country"]

        org = None
        if "org" in whois_data:
            org = whois_data["org"]
        elif "orgid" in whois_data:
            org = whois_data["orgid"]
        elif "netname" in whois_data:
            org = whois_data["netname"]
        elif "ownerid" in whois_data:
            org = whois_data["ownerid"]
    
        inet = None
        if "inetnum" in whois_data:
            inet = whois_data["inetnum"]
        elif "cidr" in whois_data:
            inet = whois_data["cidr"]
        elif "netrange" in whois_data:
            inet = whois_data["netrange"]

        return pd.Series([country, org, inet])

    except subprocess.CalledProcessError as e:
        print(f"Error running whois: {e}")
        return pd.Series([None, None, None])
    except subprocess.TimeoutExpired as e:
        print(f"Error running whois: {e} {ip}")
        return pd.Series([None, None, None])

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

def process_and_save(file_path, df_per_page):
    # rev_id,timestamp,user,comment,size,tags
    print(f"Processing {file_path}")
    try:
        df = pd.read_csv(file_path, dtype={
            "url": pd.StringDtype(),
            "rev_id": pd.Int64Dtype(),
            "timestamp": pd.StringDtype(),
            "user": pd.StringDtype(),
            "comment": pd.StringDtype(),
            "size": pd.Int64Dtype(),
            "tags": pd.StringDtype()
        })

        # diff stats
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["size_diff"] = df["size"] - df["size"].shift(-1)
        df["time_diff"] = df["timestamp"] - df["timestamp"].shift(-1)
        
        if df.shape[0] <= 1:
            print(f"Not enough revisions in {file_path}")
            return df_per_page
            
        df = df.iloc[:-1]

        # match ip addresses
        df["is_anon"] = df["user"].str.match(ipv4_match) | df["user"].str.match(ipv6_match)

        # article summary stats
        df_by_anon = df[["is_anon", "size_diff"]].copy()
        df_by_anon["size_diff"] = np.abs(df_by_anon["size_diff"])
        df_by_anon["amount"] = 1
        df_by_anon = df_by_anon.groupby(["is_anon"]).sum()

        # Initialize with default values
        anon_num = 0
        named_num = 0
        anon_contrib = 0
        named_contrib = 0
        anon_diff_avg = 0
        named_diff_avg = 0

        # Check if True exists in index before accessing
        if True in df_by_anon.index:
            anon_num = df_by_anon.loc[True, "amount"]
            anon_contrib = df_by_anon.loc[True, "size_diff"]
            anon_diff_avg = anon_contrib / anon_num if anon_num > 0 else 0

        # Check if False exists in index before accessing
        if False in df_by_anon.index:
            named_num = df_by_anon.loc[False, "amount"]
            named_contrib = df_by_anon.loc[False, "size_diff"]
            named_diff_avg = named_contrib / named_num if named_num > 0 else 0

        per_page_summary = {
            "url": df.iloc[0]["url"] if not df.empty else "",
            "num_contrib": df.shape[0],
            "time_diff_avg": df["time_diff"].mean() if not df.empty else pd.Timedelta(0),
            "anon_num": anon_num,
            "named_num": named_num,
            "anon_contrib": anon_contrib,
            "named_contrib": named_contrib,
            "anon_diff_avg": anon_diff_avg,
            "named_diff_avg": named_diff_avg,
        }

        anon_df = df[df["is_anon"] == True].copy()
        if anon_df.shape[0] > 0:
            # deduplicate to reduce number of whois calls
            unique_ips = anon_df[["user"]].drop_duplicates()
            # Run whois on unique IPs
            unique_ips[["country", "org", "inet"]] = unique_ips["user"].apply(lambda ip: run_whois(ip))
            
            # Merge the whois data back to the original dataframe
            anon_df = anon_df.merge(unique_ips, on="user", how="left")
            
            # Create a directory for whois results if it doesn't exist
            os.makedirs("whois_results", exist_ok=True)
            
            # Save the whois results to a file with a name based on the input file
            output_file = os.path.join("whois_results", os.path.basename(file_path))
            anon_df.to_csv(output_file, index=False)
            print(f"Saved whois results to {output_file}")
        else:
            print(f"No anonymous edits in {per_page_summary['url']}")

        # return pd.concat([df_per_page if not df.empty else None, pd.DataFrame([per_page_summary])])

        return pd.concat([df_per_page, pd.DataFrame([per_page_summary])], ignore_index=True)
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return df_per_page

def main():
    # Directory containing Wikipedia history CSV files
    wiki_dir = "wikipedia_histories"
    
    # Check if directory exists
    if not os.path.exists(wiki_dir):
        print(f"Directory '{wiki_dir}' does not exist. Creating it...")
        os.makedirs(wiki_dir)
        print(f"Please place your CSV files in the '{wiki_dir}' directory and run the script again.")
        return
    
    # Get all CSV files from the directory
    csv_files = [f for f in list_files(wiki_dir) if f.lower().endswith('.csv')]
    csv_files.sort()
    
    if not csv_files:
        print(f"No CSV files found in '{wiki_dir}' directory.")
        return
    
    print(f"Found {len(csv_files)} CSV files to process.")
    
    # Initialize the dataframe for per-page summaries
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
    
    start = int(sys.argv[1])
    end = int(sys.argv[2])

    # Process each file and accumulate results
    for i in range(start, end):
        csv_file = csv_files[i]
        file_path = os.path.join(wiki_dir, csv_file)
        df_per_page = process_and_save(file_path, df_per_page)
    
    # Save the final summary dataframe
    summary_file = f"./summaries/wikipedia_summary_{start}_{end}.csv"
    df_per_page.to_csv(summary_file, index=False)
    print(f"Summary saved to {summary_file}")

if __name__ == "__main__":
    main()