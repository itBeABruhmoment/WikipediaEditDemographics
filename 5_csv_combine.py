import os
import pandas as pd

def append_csv_files(folder_path, output_file):
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    
    if not csv_files:
        print("No CSV files found in the folder.")
        return
    
    df_list = [pd.read_csv(os.path.join(folder_path, file)) for file in csv_files]
    combined_df = pd.concat(df_list, ignore_index=True)
    
    combined_df.to_csv(output_file, index=False)
    print(f"Combined CSV saved to {output_file}")

append_csv_files("./whois_results", "./whois_results.csv")
append_csv_files("./summaries", "./summary.csv")