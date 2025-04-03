import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

df = pd.read_csv("summary.csv")
total = np.sum(df["num_contrib"])

named_count = np.sum(df["named_num"])
anon_count = np.sum(df["anon_num"])
print("percent anon count", anon_count / (anon_count + named_count) * 100)

named_diff = np.sum(df["named_contrib"])
anon_diff = np.sum(df["anon_contrib"])
print("percent anon diff", anon_diff / (anon_diff + named_diff) * 100)

print("percent no anon", df[df["anon_num"] == 0].shape[0] / df.shape[0] * 100)