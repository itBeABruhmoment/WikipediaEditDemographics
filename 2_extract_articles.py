import pandas as pd

df = pd.read_csv("./articles/category_articles.csv")
df = df[["article_url"]]
df = df["article_url"].drop_duplicates()
df.to_csv("./articles/articles.csv", index=False)