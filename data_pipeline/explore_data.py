import pandas as pd

df = pd.read_csv("data/HI-Small_Trans.csv")

print("Shape (rows, columns):", df.shape)
print("\nColumn names:", df.columns.tolist())
print("\nFirst 5 rows:")
print(df.head())
print("\nNumber of laundering transactions:", df["Is Laundering"].sum())