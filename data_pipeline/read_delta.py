from deltalake import DeltaTable

dt = DeltaTable("data/delta/transactions")
df = dt.to_pandas()

print("Total rows stored in Delta Lake:", len(df))
print("\nColumns:", df.columns.tolist())
print("\nSample rows:")
print(df.head())
print("\nSuspicious transactions stored:", df["is_laundering"].sum())