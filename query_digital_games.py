# Script to query Xbox store and Steam sales for previous week from SQL server and save as text file.
# Filters Bethesda titles from Steam sales.
# William Lumme

from datetime import date, timedelta
import pandas as pd
import sqlalchemy as db
import urllib

# Connect to SQL server.
print("Connecting to SQL server...")
params = urllib.parse.quote_plus("Connection string")
engine = db.create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# Get start and end date of previous week.
today = date.today()
start = today - timedelta(weeks=1, days=today.weekday())
end = start + timedelta(days=6)

queries = (
    ("steam",       f"SELECT * FROM [RPT_DLP].[vwFact_GSDSteamPurchases] WHERE SalesDate BETWEEN '{start}' AND '{end}' ORDER BY SalesDate"),
    ("xbox360",     f"SELECT * FROM [RPT_DLP].[vwFact_GSDPurchases_Daily] WHERE Platform = 'XBOX 360' AND DateStamp BETWEEN '{start}' AND '{end}' ORDER BY DateStamp, CorrectedTitleName"),
    ("xboxone",     f"SELECT * FROM [RPT_DLP].[vwFact_GSDPurchases_Daily] WHERE Platform = 'XBOX ONE' AND DateStamp BETWEEN '{start}' AND '{end}' AND CorrectedTitleName <> 'WASTELAND 3' ORDER BY DateStamp, CorrectedTitleName"),
    ("xboxseries",  f"SELECT * FROM [RPT_DLP].[vwFact_GSDPurchases_Daily] WHERE Platform = 'XBOX SERIES' AND DateStamp BETWEEN '{start}' AND '{end}' ORDER BY DateStamp, CorrectedTitleName")
)

check = pd.DataFrame()

# Run each query.
for platform, query in queries:
    print(f"Querying {platform} between {start} and {end}...")
    df = pd.read_sql(query, engine, parse_dates=["DateStamp", "SalesDate"])
    date_field = "DateStamp"

    if platform == "steam":
        date_field = "SalesDate"

        # Check for new Steam titles and prompt to label as bethesda.
        pub_df = pd.read_csv("title_publisher.csv")
        df = df.merge(right=pub_df, how="left", on="TitleName")
        new_titles = df[df.Publisher.isna()][["TitleName", "ProductTitle", "SteamProductId"]].apply(tuple, axis=1).unique()
        print(f"\n{len(new_titles)} new titles")
        print("Is title Bethesda? (y/n)")
        for title, product, id in new_titles:
            pub = {"y": "Bethesda", "n": "Xbox"}[input(f"\n{title} {product} {id}\t").lower()]
            pub_df.loc[len(pub_df.index)] = [title, pub]
            df.loc[df.TitleName == title, "Publisher"] = pub
        pub_df.to_csv("title_publisher.csv", index=False)

        # Remove Bethesda titles.
        df = df[df.Publisher == "Xbox"].drop(columns=["Publisher"])
    
    # Show total units by day and platform for user to check.
    check = pd.concat([check, df[[date_field, "PurchaseQuantity"]].groupby(date_field).sum().rename(columns={"PurchaseQuantity": platform})], axis=1)
    df.to_csv(f"./Weekly/{platform}_{start}_to_{end}.txt", index=False, sep="\t")

print()
print(check)

print("\nFinished")
input()
