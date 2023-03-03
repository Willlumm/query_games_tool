# Script to query digital games data for previous week from SQL server and save as text files.
# William Lumme

from datetime import date, timedelta
import pandas as pd
import sqlalchemy as db
import urllib

print("Connecting to SQL server...")
params = urllib.parse.quote_plus("<CONNECTION_STRING>")
engine = db.create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

today = date.today()
monday = today - timedelta(weeks=1, days=today.weekday())
sunday = monday + timedelta(days=6)

queries = (
    ("steam",       f"SELECT * FROM [RPT_DLP].[vwFact_GSDSteamPurchases] WHERE SalesDate BETWEEN '{monday}' AND '{sunday}' ORDER BY SalesDate"),
    ("xbox360",     f"SELECT * FROM [RPT_DLP].[vwFact_GSDPurchases_Daily] WHERE Platform = 'XBOX 360' AND DateStamp BETWEEN '{monday}' AND '{sunday}' ORDER BY DateStamp, CorrectedTitleName"),
    ("xboxone",     f"SELECT * FROM [RPT_DLP].[vwFact_GSDPurchases_Daily] WHERE Platform = 'XBOX ONE' AND DateStamp BETWEEN '{monday}' AND '{sunday}' AND CorrectedTitleName <> 'WASTELAND 3' ORDER BY DateStamp, CorrectedTitleName"),
    ("xboxseries",  f"SELECT * FROM [RPT_DLP].[vwFact_GSDPurchases_Daily] WHERE Platform = 'XBOX SERIES' AND DateStamp BETWEEN '{monday}' AND '{sunday}' ORDER BY DateStamp, CorrectedTitleName")
)

check = pd.DataFrame()
for platform, query in queries:
    try:
        print(f"Querying {platform} between {monday} and {sunday}...")
        df = pd.read_sql(query, engine, parse_dates=["DateStamp", "SalesDate"])
        date_field = "DateStamp"
        if platform == "steam":
            date_field = "SalesDate"
        check = pd.concat([check, df[[date_field, "PurchaseQuantity"]].groupby(date_field).sum().rename(columns={"PurchaseQuantity": platform})], axis=1)
        df.to_csv(f"C:/Users/a-wlumme/Desktop/GSD Digital Games Upload/Weekly/{platform}_{monday}_to_{sunday}.txt", index=False, sep="\t")
    except Exception as e:
        print(e)

print()
print(check)

print("\nFinished")
input()