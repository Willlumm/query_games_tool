# Script to rename files in case of change to naming scheme.
# William Lumme

import os
import re

def format_date(date):
    return f"{date[0:4]}-{date[4:6]}-{date[6:8]}"

for filename in os.listdir("Weekly/Archive"):
    match = re.fullmatch(r"(.*)_(\d{8})-(\d{8}).txt", filename)
    if match:
        print(match.group(1), match.group(2), match.group(3))
        os.rename(f"Weekly/Archive/{filename}", f"Weekly/Archive/{match.group(1)}_{format_date(match.group(2))}_to_{format_date(match.group(3))}.txt")