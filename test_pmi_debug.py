from datetime import date, timedelta
import requests
from bs4 import BeautifulSoup
import re

url = "https://www.pmi.spglobal.com/Public/Release/ReleaseDates"

print("\n=== FETCHING PAGE ===\n")

r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)

print("STATUS:", r.status_code)
print("CONTENT LENGTH:", len(r.text))

print("\n=== RAW HTML (first 1000 chars) ===\n")
print(r.text[:1000])

print("\n=== PARSED TEXT (first 2000 chars) ===\n")

soup = BeautifulSoup(r.text, "html.parser")
text = soup.get_text(" ", strip=True)

print(text[:2000])

print("\n=== CHECK KEYWORDS ===\n")
print("Eurozone in text:", "Eurozone" in text)
print("PMI in text:", "PMI" in text)

print("\n=== TRY REGEX MATCH ===\n")

pmi_re = re.compile(
    r"(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+\d{1,2}\s+\d{4}.*?Eurozone.*?PMI",
    flags=re.I,
)

matches = list(pmi_re.finditer(text))

print("Matches found:", len(matches))

for m in matches[:5]:
    print("MATCH:", m.group(0))