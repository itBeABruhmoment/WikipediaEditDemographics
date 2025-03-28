import requests

S = requests.Session()

URL = "https://www.mediawiki.org/w/api.php"

PARAMS = {
    "action": "query",
    "prop": "revisions",
    "titles": "API|Main Page",
    "rvprop": "timestamp|user|comment|content",
    "rvslots": "main",
    "formatversion": "2",
    "format": "json"
}

R = S.get(url=URL, params=PARAMS)
DATA = R.json()

PAGES = DATA["query"]["pages"]

for page in PAGES:
    print(page["revisions"])