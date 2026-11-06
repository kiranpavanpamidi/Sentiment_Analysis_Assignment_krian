import requests
import pandas as pd

API_KEY = "********Add your API KEY *******"

url = f"https://newsapi.org/v2/everything?q=technology&pageSize=100&apiKey={API_KEY}"

response = requests.get(url)
data = response.json()

rows = []

for article in data["articles"]:
    title = article["title"]
    rows.append([title])

df = pd.DataFrame(rows, columns=["text"])

df.to_csv("news_data.csv", index=False)

print("Saved", len(df), "headlines")
