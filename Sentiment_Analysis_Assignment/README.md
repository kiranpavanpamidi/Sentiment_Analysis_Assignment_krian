# Sentiment Analysis Pipeline with Social Media Data (Hadoop Streaming)

Unit 2 – Activity 3: an end-to-end sentiment analysis pipeline built with **Python** and **Hadoop Streaming (MapReduce)**. The same mapper → combiner/sort → reducer design is applied to two datasets:

| Dataset | Source | Sentiment method | Output classes |
|---|---|---|---|
| **News headlines** | NewsAPI (`technology` query) | TextBlob polarity (rule-based) | Positive / Negative / Neutral |
| **Twitter** | Labelled tweet CSV (~1.6 million tweets, 238 MB) | Existing labels (`0` = negative, `4` = positive) | Positive / Negative |

---

## Data description

### 1. News dataset (headlines)

| Property | Description |
|---|---|
| Source | NewsAPI `everything` endpoint (`q=technology`, `pageSize=100`) |
| Collected with | `news_collect.py` (Python `requests` + `pandas`) |
| File | `news_data.csv` (a copy was used as `news_data1.csv` locally and `newsdata_kiran.csv` in HDFS) |
| Size | About 70 headlines (72 in the local run), roughly 5 KB |
| Format | CSV with a header row and one column |
| Column | `text` – the headline (article title) |
| Language / topic | English, technology news |
| Labels | None. Sentiment is computed by the mapper with TextBlob (Positive / Negative / Neutral) |

Example rows:

```
text
The Science Behind the Nobel-Winning Technology That Controls Neurons With Light
Nvidia's latest GPU driver brings a new way to speed up PC games
UK Home Office May Face Investigation Over Use of OpenAI Tools
```

Notes: the free NewsAPI plan returns at most 100 articles per request, so the dataset is small and reflects a single query at one point in time. Only headlines are stored, not article bodies, authors or URLs.

### 2. Twitter dataset (tweets)

| Property | Description |
|---|---|
| Source | Pre-collected labelled tweet dataset (follows the Sentiment140 format) |
| File | `twitter_k.csv`, converted to UTF-8 as `twitter_K.csv` |
| Size | About 1.6 million tweets, 238,803,811 bytes (~238 MB) |
| Format | CSV, no header row, 6 columns, all values quoted |
| Time period | Tweets from 2009 (for example, 6 April 2009) |
| Class balance | 800,000 negative and 800,000 positive tweets |

Columns, in order:

| # | Column | Description |
|---|---|---|
| 0 | Sentiment label | `"0"` = negative, `"4"` = positive |
| 1 | Tweet ID | Numeric ID of the tweet |
| 2 | Date | Timestamp, e.g. `Mon Apr 06 22:19:45 PDT 2009` |
| 3 | Query | Search query used, here always `NO_QUERY` |
| 4 | User | Username of the author |
| 5 | Text | Tweet text, which may contain mentions, links, emoticons and slang |

Example row (shortened):

```
"0","1467810369","Mon Apr 06 22:19:45 PDT 2009","NO_QUERY","<username>","<tweet text>"
```

Notes:
- The file is ISO-8859-1 encoded and was converted to UTF-8 with `iconv`; the mapper also ignores any remaining undecodable characters.
- The dataset is balanced by design, so the class counts do not reflect the real-world share of positive and negative tweets.
- Labels in large tweet datasets may be assigned automatically, so some label noise is possible.
- The data contains usernames and tweet IDs. It is not included in this repository; obtain it from its original source and follow that source's terms of use.

---

## Repository structure

```
.
├── news/
│   ├── news_collect.py        # Collects headlines from NewsAPI -> news_data.csv
│   ├── news_mapper.py         # TextBlob polarity -> "Positive|Negative|Neutral<TAB>1"
│   └── news_reducer.py        # Sums counts per sentiment label
├── twitter/
│   ├── twitter_mapper.py      # Parses CSV, maps label 0/4 -> Negative/Positive
│   ├── twitter_combiner.py    # Local aggregation to reduce shuffle data
│   └── twitter_reducer.py     # Final totals per sentiment label
├── screenshots/               # (optional) execution screenshots
└── README.md
```

> Adjust the folder names above to match your repository.

---

## How it works

**Mapper** – reads one record per line from `stdin`, classifies sentiment, and prints `label<TAB>1`.

- *News:* skips the header and empty lines, then uses `TextBlob(text).sentiment.polarity` (`> 0` Positive, `< 0` Negative, `= 0` Neutral).
- *Twitter:* re-encodes each line with invalid characters ignored, parses it with the `csv` module, and maps the first column (`"0"` → Negative, `"4"` → Positive). Malformed lines are skipped.

**Combiner (Twitter only)** – sums the `1`s per key on each mapper's sorted output, so only a few totals are sent over the network.

**Reducer** – receives key-sorted input and prints the total count for each sentiment label.

---

## Requirements

- Python 3
- Hadoop 3.3.x with HDFS and YARN (tested with `hadoop-streaming-3.3.6.jar`)
- For the news pipeline: `requests`, `pandas`, `textblob` (and the TextBlob corpora: `python -m textblob.download_corpora`)

```bash
pip install requests pandas textblob
python -m textblob.download_corpora
```

---

## Dataset setup

### News (collected with NewsAPI)

1. Create a free API key at <https://newsapi.org>.
2. **Do not hard-code the key.** Store it in an environment variable:

   ```bash
   export NEWSAPI_KEY="your_key_here"
   ```

   and read it in `news_collect.py`:

   ```python
   import os
   API_KEY = os.environ["NEWSAPI_KEY"]
   ```

3. Run the collector:

   ```bash
   python3 news_collect.py      # writes news_data.csv (headlines in a "text" column)
   ```

### Twitter

The Twitter CSV (~238 MB) is **not included** in this repository because it exceeds GitHub's file-size limit. Use a labelled tweet dataset with the sentiment label in the first column (`0` / `4`). If the file is not UTF-8, convert it first:

```bash
iconv -f ISO-8859-1 -t UTF-8 twitter_k.csv -o twitter_K.csv
```

---

## Run locally (test with Unix pipes)

Test the scripts before running on the cluster.

**News**

```bash
cat news_data1.csv | python3 news_mapper.py | sort | python3 news_reducer.py
```

**Twitter**

```bash
cat twitter_K.csv | python3 twitter_mapper.py | sort | python3 twitter_combiner.py | sort | python3 twitter_reducer.py
```

---

## Run on Hadoop (Hadoop Streaming)

### News

```bash
hdfs dfs -mkdir /newsdata
hdfs dfs -put newsdata_kiran.csv /newsdata

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
  -files news_mapper.py,news_reducer.py,myenv.zip \
  -mapper "python3 news_mapper.py" \
  -reducer "python3 news_reducer.py" \
  -input /newsdata/newsdata_kiran.csv \
  -output /newsdata/output_k1

hdfs dfs -cat /newsdata/output_k1/part-00000
```

`myenv.zip` is a zipped Python environment that provides TextBlob to the worker nodes.

### Twitter

```bash
hdfs dfs -mkdir /twitter
hdfs dfs -put twitter_K.csv /twitter

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
  -files twitter_mapper.py,twitter_reducer.py,twitter_combiner.py \
  -mapper "python3 twitter_mapper.py" \
  -combiner "python3 twitter_combiner.py" \
  -reducer "python3 twitter_reducer.py" \
  -input /twitter/twitter_K.csv \
  -output /twitter/outputKiran

hdfs dfs -cat /twitter/outputKiran/part-00000
```

> The output directory must not already exist; delete it with `hdfs dfs -rm -r <dir>` before re-running.

---

## Results

| Run | Records | Negative | Neutral | Positive | Time |
|---|---|---|---|---|---|
| News – local | 72 | 10 | 37 | 25 | – |
| News – Hadoop | 71 processed | 10 | 37 | 24 | ~23 s |
| Twitter – local | 1,600,000 | 800,000 | – | 800,000 | – |
| Twitter – Hadoop | 1,600,000 | – | – | – | ~14 s |

- The Twitter job processed roughly 1.6 million tweets in about 14 seconds (roughly 110,000 records/second) on a small cluster.
- For the news data, fixed job start-up time dominates because the input is only about 70 headlines.

---

## Known issue

The news mapper skips the **first line it receives** as the CSV header. On Hadoop the input is divided into multiple splits (2 in this run), so the first line of the second split is also skipped. This is the likely reason for 24 Positive on Hadoop versus 25 locally. A fix is to skip only the line whose text equals the header:

```python
if text == "text":
    continue
```

---

## Ethical considerations

- **Privacy:** tweets contain usernames, IDs and timestamps; anonymise or drop them before sharing data. The Twitter pipeline here uses only the label column.
- **Credentials:** never commit API keys. Use environment variables and regenerate any key that has been exposed.
- **Bias:** TextBlob is lexicon-based and tuned for standard English, so it can misread slang, dialects and sarcasm. Results should be validated against a manually labelled sample.

---

## Future work

- Live collection with Tweepy / PRAW
- Richer preprocessing (stopwords, hashtags, URLs, emojis)
- Model comparison with accuracy / F1-score (e.g. VADER, ML classifiers)
- Spark (RDDs / DataFrames) with caching and partitioning

---

## Author

Name: Kiran Pavan Pamidi
Course: Foundations of Big Data
Enrollment No.: 260102006
