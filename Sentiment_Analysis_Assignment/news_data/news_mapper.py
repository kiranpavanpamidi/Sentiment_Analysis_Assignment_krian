#!/usr/bin/python3

from textblob import TextBlob
import sys

header = True

for line in sys.stdin:

    if header:
        header = False
        continue

    text = line.strip()

    if not text:
        continue

    polarity = TextBlob(text).sentiment.polarity

    if polarity > 0:
        print("Positive\t1")
    elif polarity < 0:
        print("Negative\t1")
    else:
        print("Neutral\t1")
