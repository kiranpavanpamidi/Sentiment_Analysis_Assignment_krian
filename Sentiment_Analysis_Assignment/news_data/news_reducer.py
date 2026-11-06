#!/usr/bin/python3

import sys

current = None
count = 0

for line in sys.stdin:

    key, value = line.strip().split("\t")
    value = int(value)

    if key == current:
        count += value
    else:
        if current:
            print(f"{current}\t{count}")

        current = key
        count = value

if current:
    print(f"{current}\t{count}")
