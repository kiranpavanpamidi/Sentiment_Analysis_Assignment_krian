import sys

current = None
cuont = 0

for line in sys.stdin:
	key, value = line.strip().split('\t')
	value = int(value)

	if current == key:
	  count += value
	else:
	    if current:
		print(f"{current}\t{count}")

	   current = key
	   count = value

if current:
	print(f"{current}\t{count}")
