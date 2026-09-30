#!/usr/bin/env python
"""MapReduce REDUCE step: sum the 1s for each (borough, zone, hour) key.

Input arrives SORTED by key, so all lines for one key are consecutive: keep a
running total and print it whenever the key changes.

The same script is also the job's COMBINER. A combiner runs on each mapper's
own output before the shuffle, turning thousands of "zone hour 1" lines into
one "zone hour 4127" line, so far less data crosses the network. That is only
correct because addition is associative: summing partial sums gives the same
answer as summing the 1s directly.
"""
import sys


def main():
    current, total = None, 0
    for line in sys.stdin:
        borough, zone, hour, count = line.rstrip("\n").split("\t")
        key = (borough, zone, hour)
        if key != current:
            if current is not None:
                sys.stdout.write("%s\t%s\t%s\t%d\n" % (current + (total,)))
            current, total = key, 0
        total += int(count)
    if current is not None:
        sys.stdout.write("%s\t%s\t%s\t%d\n" % (current + (total,)))


if __name__ == "__main__":
    main()
