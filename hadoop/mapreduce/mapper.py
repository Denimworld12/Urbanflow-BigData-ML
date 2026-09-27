#!/usr/bin/env python
"""MapReduce MAP step: one input line = one trip.

Input  (from Hive's text extract): borough TAB zone TAB pickup_hour
Output (to the shuffle):           borough TAB zone TAB pickup_hour TAB 1

Hadoop sorts and groups everything the mappers print by the KEY, which the job
sets to the first three fields (stream.num.map.output.key.fields=3), so every
"1" for the same zone and hour arrives at the same reducer, next to each other.

Written to run on both Python 2.7 (what the apache/hadoop NodeManager image
ships) and Python 3, using only the standard library.
"""
import sys


def main():
    for line in sys.stdin:
        fields = line.rstrip("\n").split("\t")
        if len(fields) != 3:
            sys.stderr.write("reporter:counter:UrbanFlow,MalformedLines,1\n")
            continue
        borough, zone, hour = fields
        sys.stdout.write("%s\t%s\t%s\t1\n" % (borough, zone, hour))


if __name__ == "__main__":
    main()
