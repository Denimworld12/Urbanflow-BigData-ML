"""Opt-in HBase serving layer: gold tables copied into HBase for keyed lookups.

keys.py  row-key design (pure, unit-tested)
store.py Thrift connection + table/column-family layout
load.py  data/gold Parquet -> HBase
query.py gets, prefix scans, range scans and counts
"""
