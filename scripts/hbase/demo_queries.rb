# The same access patterns as `python -m urbanflow.hbase.query demo`, in the HBase shell.
# Run by `make hbase-query`:  hbase shell -n /app/scripts/hbase/demo_queries.rb

status 'simple'
describe 'urbanflow:demand'

# Point get: one row by its full key, then only one column family of it.
get 'urbanflow:demand', 'JFK Airport#weekday#17'
get 'urbanflow:demand', 'JFK Airport#weekday#17', {COLUMN => 'm'}

# Prefix scan: every weekday hour at JFK. Zero-padded hours come back in order.
scan 'urbanflow:demand', {ROWPREFIXFILTER => 'JFK Airport#weekday#', COLUMNS => ['m:trips'], LIMIT => 6}

# The same kind of prefix restriction written in HBase's filter language.
scan 'urbanflow:demand', {FILTER => "PrefixFilter('Times Sq/Theatre District#weekend#2')", COLUMNS => ['m:trips']}

# Model lookup: predicted minutes for Manhattan -> Queens, weekday, 08:00, 10 miles.
get 'urbanflow:duration_pred', 'Manhattan#Queens#weekday#08#10.0'
scan 'urbanflow:duration_pred', {ROWPREFIXFILTER => 'Manhattan#Queens#weekday#08#', LIMIT => 5}

# Range scan: first week of March (STOPROW is exclusive).
scan 'urbanflow:daily_kpis', {STARTROW => '2025-03-01', STOPROW => '2025-03-08', COLUMNS => ['k:trips', 'k:revenue']}

# Row counts, and how the pre-split table is laid out in regions.
count 'urbanflow:demand', INTERVAL => 5000, CACHE => 1000
count 'urbanflow:duration_pred', INTERVAL => 20000, CACHE => 5000
count 'urbanflow:daily_kpis'
list_regions 'urbanflow:duration_pred'
exit
