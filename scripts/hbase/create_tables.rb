# HBase shell DDL for UrbanFlow's serving tables. Safe to re-run.
# Run by `make hbase-load`:  hbase shell -n /app/scripts/hbase/create_tables.rb
# The shell is JRuby, so plain Ruby (unless/include?) works around the commands.
# (The list_namespace *command* only prints; @shell.admin.list_namespace returns the names.)

create_namespace 'urbanflow' unless @shell.admin.list_namespace.include?('urbanflow')
existing = list('urbanflow:.*')

# demand_by_zone_hour, key zone#daytype#HH.
#   m = metrics every lookup reads; kept small, cached in memory, bloom filter on
#       the row so a point get can skip store files that cannot hold the key.
#   z = descriptive zone attributes, stored in separate files, read only on demand.
unless existing.include?('urbanflow:demand')
  create 'urbanflow:demand',
         {NAME => 'm', VERSIONS => 1, BLOOMFILTER => 'ROW', IN_MEMORY => 'true'},
         {NAME => 'z', VERSIONS => 1}
end

# duration_predictions, key pu#do#daytype#HH#DD.D. Pre-split into one region per
# pickup borough, so on a multi-node cluster the lookups spread across servers
# from the first write instead of all landing in a single region.
unless existing.include?('urbanflow:duration_pred')
  create 'urbanflow:duration_pred',
         {NAME => 'p', VERSIONS => 1, BLOOMFILTER => 'ROW'},
         SPLITS => ['Brooklyn', 'EWR', 'Manhattan', 'Queens', 'Staten Island']
end

# daily_kpis, key YYYY-MM-DD: date order = key order, so a month is one range scan.
unless existing.include?('urbanflow:daily_kpis')
  create 'urbanflow:daily_kpis', {NAME => 'k', VERSIONS => 1}
end

list_namespace_tables 'urbanflow'
exit
