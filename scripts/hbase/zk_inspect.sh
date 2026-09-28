#!/usr/bin/env bash
# What HBase keeps in ZooKeeper. Run by `make hbase-zk` inside hbase-client.
# Uses `hbase zkcli`, which is ZooKeeper's own zkCli bundled with HBase.
set -u
zk() { hbase zkcli -server zookeeper:2181 "$@" 2>/dev/null | grep -avE '^(Connecting|WATCHER|WatchedEvent|JLine|$)'; }
# znode payloads are protobuf; keep only the printable parts (host names, ports).
readable() { LC_ALL=C tr -c '[:print:]' ' ' | tr -s ' '; echo; }

echo "== ls /hbase            (every znode HBase created)"; zk ls /hbase
echo "== ls /hbase/rs         (one ephemeral znode per live RegionServer)"; zk ls /hbase/rs
echo "== ls /hbase/backup-masters"; zk ls /hbase/backup-masters
echo "== get /hbase/master    (active master's address)"; zk get /hbase/master | readable
echo "== get /hbase/meta-region-server   (who serves hbase:meta)"; zk get /hbase/meta-region-server | readable
for rs in $(zk ls /hbase/rs | tr -d '[]' | sed 's/, / /g'); do
  echo "== stat /hbase/rs/$rs"
  zk stat "/hbase/rs/$rs" | grep -E 'ephemeralOwner|ctime'
done
echo "== stat /hbase/master"; zk stat /hbase/master | grep -E 'ephemeralOwner|ctime'
echo "== ZooKeeper server (four-letter word 'srvr')"
echo srvr | nc -w 2 zookeeper 2181 | grep -E 'version|Mode|Node count|Connections'
