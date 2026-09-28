# One image for every HBase role in docker-compose.hbase.yml: HMaster,
# RegionServer, Thrift gateway, and the Python client that loads gold data.
# Built from the official Apache release tarball so the version is pinned and
# nothing depends on a third-party image going stale.
FROM eclipse-temurin:11-jre-jammy

ARG HBASE_VERSION=2.5.15
ARG HBASE_SHA512=3186E87BDEF4A6A464F2C2C37F6453751AD2DA9E4D44AE926DC82034FF8C7EC07ABCD6A3C5DE1C30A8A43ED4E7FA2238A4323AA8204D5C62DEA6D702287C6B36

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-pip netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# The -hadoop3 build is the one that would talk to an HDFS 3.x cluster if
# hbase.rootdir pointed at hdfs:// (see docs/hadoop/hbase.md).
# dlcdn is Apache's fast CDN but only keeps current releases; archive.apache.org
# keeps every release forever but is slow. Try the CDN, fall back to the archive.
RUN f="hbase-${HBASE_VERSION}-hadoop3-bin.tar.gz" \
    && { curl -fsSL -o /tmp/hbase.tgz "https://dlcdn.apache.org/hbase/${HBASE_VERSION}/$f" \
         || curl -fsSL -o /tmp/hbase.tgz "https://archive.apache.org/dist/hbase/${HBASE_VERSION}/$f"; } \
    && echo "${HBASE_SHA512}  /tmp/hbase.tgz" | tr 'A-F' 'a-f' | sha512sum -c - \
    && tar -xzf /tmp/hbase.tgz -C /opt \
    && mv /opt/hbase-${HBASE_VERSION}-hadoop3 /opt/hbase \
    && rm -rf /tmp/hbase.tgz /opt/hbase/docs

ENV HBASE_HOME=/opt/hbase \
    PATH=/opt/hbase/bin:$PATH \
    HBASE_MANAGES_ZK=false \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

COPY requirements-hbase.txt /tmp/requirements-hbase.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements-hbase.txt

COPY docker/hbase/hbase-site.xml /opt/hbase/conf/hbase-site.xml
WORKDIR /app
