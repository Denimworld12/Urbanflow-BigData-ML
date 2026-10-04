# UrbanFlow in GitHub Codespaces

The dev container (`.devcontainer/devcontainer.json`) gives a Codespace the
same setup as the laptop Quickstart: Python 3.11, Java 21 and `make`, with
`make setup && make check` run on creation. It also installs a Docker daemon
inside the Codespace (the `docker-in-docker` feature), so the opt-in Big Data
stacks from the README run there too.

## Which machine type

The dev container asks for at least **4 cores, 16 GB RAM, 32 GB storage**, so
Codespaces offers the 4-core machine or larger. That size comes from the
memory limits in the compose files and Hadoop configs:

| Stack | What sets its memory | Budget |
|---|---|---|
| Hadoop (`make hadoop-up`, 8 containers) | NameNode, DataNode, ResourceManager, NodeManager, JobHistory: `HADOOP_HEAPSIZE_MAX=1g` each. Hive metastore and HiveServer2: `-Xmx1G` each (set by the `apache/hive` image). Postgres is small. | ~7 GB of JVM heap |
| YARN jobs (`make hadoop-demo`) | `yarn.nodemanager.resource.memory-mb=6144` in `hadoop/conf/yarn-site.xml`. MapReduce and Tez containers take 1 GB each. | up to 6 GB more |
| HBase (`make hbase-up`) | ZooKeeper 256 MB, HMaster 1 GB, RegionServer 1 GB, Thrift 512 MB | ~3 GB |
| Kafka (streaming stack) | `KAFKA_HEAP_OPTS=-Xmx512m` | ~0.5 GB |
| Spark (pipeline stages, streaming job) | `URBANFLOW_DRIVER_MEM`, default `4g` | 4 GB |

So the Hadoop stack has a ceiling of about 13 GB plus JVM overhead. Heaps
rarely fill, and the demo's jobs use only part of YARN's 6 GB, so it fits on a
16 GB machine. Everything at once does not. On 16 GB, run one heavy stack at a
time:

* Hadoop on its own, plus monitoring if you want it.
* Or HBase + streaming + monitoring, with `make hadoop-down` first.

If your account or organisation offers an 8-core / 32 GB machine, everything
fits at once there. When this was written, the only machines offered to this
repository were 2-core / 8 GB and 4-core / 16 GB.

The 4-core machine uses your monthly Codespaces quota twice as fast as the
2-core one. Stop the Codespace when you are done.

Disk: the Hadoop images are about 6.5 GB and the HBase image about 1.4 GB.
They are stored in a Docker volume on the Codespace's 32 GB disk.
`make hadoop-clean`, `make hbase-clean` and `docker system prune -a` free the
space.

## Starting the stacks

Nothing starts on its own. In the Codespace terminal, use the same `make`
targets as on a laptop. See the README's
[Big Data ecosystem](../README.md#big-data-ecosystem) section and
[`docs/hadoop/`](hadoop/README.md):

```bash
make synth curate gold   # the local data layers the stacks load
make hadoop-up           # then make hadoop-demo; make hadoop-down when done
make hbase-up            # then make hbase-load, make hbase-query
```

`docker` and `docker compose` talk to the daemon inside the Codespace, so the
compose files need no changes.

## Opening the web UIs

These ports are forwarded and labelled in the **Ports** tab: 8501 (dashboard),
19870, 18088, 19888, 20002 (Hadoop), 16010, 16030 (HBase), 3000, 9090
(Grafana, Prometheus), 9308 (kafka-exporter) and 4050 (Spark streaming).

What GitHub's [port-forwarding docs](https://docs.github.com/en/codespaces/developing-in-a-codespace/forwarding-ports-in-your-codespace)
say, and what it means here:

* **In the browser editor, `http://localhost:PORT` will not open in your
  browser.** Your browser runs on your machine, not in the Codespace. Open the
  port from the Ports tab (globe icon) or click the URL in the terminal. The
  address looks like `https://<codespace-name>-19870.app.github.dev`.
* **In VS Code desktop** connected to the Codespace, forwarded ports are also
  available on `localhost` on your machine. The URLs the `make` targets print
  work as they are. If one does not, the Ports tab shows the address.
* **You do not need to make ports Public to use them yourself.** Forwarded
  ports are private by default: only you can open them. Make a port Public, or visible to your organisation, only to share it with
  someone else. Grafana's admin password is `urbanflow` and anonymous viewing
  is on, so think before making 3000 public.
* **`curl` from inside the Codespace terminal** can use `localhost:PORT`
  directly. For `curl` from your own machine to a private port, pass the
  Codespace's `GITHUB_TOKEN` (an environment variable in the Codespace) as a
  header: `curl -H "X-Github-Token: <token>" https://<codespace-name>-9308.app.github.dev/metrics`.

## Not yet tested

The Hadoop, HBase, streaming and monitoring stacks have **not** been started
inside a real Codespace yet. If one misbehaves there, please open an issue.
