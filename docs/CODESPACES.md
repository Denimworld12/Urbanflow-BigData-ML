# UrbanFlow in GitHub Codespaces

The dev container (`.devcontainer/devcontainer.json`) gives a Codespace the
same setup as the laptop Quickstart: Python 3.11, Java 21 and `make`. It also
installs a Docker daemon inside the Codespace (the `docker-in-docker`
feature), so the opt-in Big Data stacks from the README run there too.

## What happens when the Codespace is created

After the image builds, the `postCreateCommand` runs
`make tier2-data && make setup && make check`:

1. `make tier2-data` clones the real Tier 2 results into
   `external/Urbanflow-BDA-data` (see [below](#the-real-tier-2-data)).
2. `make setup` creates `.venv` and installs `requirements.txt`.
3. `make check` starts Spark once and prints the Java and Spark versions.

This takes a few minutes. The Codespace opens before it finishes. Watch the
**Creation log** (Command Palette: *Codespaces: View Creation Log*) until it
shows `Spark 4.2.0 | master=local[...]`. After that, `make synth`, `curate`,
`gold`, `model`, `predict-grid` and `dash` work.

### If `make` says `.venv/bin/python: not found`

Then `make setup` never ran. Codespaces made before this fix were built from
`mcr.microsoft.com/devcontainers/python:1-3.11-bullseye`. That image has a
Yarn apt source whose signing key is no longer valid, so `apt-get update`
fails inside it (`NO_PUBKEY 62D54FD4003F6525`). The `docker-in-docker`
feature runs `apt-get update` while it installs, so the image build failed.
When the dev container fails to build, Codespaces starts the Codespace in
[recovery mode](https://docs.github.com/en/codespaces/troubleshooting/troubleshooting-creation-and-deletion-of-codespaces)
instead, and the `postCreateCommand` does not run. The dev container now uses
`python:3-3.11-bookworm`, which has no Yarn source.

A Codespace already in this state does not fix itself when you pull. Once
this fix is on `main`, either:

* **Delete it and create a new one** from `main` (simplest). Or:
* `git pull` in the old Codespace, then run *Codespaces: Rebuild
  Container* from the Command Palette and choose **Full Rebuild**.

If it happens again in a new Codespace, the Creation log shows which step
failed. You can also run `make setup && make check` by hand.

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

## The real Tier 2 data

[`Denimworld12/Urbanflow-BDA-data`](https://github.com/Denimworld12/Urbanflow-BDA-data)
holds the output of one full Tier 2 run: the gold tables from 243.5M FHVHV
trips (`data/gold`) and the trained model (`data/models`). The repository is
public and small: about 340 KB in all, of which `data/gold` is about 230 KB.
So every new Codespace clones it, with no sign-in or token needed:

* `make tier2-data` (run by the `postCreateCommand`) clones it into
  `external/Urbanflow-BDA-data`. `external/` is gitignored.
* If the clone is already there, it does nothing. Use
  `git -C external/Urbanflow-BDA-data pull` to update it.
* If GitHub cannot be reached, it prints `skipped: ...` and setup carries on
  without the data. Run `make tier2-data` again later.
* `make hadoop-load` loads `external/Urbanflow-BDA-data/data/gold` into HDFS
  under `/urbanflow/gold/fhvhv/` automatically when it is there. Point it
  somewhere else with `make hadoop-load TIER2_GOLD=/path/to/data/gold`.

On a laptop, `make tier2-data` does the same thing.

## What has been tested

The dev container was built and run on 2026-10-04 with the
[Dev Containers CLI](https://github.com/devcontainers/cli) (`devcontainer up`
on a fresh clone). That builds the same image, with the same features, and
runs the same `postCreateCommand` as Codespaces. It was built twice: natively
on arm64, and as x86_64 (the architecture Codespaces uses) under emulation.
With the old image the build failed at the `docker-in-docker` feature, as
described above (`apt-get update` in the old image fails on both
architectures). With the new image, on both architectures:

* the Tier 2 clone, `make setup` and `make check` all succeeded;
* `.venv/bin/python` existed;
* `docker` talked to the daemon inside the container.

On arm64, also:

* `make synth curate gold model predict-grid` all exited 0;
* `make dash` answered on port 8501;
* `docker compose` was available.

It was not run inside a GitHub Codespace itself. The Hadoop, HBase,
streaming and monitoring stacks have not been started in a Codespace yet. If
one misbehaves there, please open an issue.
