# Cassandra Operator

## Description

Apache [Cassandra] is an open source NoSQL distributed database providing scalability, high availability and fault-tolerance on commodity hardware.

This Cassandra operator is designed to integrate Cassandra in the [Juju] .[charm ecosystem].

## Usage

```sh
$ juju deploy cassandra-k8s
```

### Scale out and down

Changing the amount of units of Cassandra is done by executing:

```sh
$ juju scale-application cassandra-k8s <new_unit_count>
```

You should scale the Cassandra cluster gradually, not adding or removing units too fast.
The rebalancing of the cluster is I/O intensive, with potentially large amounts of data moving between nodes, and that can measurably affect performance.

## Relations

This operator provides the following relation interfaces:

* `cassandra` provides access to the Cassandra cluster over the [Cassandra Query Language (CQL)][CQL].

The following relation interfaces are consumed:

* `prometheus_scrape` enables the monitoring of the Cassandra cluster by the [Prometheus Operator] charm.

## OCI Images

This charm by default uses the latest version of the [`cassandra`](https://hub.docker.com/_/cassandra) image on Docker Hub.

[Cassandra]: https://cassandra.apache.org/
[CQL]: https://cassandra.apache.org/doc/latest/cql/
[Juju]: https://jaas.ai/
[ecosystem]: https://charmhub.io/
[Prometheus Operator]: https://charmhub.io/prometheus-k8s
