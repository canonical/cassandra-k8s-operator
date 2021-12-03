# Cassandra Operator

WARNING: This is not a production branch. It is used only for testing the loki log_proxy library.
WARNING: For some reason you need to let cassandra reach idle before creating the log_proxy relation.

## Description

Apache [Cassandra] is an open source NoSQL distributed database providing scalability, high
availability and fault-tolerance on commodity hardware.

This Cassandra operator is designed to integrate Cassandra in the [Juju] charm [ecosystem].

## Usage

```sh
$ juju deploy cassandra-k8s
```

### Scale out and down

Changing the amount of units of Cassandra is done by executing:

```sh
$ juju scale-application cassandra-k8s <new_unit_count>
```

You should scale the Cassandra cluster gradually, not adding or removing units too fast. The
rebalancing of the cluster is I/O intensive, with potentially large amounts of data moving between
nodes, and that can measurably affect performance.

## Integrations

Cassandra integrates with the following

1. Any charm that implements the `cassandra` consumer interface. This interface
   database credentials and keyspace creation for the consumer charm. The
   interface can be implemented by using the cassandra charm library. Detailed
   information about how to do this can be found
   [here](https://charmhub.io/cassandra-k8s/libraries/cassandra).

2. Any charm that implements the `prometheus_scrape` consumer interface. Most
   likely this will be the Prometheus charm. This allows monitoring of the
   Cassandra cluster. More information about the `prometheus_scrape` interface can
   be found in the library docs
   [here](https://charmhub.io/prometheus-k8s/libraries/prometheus_scrape).

For more information on using charm libraries, see the docs
[here](https://charmhub.io/cassandra-k8s/libraries).

## OCI Images

This charm by default uses the latest version of the [`cassandra`](https://hub.docker.com/_/cassandra) image on Docker Hub.

[Cassandra]: https://cassandra.apache.org/
[CQL]: https://cassandra.apache.org/doc/latest/cql/
[Juju]: https://jaas.ai/
[ecosystem]: https://charmhub.io/
[Prometheus Operator]: https://charmhub.io/prometheus-k8s
