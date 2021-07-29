Bugs and Pull Requests
======================

All bugs and pull requests should be submitted to the [github repo](https://github.com/canonical/cassandra-operator).

Building and Deploying
======================

```sh
$ charmcraft build
$ curl -L https://github.com/instaclustr/cassandra-exporter/releases/download/v0.9.10/cassandra-exporter-agent-0.9.10.jar -o cassandra-exporter-agent.jar
$ juju deploy ./cassandra-k8s_ubuntu-20.04-amd64.charm \
    --resource cassandra-image='cassandra:3.11' \
    --resource cassandra-prometheus-exporter="$(pwd)/cassandra-exporter-agent.jar"
```

Tests
=====

```sh
$ ./run_tests
```
