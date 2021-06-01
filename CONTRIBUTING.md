Bugs and Pull Requests
======================

All bugs and pull requests should be submitted to the [github repo](https://github.com/canonical/cassandra-operator).

Building and Deploying
======================

    charmcraft build
    juju deploy ./cassandra-k8s.charm --resource cassandra-image='dstathis/cassandra-oper    ator-image:latest'

Tests
=====

    ./run_tests
