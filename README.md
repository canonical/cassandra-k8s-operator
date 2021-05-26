# Cassandra-Operator

## Description

The [Cassandra] operator provides a NoSQL distributed database solution. It is part of the Observability stack in the [Juju] charm [ecosystem]. Cassandra is highly scalable and fault tolerant.

[Cassandra]: https://cassandra.apache.org/
[Juju]: https://jaas.ai/
[ecosystem]: https://charmhub.io/

## Usage

Create a Juju model (say "lma" for your observability operators)

    juju add-model lma

### Deploy Cassandra

    juju deploy ./cassandra.charm --resource cassandra-image='dstathis/cassandra-operator-image:latest'

### Scale Out Usage

You may add additional Cassandra units for high availability

    juju add-unit cassandra

## Developing

Use your existing Python 3 development environment or create and
activate a Python 3 virtualenv

    virtualenv -p python3 venv
    source venv/bin/activate

Install the development requirements

    pip install -r requirements-dev.txt

## Testing

Just run `run_tests`:

    ./run_tests
