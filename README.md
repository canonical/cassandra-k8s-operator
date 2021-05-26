# Cassandra-Operator

## Description

The [Cassandra] operator provides a NoSQL distributed database solution. It is part of the Observability stack in the [Juju] charm [ecosystem]. Cassandra is highly scalable and fault tolerant.

[Cassandra]: https://cassandra.apache.org/
[Juju]: https://jaas.ai/
[ecosystem]: https://charmhub.io/

## Setup

A typical setup using [snaps](https://snapcraft.io/), for deployments
to a [microk8s](https://microk8s.io/) cluster can be done using the
following commands

    sudo snap install microk8s --classic
    sudo usermod -a -G microk8s $USER
    sudo microk8s.enable dns storage registry dashboard
    sudo snap install juju --classic
    sudo microk8s config | juju add-k8s mk8s --client
    juju bootstrap mk8s

## Build

Install the charmcraft tool

    sudo snap install charmcraft

Build the charm in this git repository using

    charmcraft build

## Usage

Create a Juju model (say "lma" for your observability operators)

    juju add-model lma

### Deploy Alertmanager with PagerDuty configuration

    juju deploy ./cassandra.charm --resource cassandra-image='dstathis/cassandra-operator-image:latest'

### Scale Out Usage

You may add additional Cassandra units for high availability

    juju add-unit cassadnra

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
