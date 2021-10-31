## Bugs and Pull requests

All bugs and pull requests should be submitted to the [github repo](https://github.com/canonical/cassandra-operator).

- Generally, before developing enhancements to this charm, you should consider
  explaining your use case.
- If you would like to chat with us about your use-cases or proposed
  implementation, you can reach us at
  [the CharmHub Mattermost server](https://chat.charmhub.io/charmhub/channels/charm-dev)
  or [Discourse](https://discourse.charmhub.io/).
- All enhancements require review before being merged. Apart from
  code quality and test coverage, the review will also take into
  account the resulting user experience for Juju administrators using
  this charm.

## Setup

A typical setup using [snaps](https://snapcraft.io/) can be found in the
[Juju docs](https://juju.is/docs/sdk/dev-setup).

## Developing

To build and deploy the charm:

```sh
# Build the charm
charmcraft pack
# Download the Cassandra Prometheus exporter
curl \
  -L https://github.com/instaclustr/cassandra-exporter/releases/download/v0.9.10/cassandra-exporter-agent-0.9.10.jar \
  -o cassandra-exporter-agent.jar
# Deploy the charm
juju deploy ./cassandra-k8s_ubuntu-20.04-amd64.charm \
    --resource cassandra-image='cassandra:3.11' \
    --resource cassandra-prometheus-exporter="$(pwd)/cassandra-exporter-agent.jar"
```

## Testing

The Charmed Operator Framework includes a very nice harness for testing operator
behavior without full deployment. These tests are run using tox:

```sh
tox
```

To run the integration tests, use tox as well:

```sh
tox -e integration
```
