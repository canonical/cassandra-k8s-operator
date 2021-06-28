# Overview

This documents explains the processes and practices recommended for contributing enhancements to the Cassandra charm.

- Generally, before developing enhancements to this charm, you should consider [opening an issue ](https://github.com/canonical/prometheus-operator) explaining your use case.
- If you would like to chat with us about your use-cases or proposed implementation, you can reach us on the [Canonical Mattermost public channel](https://chat.charmhub.io/charmhub/channels/charm-dev) or [Discourse](https://discourse.charmhub.io/).
- It is strongly recommended that prior to engaging in any enhancements to this charm you familiarise your self with Juju.
- Familiarising yourself with the [Charmed Operator Framework](https://juju.is/docs/sdk) library will help you a lot when working on PRs.
- All enhancements require review before being merged.
  Besides the code quality and test coverage, the review will also take into account the resulting user experience for Juju administrators using this charm.
  Please help us out in having easier reviews by rebasing onto the `main` branch, avoid merge commits and enjoy a linear Git history.

## Developing

Create and activate a virtualenv with the development requirements:

```bash
$ virtualenv -p python3 venv
$ source venv/bin/activate
$ pip install -r requirements-dev.txt
```

### Setup

A typical setup using [Snap](https://snapcraft.io/), for deployments to a [microk8s](https://microk8s.io/) cluster can be achieved by following instructions in the Juju SDK [development setup](https://juju.is/docs/sdk/dev-setup).

It is also essential that a Juju storage pool is created as follows

```bash
$ juju create-storage-pool operator-storage kubernetes storage-class=microk8s-hostpath
```

### Build

Build the charm in this git repository

```bash
$ charmcraft build
```

### Deploy

```bash
$ juju deploy ./prometheus-k8s.charm --resource prometheus-image=ubuntu/prometheus:latest
```

## Testing

Unit tests are implemented using the Operator Framework test [harness](https://ops.readthedocs.io/en/latest/#module-ops.testing).
These tests may executed by doing

```bash
$ ./run_tests
```
