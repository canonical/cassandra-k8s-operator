#!/usr/bin/env python3

# Copyright 2020 Canonical Ltd.
# See LICENSE file for licensing details.

"""Cassandra Operator Charm."""

import logging
from re import IGNORECASE, match
from typing import Optional

import yaml
from charms.cassandra_k8s.v0.cassandra import CassandraProvider
from charms.grafana_k8s.v0.grafana_dashboard import GrafanaDashboardProvider
from charms.prometheus_k8s.v0.prometheus_scrape import MetricsEndpointProvider
from ops.charm import CharmBase
from ops.framework import EventBase
from ops.main import main
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus, ModelError
from ops.pebble import APIError

from cassandra_server import CQL_PORT, Cassandra

logger = logging.getLogger(__name__)


ROOT_USER = "charm_root"
CONFIG_PATH = "/etc/cassandra/cassandra.yaml"
ENV_PATH = "/etc/cassandra/cassandra-env.sh"

PROMETHEUS_EXPORTER_PORT = 9500
PROMETHEUS_EXPORTER_DIR = "/opt/cassandra/lib"
PROMETHEUS_EXPORTER_FILE = "prometheus_exporter_javaagent.jar"
PROMETHEUS_EXPORTER_PATH = f"{PROMETHEUS_EXPORTER_DIR}/{PROMETHEUS_EXPORTER_FILE}"


# This is needed due to a bug where pebble does not support restart.
# We should remove it when the bug is fixed.
def restart(container):
    """Restart the cassandra service in container."""
    if container.get_service("cassandra").is_running():
        container.stop("cassandra")
    container.start("cassandra")


class CassandraOperatorCharm(CharmBase):
    """Charm object for the Cassandra charm."""

    def __init__(self, *args):
        """Constructor for a CassandraOperatorCharm."""
        super().__init__(*args)
        self._container = self.unit.get_container("cassandra")
        self.framework.observe(self.on.cassandra_pebble_ready, self.on_pebble_ready)
        self.framework.observe(self.on.config_changed, self.on_config_changed)
        self.framework.observe(self.on.leader_elected, self.on_leader_elected)
        self.framework.observe(self.on["database"].relation_joined, self.on_database_joined)
        # TODO: We need to query the version from the database itself
        self.provider = CassandraProvider(charm=self, name="database")
        self.framework.observe(self.provider.on.data_changed, self.on_provider_data_changed)

        self.prometheus_provider = MetricsEndpointProvider(
            charm=self,
            relation_name="monitoring",
            jobs=[
                {
                    "static_configs": [{"targets": [f"*:{PROMETHEUS_EXPORTER_PORT}"]}],
                }
            ],
        )
        self.framework.observe(self.on["monitoring"].relation_created, self.on_monitoring_created)
        self.framework.observe(self.on["monitoring"].relation_broken, self.on_monitoring_broken)

        self.dashboard_provider = GrafanaDashboardProvider(self)
        self.cassandra = Cassandra(charm=self)
        logging.getLogger("cassandra").setLevel(logging.CRITICAL)

    def on_pebble_ready(self, event: EventBase) -> None:
        """Run the pebble ready hook.

        Args:
            event: the event object
        """
        if self._num_units() != self._goal_units():
            self.unit.status = MaintenanceStatus("Waiting for units")
            event.defer()
            return

        if self._set_config_file(event) is False:
            return

        if self._set_layer():
            # self._container.restart("cassandra")
            restart(self._container)

        if self.unit.is_leader():
            if not self.cassandra.root_password(event):
                # Event was deffered.
                return

        if self.model.relations["monitoring"]:
            self._setup_monitoring()

        self.provider.update_address("database", self._hostname())

        self.unit.status = ActiveStatus()

    def on_config_changed(self, event: EventBase) -> None:
        """Run the config changed hook.

        Args:
            event: the event object
        """
        heap_size = self.config["heap_size"]
        if match(r"^\d+[KMG]$", heap_size, IGNORECASE) is None:
            self.unit.status = BlockedStatus(f"Invalid Cassandra heap size setting: '{heap_size}'")
            return
        if "cassandra" in self._container.get_plan().services:
            restart(self._container)
            # self._container.restart("cassandra")

    def on_leader_elected(self, _):
        """Run the leader elected hook."""
        self.provider.update_address("database", self._hostname())

    def on_database_joined(self, event):
        """Run the joined hook for the database relation."""
        self.provider.update_port("database", CQL_PORT)
        self.provider.update_address("database", self._hostname())

    def on_monitoring_created(self, _) -> None:
        """Run the joined hook for the monitoring relation."""
        self._setup_monitoring()

    def _setup_monitoring(self) -> None:
        # Turn on metrics exporting. This should be on on ALL NODES, since it does not
        # export per node cluster metrics with the default JMX exporter
        if self.model.relations["monitoring"]:
            cassandra_env = self._container.pull(ENV_PATH).read()
            restart_required = False
            if PROMETHEUS_EXPORTER_PATH not in cassandra_env:
                restart_required = True
                self._container.push(
                    ENV_PATH,
                    f'{cassandra_env}\nJVM_OPTS="$JVM_OPTS -javaagent:{PROMETHEUS_EXPORTER_PATH}"',
                )
            try:
                self._container.list_files(PROMETHEUS_EXPORTER_PATH)
            except APIError:
                restart_required = True
                logger.debug(
                    "Pushing Prometheus exporter to container to %s", PROMETHEUS_EXPORTER_PATH
                )

                with open(
                    self.model.resources.fetch("cassandra-prometheus-exporter"),
                    "rb",
                ) as f:
                    self._container.push(path=PROMETHEUS_EXPORTER_PATH, source=f, make_dirs=True)

            if restart_required:
                try:
                    restart(self._container)
                    # self._container.restart("cassandra")
                except ModelError as e:
                    if "service 'cassandra' not found" in str(e):
                        # The service has not yet been created. This is okay.
                        pass
                    else:
                        raise

    def on_monitoring_broken(self, _) -> None:
        """Run the broken hook for the monitoring relation.

        Args:
            event: the event object
        """
        # If there are no monitoring relations, disable metrics
        if not self.model.relations["monitoring"] and self._container.can_connect():
            cassandra_env = self._container.pull(ENV_PATH).readlines()
            for line in cassandra_env:
                if PROMETHEUS_EXPORTER_PATH in line:
                    cassandra_env.remove(line)
                    self._container.push(ENV_PATH, "\n".join(cassandra_env))
                    restart(self._container)
                    # self._container.restart("cassandra")
                    break
            self._container.remove_path(PROMETHEUS_EXPORTER_PATH)

    def on_provider_data_changed(self, event: EventBase) -> None:
        """Run the provider data changed hook.

        Args:
            event: the event object
        """
        if not self.unit.is_leader():
            return
        creds = self.provider.credentials(event.rel_id)
        if not creds:
            # Create a user for the related charm to use.
            username = f"juju-user-{event.app_name}"
            if not (creds := self.cassandra.create_user(event, username)):
                # Event was deffered.
                return
            self.provider.set_credentials(event.rel_id, creds)

        requested_dbs = self.provider.requested_databases(event.rel_id)
        dbs = self.provider.databases(event.rel_id)
        for db in requested_dbs:
            if db not in dbs:
                if not self.cassandra.create_db(event, db, creds[0], self._goal_units()):
                    # Event was deffered.
                    return
                dbs.append(db)
        self.provider.set_databases(event.rel_id, dbs)

    def _set_layer(self) -> bool:
        """Set the layer for cassandra.

        Returns:
            Return True if there has been a change else False
        """
        if not self._container.can_connect():
            return False
        heap_size = self.config["heap_size"]
        layer = {
            "summary": "Cassandra Layer",
            "description": "pebble config layer for Cassandra",
            "services": {
                "cassandra": {
                    "override": "replace",
                    "summary": "cassandra service",
                    "command": "docker-entrypoint.sh cassandra -f",
                    "startup": "enabled",
                    "environment": {
                        "JVM_OPTS": f"-Xms{heap_size} -Xmx{heap_size}",
                        "CASSANDRA_SEEDS": self._seeds(),
                    },
                }
            },
        }
        services = self._container.get_plan().to_dict().get("services", {})
        if services != layer["services"]:
            self._container.add_layer("cassandra", layer, combine=True)
            return True
        return False

    def _seeds(self) -> str:
        # The seeds should be the hostnames of the first 3 units.
        seeds_list = [self._hostname(unit_number=x) for x in [0, 1, 2]][: self._goal_units()]
        return ",".join(seeds_list)

    def _num_units(self) -> int:
        relation = self.model.get_relation("cassandra-peers")
        # The relation does not list ourself as a unit so we must add 1
        return len(relation.units) + 1 if relation is not None else 1

    def _goal_units(self) -> int:
        return self.app.planned_units()

    def _hostname(self, unit_number: Optional[int] = None) -> str:
        """Return the hostname of the unit pod.

        Args:
            unit_number: The unit number you wish to get a hostname for.

        Returns:
            The pod hostname
        """
        if unit_number is not None:
            pod_name = f"{self.app.name}-{unit_number}"
        else:
            pod_name = self.unit.name.replace("/", "-")
        return f"{pod_name}.{self.app.name}-endpoints.{self.model.name}.svc.cluster.local"

    def _bind_address(self) -> Optional[str]:
        try:
            return str(self.model.get_binding("cassandra-peers").network.bind_address)
        except TypeError as e:
            if str(e) == "'NoneType' object is not iterable":
                return None
            else:
                raise

    def _set_config_file(self, event) -> bool:
        if (bind_address := self._bind_address()) is None:
            self.unit.status = MaintenanceStatus("Waiting for IP address")
            event.defer()
            return False
        seeds = self._seeds())
        conf = {
            "cluster_name": f"juju-cluster-{self.app.name}",
            "num_tokens": 256,
            "listen_address": bind_address,
            "start_native_transport": "true",
            "native_transport_port": CQL_PORT,
            "seed_provider": [
                {
                    "class_name": "org.apache.cassandra.locator.SimpleSeedProvider",
                    "parameters": [{"seeds": seeds}],
                }
            ],
            "authenticator": "PasswordAuthenticator",
            "authorizer": "CassandraAuthorizer",
            # Required configs
            "commitlog_sync": "periodic",
            "commitlog_sync_period_in_ms": 10000,
            "partitioner": "org.apache.cassandra.dht.Murmur3Partitioner",
            "endpoint_snitch": "GossipingPropertyFileSnitch",
        }
        if yaml.safe_load(self._container.pull(CONFIG_PATH).read()) != conf:
            self._container.push(CONFIG_PATH, yaml.dump(conf))
            try:
                if not self._container.get_service("cassandra").is_running():
                    restart(self._container)
                    # self._container.restart("cassandra")
            except ModelError as e:
                if str(e) != "service 'cassandra' not found":
                    raise
        return True


if __name__ == "__main__":
    # see https://github.com/canonical/operator/issues/506
    main(CassandraOperatorCharm, use_juju_for_storage=True)
