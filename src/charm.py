#!/usr/bin/env python3

# Copyright 2020 Canonical Ltd.
# See LICENSE file for licensing details.

"""Cassandra Operator Charm."""

import logging
import sys
from pathlib import Path
from re import IGNORECASE, match
from typing import Optional

import yaml
from charms.cassandra_k8s.v0.cassandra import CassandraProvider
from charms.grafana_k8s.v0.grafana_dashboard import GrafanaDashboardConsumer
from charms.prometheus_k8s.v0.prometheus import MetricsEndpointProvider
from ops.charm import CharmBase
from ops.framework import EventBase
from ops.main import main
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus
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


class CassandraOperatorCharm(CharmBase):
    """Charm object for the Cassandra charm."""

    def __init__(self, *args):
        """Constructor for a CassandraOperatorCharm."""
        super().__init__(*args)
        self.framework.observe(self.on.cassandra_pebble_ready, self.on_pebble_ready)
        self.framework.observe(self.on.config_changed, self.on_config_changed)
        self.framework.observe(self.on.leader_elected, self.on_leader_elected)
        self.framework.observe(self.on["database"].relation_joined, self.on_database_joined)
        self.framework.observe(
            self.on["cassandra_peers"].relation_changed, self.on_cassandra_peers_changed
        )
        self.framework.observe(
            self.on["cassandra_peers"].relation_departed,
            self.on_cassandra_peers_departed,
        )
        # TODO: We need to query the version from the database itself
        self.provider = CassandraProvider(charm=self, name="database")
        self.framework.observe(self.provider.on.data_changed, self.on_provider_data_changed)

        self.prometheus_consumer = MetricsEndpointProvider(
            charm=self,
            name="monitoring",
            service_event=self.on.cassandra_pebble_ready,
            jobs=[
                {
                    "static_configs": [{"targets": [f"*:{PROMETHEUS_EXPORTER_PORT}"]}],
                }
            ],
        )
        self.framework.observe(self.on["monitoring"].relation_joined, self.on_monitoring_joined)
        self.framework.observe(self.on["monitoring"].relation_broken, self.on_monitoring_broken)

        self.dashboard_consumer = GrafanaDashboardConsumer(
            charm=self,
            name="grafana-dashboard",
        )
        self.framework.observe(
            self.on["grafana-dashboard"].relation_joined, self.on_dashboard_joined
        )
        self.framework.observe(
            self.on["grafana-dashboard"].relation_broken, self.on_dashboard_broken
        )
        self.framework.observe(
            self.dashboard_consumer.on.dashboard_status_changed,
            self.on_dashboard_status_changed,
        )
        self.cassandra = Cassandra(charm=self)

    def on_pebble_ready(self, event: EventBase) -> None:
        """Run the pebble ready hook.

        Args:
            event: the event object
        """
        if self._configure(event) is False:
            return
        container = event.workload
        if len(self.model.relations["monitoring"]) > 0:
            self._setup_monitoring()
        if not container.get_service("cassandra").is_running():
            logger.info("Starting Cassandra")
            container.start("cassandra")
        self.provider.update_address("database", self._hostname())

    def on_config_changed(self, event: EventBase) -> None:
        """Run the config changed hook.

        Args:
            event: the event object
        """
        self._configure(event)

    def on_leader_elected(self, _):
        """Run the leader elected hook."""
        self.provider.update_address("database", self._hostname())

    def on_database_joined(self, event):
        """Run the joined hook for the database relation."""
        self.provider.update_port("database", CQL_PORT)
        self.provider.update_address("database", self._hostname())

    def on_dashboard_joined(self, _) -> None:
        """Run the joined hook for the dashboard relation."""
        if not self.unit.is_leader():
            return

        dashboard_path = Path(sys.path[0]) / "dashboard.json.tmpl"
        with dashboard_path.open() as f:
            self.dashboard_consumer.add_dashboard(f.read())

        if isinstance(self.unit.status, BlockedStatus) and "dashboard" in self.unit.status.message:
            self.unit.status = ActiveStatus()

    def on_dashboard_broken(self, _) -> None:
        """Run the broken hook for the dashboard relation."""
        self.dashboard_consumer.remove_dashboard()
        if isinstance(self.unit.status, BlockedStatus) and "dashboard" in self.unit.status.message:
            self.unit.status = ActiveStatus()

    def on_dashboard_status_changed(self, event: EventBase) -> None:
        """Run the dashboard status changed hook.

        Args:
            event: the event object
        """
        if event.valid:
            self._dashboard_valid = True
            self.unit.status = ActiveStatus()
        elif event.error_message:
            self._dashboard_valid = False
            self.unit.status = BlockedStatus(event.error_message)

    def on_monitoring_joined(self, _) -> None:
        """Run the joined hook for the monitoring relation."""
        self._setup_monitoring()

    def _setup_monitoring(self) -> None:
        # Turn on metrics exporting. This should be on on ALL NODES, since it does not
        # export per node cluster metrics with the default JMX exporter
        if self.model.relations["monitoring"]:
            container = self.unit.get_container("cassandra")
            cassandra_env = container.pull(ENV_PATH).read()
            restart_required = False
            if PROMETHEUS_EXPORTER_PATH not in cassandra_env:
                restart_required = True
                container.push(
                    ENV_PATH,
                    f'{cassandra_env}\nJVM_OPTS="$JVM_OPTS -javaagent:{PROMETHEUS_EXPORTER_PATH}"',
                )
            try:
                container.list_files(PROMETHEUS_EXPORTER_PATH)
            except APIError:
                restart_required = True
                logger.debug(
                    "Pushing Prometheus exporter to container to %s", PROMETHEUS_EXPORTER_PATH
                )

                with open(
                    self.model.resources.fetch("cassandra-prometheus-exporter"),
                    "rb",
                ) as f:
                    container.push(path=PROMETHEUS_EXPORTER_PATH, source=f, make_dirs=True)

            if restart_required:
                try:
                    container.restart("cassandra")
                except APIError as e:
                    if str(e) == 'service "cassandra" does not exist':
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
        if not self.model.relations["monitoring"]:
            container = self.unit.get_container("cassandra")
            if container.can_connect():
                container = self.unit.get_container("cassandra")
                cassandra_env = container.pull(ENV_PATH).readlines()
                for line in cassandra_env:
                    if PROMETHEUS_EXPORTER_PATH in line:
                        cassandra_env.remove(line)
                        container.push(ENV_PATH, "\n".join(cassandra_env))
                        container.restart("cassandra")
                        break
                container.remove_path(PROMETHEUS_EXPORTER_PATH)

    def on_cassandra_peers_changed(self, event) -> None:
        """Run the relation changed hook for the peer relation.

        Args:
            event: the event object
        """
        self._configure(event)

    def on_cassandra_peers_departed(self, event) -> None:
        """Run the departed hook for the peer relation.

        Args:
            event: the event object
        """
        self._configure(event)

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
            if not (creds := self.cassandra.create_user(event, username)):  # type: ignore
                return
            self.provider.set_credentials(event.rel_id, creds)

        requested_dbs = self.provider.requested_databases(event.rel_id)
        dbs = self.provider.databases(event.rel_id)
        for db in requested_dbs:
            if db not in dbs:
                if not self.cassandra.create_db(event, db, creds[0], self._goal_units()):
                    return
                dbs.append(db)
        self.provider.set_databases(event.rel_id, dbs)

    def _configure(self, event: EventBase) -> bool:
        container = self.unit.get_container("cassandra")
        if not container.can_connect():
            return True

        heap_size = self.config["heap_size"]

        if match(r"^\d+[KMG]$", heap_size, IGNORECASE) is None:
            self.unit.status = BlockedStatus(f"Invalid Cassandra heap size setting: '{heap_size}'")
            return False

        if self._num_units() != self._goal_units():
            self.unit.status = MaintenanceStatus("Waiting for units")
            event.defer()
            return False

        peer_rel = self.model.get_relation("cassandra-peers")
        peer_rel.data[self.unit]["peer_address"] = self._hostname()

        needs_restart = False

        if not (conf := self._config_file(event)):
            return False
        if yaml.safe_load(container.pull(CONFIG_PATH).read()) != yaml.safe_load(conf):
            container.push(CONFIG_PATH, conf)
            needs_restart = True

        if not (layer := self._build_layer(event)):
            return False
        services = container.get_plan().to_dict().get("services", {})
        if services != layer["services"]:
            container.add_layer("cassandra", layer, combine=True)
            needs_restart = True

        if needs_restart:
            container.restart("cassandra")

        if self.unit.is_leader():
            if not self.cassandra.root_password(event):
                return False

        self.unit.status = ActiveStatus()
        return True

    def _build_layer(self, event) -> dict:
        heap_size = self.config["heap_size"]
        if not (seeds := self._seeds(event)):
            return {}
        return {
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
                        "CASSANDRA_SEEDS": seeds,
                    },
                }
            },
        }

    def _seeds(self, event) -> str:
        peers = [self._hostname()]
        rel = self.model.get_relation("cassandra-peers")
        for unit in rel.units:
            if (addr := rel.data[unit].get("peer_address")) is None:
                self.unit.status = MaintenanceStatus("Waiting for peer IP addresses")
                event.defer()
                return ""
            peers.append(addr)
        peers.sort()
        # Cassandra documentation recommends 3 seeds
        return ",".join(peers[:3])

    def _num_units(self) -> int:
        relation = self.model.get_relation("cassandra-peers")
        # The relation does not list ourself as a unit so we must add 1
        return len(relation.units) + 1 if relation is not None else 1

    def _goal_units(self) -> int:
        return self.app.planned_units()

    def _hostname(self) -> str:
        """Return the hostname of the unit pod.

        Returns:
            The pod hostname
        """
        return f"{self.unit.name.replace('/','-')}.{self.app.name}-endpoints.{self.model.name}.svc.cluster.local"

    def _bind_address(self) -> Optional[str]:
        try:
            return str(self.model.get_binding("cassandra-peers").network.bind_address)
        except TypeError as e:
            if str(e) == "'NoneType' object is not iterable":
                return None
            else:
                raise

    def _config_file(self, event) -> str:
        if (bind_address := self._bind_address()) is None:
            self.unit.status = MaintenanceStatus("Waiting for IP address")
            event.defer()
            return ""
        if not (seeds := self._seeds(event)):
            return ""
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
        return yaml.dump(conf)


if __name__ == "__main__":
    # see https://github.com/canonical/operator/issues/506
    main(CassandraOperatorCharm, use_juju_for_storage=True)
