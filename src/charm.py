#!/usr/bin/env python3

#  Copyright 2021 Canonical Ltd.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import json
import logging
import os
import subprocess
import sys
from re import IGNORECASE, match
from typing import Optional

import yaml
from cassandra import ConsistencyLevel, InvalidRequest  # type: ignore
from cassandra.query import SimpleStatement
from charms.cassandra_k8s.v0.cassandra import (
    BlockedStatusError,
    CassandraProvider,
    DeferEventError,
    generate_password,
    status_catcher,
)
from charms.grafana_k8s.v0.grafana_dashboard import GrafanaDashboardConsumer
from charms.prometheus_k8s.v0.prometheus import PrometheusConsumer
from ops.charm import CharmBase
from ops.framework import EventBase
from ops.main import main
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus, ModelError
from ops.pebble import APIError, ConnectionError

from cassandra_server import Cassandra

logger = logging.getLogger(__name__)


ROOT_USER = "charm_root"
CONFIG_PATH = "/etc/cassandra/cassandra.yaml"
ENV_PATH = "/etc/cassandra/cassandra-env.sh"

PROMETHEUS_EXPORTER_PORT = 9500
PROMETHEUS_EXPORTER_DIR = "/opt/cassandra/lib"
PROMETHEUS_EXPORTER_FILE = "prometheus_exporter_javaagent.jar"
PROMETHEUS_EXPORTER_PATH = f"{PROMETHEUS_EXPORTER_DIR}/{PROMETHEUS_EXPORTER_FILE}"


def restart(container):
    logger.info("Restarting cassandra")
    try:
        if container.get_service("cassandra").is_running():
            container.stop("cassandra")
        container.start("cassandra")
    except ModelError as e:
        if str(e) != "service 'cassandra' not found":
            raise


def make_started(container):
    try:
        if not container.get_service("cassandra").is_running():
            logger.info("Starting Cassandra")
            container.start("cassandra")
    except ModelError as e:
        if str(e) != "service 'cassandra' not found":
            raise


class CassandraOperatorCharm(CharmBase):
    def __init__(self, *args):
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
        self.provider = CassandraProvider(
            charm=self, name="database", service="cassandra", version="3.11"
        )
        self.framework.observe(self.provider.on.data_changed, self.on_provider_data_changed)

        self.prometheus_consumer = PrometheusConsumer(
            charm=self,
            name="monitoring",
            consumes={"prometheus": ">=2"},
            service_event=self.on.cassandra_pebble_ready,
            jobs=[
                {
                    "static_configs": [{"targets": ["*:{}".format(PROMETHEUS_EXPORTER_PORT)]}],
                }
            ],
        )
        self.framework.observe(self.on["monitoring"].relation_joined, self.on_monitoring_joined)
        self.framework.observe(self.on["monitoring"].relation_broken, self.on_monitoring_broken)

        self.dashboard_consumer = GrafanaDashboardConsumer(
            charm=self,
            name="grafana-dashboard",
            consumes={"Grafana": ">=2.0.0"},
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

    @status_catcher
    def on_pebble_ready(self, event: EventBase) -> None:
        self._configure()
        container = event.workload
        if len(self.model.relations["monitoring"]) > 0:
            self._setup_monitoring()
        make_started(container)
        self.provider.update_address("database", self._bind_address())

    @status_catcher
    def on_config_changed(self, _) -> None:
        self._configure()
        self.provider.update_port("database", self.model.config["port"])

    def on_leader_elected(self, _):
        self.provider.update_address("database", self._bind_address())

    def on_database_joined(self, _):
        self.provider.update_port("database", self.model.config["port"])
        self.provider.update_address("database", self._bind_address())

    @status_catcher
    def on_dashboard_joined(self, _) -> None:
        if not self.unit.is_leader():
            return

        if isinstance(self.unit.status, BlockedStatus) and "dashboard" in self.unit.status.message:
            self.unit.status = ActiveStatus()

        dashboard_tmpl = open(os.path.join(sys.path[0], "dashboard.json.tmpl"), "r").read()

        self.dashboard_consumer.add_dashboard(dashboard_tmpl)

    def on_dashboard_broken(self, _) -> None:
        self.dashboard_consumer.remove_dashboard()
        if isinstance(self.unit.status, BlockedStatus) and "dashboard" in self.unit.status.message:
            self.unit.status = ActiveStatus()

    def on_dashboard_status_changed(self, event: EventBase) -> None:
        if event.valid:
            self._dashboard_valid = True
            self.unit.status = ActiveStatus()
        elif event.error_message:
            self._dashboard_valid = False
            self.unit.status = BlockedStatus(event.error_message)

    @status_catcher
    def on_monitoring_joined(self, _) -> None:
        self._setup_monitoring()

    def _reset_monitoring(self) -> None:
        if not self.unit.is_leader():
            return
        if len(self.model.relations["monitoring"]) > 0:
            for endpoint in self.prometheus_consumer.endpoints:
                address, port = endpoint.split(":")
                self.prometheus_consumer.remove_endpoint(address=address, port=int(port))
            self._setup_monitoring()

    def _setup_monitoring(self) -> None:
        # Turn on metrics exporting. This should be on on ALL NODES, since it does not
        # export per node cluster metrics with the default JMX exporter
        if len(self.model.relations["monitoring"]) > 0:
            container = self.unit.get_container("cassandra")
            cassandra_env = container.pull(ENV_PATH).read()
            restart_required = False
            if "jmx_prometheus_javaagent" not in cassandra_env:
                restart_required = True
                container.push(
                    ENV_PATH,
                    '{}\nJVM_OPTS="$JVM_OPTS -javaagent:{}"'.format(
                        cassandra_env, PROMETHEUS_EXPORTER_PATH
                    ),
                )

            try:
                container.list_files(PROMETHEUS_EXPORTER_PATH)
            except APIError:
                restart_required = True
                logger.debug(
                    "Pushing Prometheus exporter to container to {}".format(
                        PROMETHEUS_EXPORTER_PATH
                    )
                )

                with open(
                    self.model.resources.fetch("cassandra-prometheus-exporter"),
                    "rb",
                ) as f:
                    container.push(path=PROMETHEUS_EXPORTER_PATH, source=f, make_dirs=True)

            if restart_required:
                restart(container)

    @status_catcher
    def on_monitoring_broken(self, _) -> None:
        # If there are no monitoring relations, disable metrics
        if len(self.model.relations["monitoring"]) == 0:
            try:
                container = self.unit.get_container("cassandra")
                cassandra_env = container.pull(ENV_PATH).readlines()
                for line in cassandra_env:
                    if "jmx_prometheus_javaagent" in line:
                        cassandra_env.remove(line)
                        container.push(ENV_PATH, "\n".join(cassandra_env))
                        restart(container)
                        break
                container.remove_path(PROMETHEUS_EXPORTER_PATH)
            except ConnectionError:
                logger.warning("Could not disable monitoring. Could not connect to Pebble.")

    @status_catcher
    def on_cassandra_peers_changed(self, _) -> None:
        self._configure()

    @status_catcher
    def on_cassandra_peers_departed(self, _) -> None:
        self._configure()

    @status_catcher
    def on_provider_data_changed(self, event: EventBase) -> None:
        if not self.unit.is_leader():
            return
        creds = self.provider.credentials(event.rel_id)
        if creds == []:
            username = f"juju-user-{event.app_name}"
            password = generate_password()
            self.cassandra.create_user(username, password)
            creds = [username, password]
            self.provider.set_credentials(event.rel_id, creds)

        requested_dbs = self.provider.requested_databases(event.rel_id)
        dbs = self.provider.databases(event.rel_id)
        for db in requested_dbs:
            if db not in dbs:
                self.cassandra.create_db(db, creds[0], self._goal_units())
                dbs.append(db)
        self.provider.set_databases(event.rel_id, dbs)

    def _root_password(self) -> str:
        peer_relation = self.model.get_relation("cassandra-peers")
        if root_pass := peer_relation.data[self.app].get("root_password", None):
            return root_pass

        # Without this the query to create a user for some reason does nothing
        if self._num_units() != self._goal_units():
            self.unit.status = MaintenanceStatus("Waiting for units")
            raise DeferEventError("Units not up in _root_password()", "Waiting for units")

        # First create a new superuser
        try:
            with self.cassandra.connect(username="cassandra", password="cassandra") as session:
                # Set system_auth replication here once we have one shot commands in pebble
                # See https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/configuration/secureConfigNativeAuth.html # noqa: W505
                root_pass_secondary = peer_relation.data[self.app].get(
                    "root_password_secondary", None
                )
                if root_pass_secondary is None:
                    root_pass_secondary = generate_password()
                    peer_relation.data[self.app]["root_password_secondary"] = root_pass_secondary
                query = SimpleStatement(
                    f"CREATE ROLE {ROOT_USER} WITH PASSWORD = '{root_pass_secondary}' AND SUPERUSER = true AND LOGIN = true",
                    consistency_level=ConsistencyLevel.QUORUM,
                )
                session.execute(query)
        except InvalidRequest as e:
            if (
                not str(e)
                == 'Error from server: code=2200 [Invalid query] message="charm_root already exists"'
            ):
                raise

        # Now disable the original superuser
        with self.cassandra.connect(
            username="charm_root", password=root_pass_secondary
        ) as session:
            random_password = generate_password()
            session.execute(
                "ALTER ROLE cassandra WITH PASSWORD=%s AND SUPERUSER=false",
                (random_password,),
            )
        peer_relation.data[self.app]["root_password"] = root_pass_secondary
        return root_pass_secondary

    def _configure(self) -> None:
        heap_size = self.model.config["heap_size"]

        if match(r"^\d+[KMG]$", heap_size, IGNORECASE) is None:
            raise BlockedStatusError(f"Invalid Cassandra heap size setting: '{heap_size}'")

        if self._num_units() != self._goal_units():
            raise DeferEventError("Units not up in _configure()", "Waiting for units")

        if (bind_address := self._bind_address()) is None:
            raise DeferEventError("No ip address in _configure()", "Waiting for units")

        peer_rel = self.model.get_relation("cassandra-peers")
        peer_rel.data[self.unit]["peer_address"] = bind_address

        needs_restart = False

        conf = self._config_file()
        container = self.unit.get_container("cassandra")
        if yaml.safe_load(container.pull(CONFIG_PATH).read()) != yaml.safe_load(conf):
            container.push(CONFIG_PATH, conf)
            needs_restart = True

        layer = self._build_layer()
        services = container.get_plan().to_dict().get("services", {})
        if services != layer["services"]:
            container.add_layer("cassandra", layer, combine=True)
            needs_restart = True

        if needs_restart:
            restart(container)

        if self.unit.is_leader():
            self._root_password()

        if self.unit.is_leader():
            self.provider.ready()

        self.unit.status = ActiveStatus()
        logger.debug("Pod spec set successfully.")

    def _build_layer(self) -> dict:
        heap_size = self.model.config["heap_size"]
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
        return layer

    def _seeds(self) -> str:
        if (bind_address := self._bind_address()) is None:
            self.unit.status = MaintenanceStatus("Waiting for network address")
            raise DeferEventError("No ip address in _seeds()", "Waiting for units")
        peers = [bind_address]
        rel = self.model.get_relation("cassandra-peers")
        for unit in rel.units:
            if (addr := rel.data[unit].get("peer_address")) is None:
                self.unit.status = MaintenanceStatus("Waiting for peer addresses")
                raise DeferEventError("No peer ip address in _seeds()", "Waiting for units")
            peers.append(addr)
        peers.sort()
        return ",".join(peers[:3])

    def _num_units(self) -> int:
        relation = self.model.get_relation("cassandra-peers")
        # The relation does not list ourself as a unit so we must add 1
        return len(relation.units) + 1 if relation is not None else 1

    def _goal_units(self) -> int:
        # We need to shell out here as goal state is not yet implemented in operator
        # See https://github.com/canonical/operator/pull/453
        goal_state = json.loads(subprocess.check_output(["goal-state", "--format", "json"]))
        return len(goal_state["units"])

    def _bind_address(self, timeout=60) -> Optional[str]:
        try:
            return str(self.model.get_binding("cassandra-peers").network.bind_address)
        except TypeError as e:
            if str(e) == "'NoneType' object is not iterable":
                return None
            else:
                raise

    def _config_file(self) -> str:
        if (bind_address := self._bind_address()) is None:
            self.unit.status = MaintenanceStatus("Waiting for network address")
            raise DeferEventError("No ip address in _config_file()", "Waiting for units")
        conf = {
            "cluster_name": f"juju-cluster-{self.app.name}",
            "num_tokens": 256,
            "listen_address": bind_address,
            "start_native_transport": "true",
            "native_transport_port": self.model.config["port"],
            "seed_provider": [
                {
                    "class_name": "org.apache.cassandra.locator.SimpleSeedProvider",
                    "parameters": [{"seeds": self._seeds()}],
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
