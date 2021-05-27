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

import contextlib
import json
import logging
import subprocess
import yaml

from charms.cassandra.v1.cql import (
    DeferEventError,
    status_catcher,
    generate_password,
    CQLProvider,
)
from charms.prometheus.v1.prometheus import PrometheusConsumer

from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import (
    Cluster,
    ExecutionProfile,
    EXEC_PROFILE_DEFAULT,
    NoHostAvailable,
)
from cassandra.policies import RoundRobinPolicy
from cassandra.query import SimpleStatement

from ops.charm import CharmBase
from ops.framework import StoredState
from ops.main import main
from ops.model import ActiveStatus

logger = logging.getLogger(__name__)


CQL_PROTOCOL_VERSION = 4
ROOT_USER = "charm_root"
CONFIG_PATH = "/etc/cassandra/cassandra.yaml"
ENV_PATH = "/etc/cassandra/cassandra-env.sh"


def restart(container):
    logger.info("Restarting cassandra")
    if container.get_service("cassandra").is_running():
        container.stop("cassandra")
    container.start("cassandra")


def make_started(container):
    if not container.get_service("cassandra").is_running():
        logger.info("Starting Cassandra")
        container.start("cassandra")


class CassandraOperatorCharm(CharmBase):
    _stored = StoredState()

    def __init__(self, *args):
        super().__init__(*args)
        self._stored.set_default(root_password="")
        # If the _root_password() method partially completes we need to store the password while keeping self._stored.root_password empty
        self._stored.set_default(root_password_secondary="")
        self.framework.observe(self.on.cassandra_pebble_ready, self.on_pebble_ready)
        self.framework.observe(self.on.config_changed, self.on_config_changed)
        self.framework.observe(self.on["cql"].relation_joined, self.on_cql_joined)
        self.framework.observe(
            self.on["monitoring"].relation_joined, self.on_monitoring_joined
        )
        self.framework.observe(
            self.on["monitoring"].relation_broken, self.on_monitoring_broken
        )
        self.framework.observe(
            self.on["cassandra_peers"].relation_changed, self.on_cassandra_peers_changed
        )
        self.framework.observe(
            self.on["cassandra_peers"].relation_departed,
            self.on_cassandra_peers_departed,
        )
        self.provider = CQLProvider(charm=self, name="cql", service="cassandra")
        self.prometheus_consumer = PrometheusConsumer(
            charm=self, name="monitoring", consumes={"Prometheus": ">=2"}
        )

    @status_catcher
    def on_pebble_ready(self, event):
        self._configure(event)
        container = event.workload
        make_started(container)

    @status_catcher
    def on_config_changed(self, event):
        self._configure(event)
        self.provider.update_port("cql", self.model.config["port"])

    def on_cql_joined(self, event):
        self.provider.update_port("cql", self.model.config["port"])

    @status_catcher
    def on_monitoring_joined(self, event):
        # Turn on metrics exporting
        if not self.unit.is_leader():
            return
        if len(self.model.relations["monitoring"]) > 0:
            container = self.unit.get_container("cassandra")
            cassandra_env = container.pull(ENV_PATH).read()
            if "jmx_prometheus_javaagent" not in cassandra_env:
                container.push(
                    ENV_PATH,
                    cassandra_env
                    + '\nJVM_OPTS="$JVM_OPTS -javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent-0.15.0.jar=7070:/opt/jmx-exporter/cassandra.yaml"',
                )
                restart(container)
                self.prometheus_consumer.add_endpoint(
                    address=self._bind_address(), port=7070
                )

    @status_catcher
    def on_monitoring_broken(self, event):
        if not self.unit.is_leader():
            return
        # If there are no monitoring relations, disable metrics
        if len(self.model.relations["monitoring"]) == 0:
            container = self.unit.get_container("cassandra")
            cassandra_env = container.pull(ENV_PATH).readlines()
            for line in cassandra_env:
                if "jmx_prometheus_javaagent" in line:
                    cassandra_env.remove(line)
                    container.push(ENV_PATH, "\n".join(cassandra_env))
                    restart(container)
                    break

    @status_catcher
    def on_cassandra_peers_changed(self, event):
        self._configure(event)

    @status_catcher
    def on_cassandra_peers_departed(self, event):
        self._configure(event)

    def _root_password(self, event):
        if self._stored.root_password:
            return self._stored.root_password

        # Without this the query to create a user for some reason does nothing
        if self._num_units() != self._goal_units():
            raise DeferEventError(event, "Units not up in _root_password()")

        # First create a new superuser
        auth_provider = PlainTextAuthProvider(
            username="cassandra", password="cassandra"
        )
        profile = ExecutionProfile(load_balancing_policy=RoundRobinPolicy())
        cluster = Cluster(
            [self._bind_address()],
            port=self.model.config["port"],
            auth_provider=auth_provider,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            protocol_version=CQL_PROTOCOL_VERSION,
        )
        try:
            try:
                session = cluster.connect()
            except NoHostAvailable as e:
                logger.info("Caught exception %s:%s", type(e), e)
                raise DeferEventError(
                    event, "Can't connect to database in _root_password()"
                )
            # Set system_auth replication here once we have pebble
            # See https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/configuration/secureConfigNativeAuth.html
            if not self._stored.root_password_secondary:
                self._stored.root_password_secondary = generate_password()
            query = SimpleStatement(
                f"CREATE ROLE {ROOT_USER} WITH PASSWORD = '{self._stored.root_password_secondary}' AND SUPERUSER = true AND LOGIN = true",
                consistency_level=ConsistencyLevel.QUORUM,
            )
            session.execute(query)
        except InvalidRequest as e:
            if (
                not str(e)
                == 'Error from server: code=2200 [Invalid query] message="charm_root already exists"'
            ):
                raise
        finally:
            cluster.shutdown()

        # Now disable the original superuser
        auth_provider = PlainTextAuthProvider(
            username=ROOT_USER, password=self._stored.root_password_secondary
        )
        cluster = Cluster(
            [self._bind_address()],
            port=self.model.config["port"],
            auth_provider=auth_provider,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            protocol_version=CQL_PROTOCOL_VERSION,
        )
        try:
            try:
                session = cluster.connect()
            except NoHostAvailable as e:
                logger.info("Caught exception %s:%s", type(e), e)
                raise DeferEventError(
                    event, "Can't connect to database in _root_password()"
                )
            random_password = generate_password()
            session.execute(
                "ALTER ROLE cassandra WITH PASSWORD=%s AND SUPERUSER=false",
                (random_password,),
            )
        finally:
            cluster.shutdown()
        self._stored.root_password = self._stored.root_password_secondary
        return self._stored.root_password

    @contextlib.contextmanager
    def cql_connection(self, event):
        auth_provider = PlainTextAuthProvider(
            username=ROOT_USER, password=self._root_password(event)
        )
        profile = ExecutionProfile(load_balancing_policy=RoundRobinPolicy())
        cluster = Cluster(
            [self._bind_address()],
            port=self.model.config["port"],
            auth_provider=auth_provider,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            protocol_version=CQL_PROTOCOL_VERSION,
        )
        try:
            session = cluster.connect()
            yield session
        except NoHostAvailable as e:
            logger.info("Caught exception %s:%s", type(e), e)
            raise DeferEventError(event, "Can't connect to database")
        finally:
            cluster.shutdown

    def create_user(self, event, user, password):
        with self.cql_connection(event) as conn:
            conn.execute(
                f"CREATE ROLE IF NOT EXISTS '{user}' WITH PASSWORD = '{password}' AND LOGIN = true"
            )

    def create_db(self, event, db_name, user):
        with self.cql_connection(event) as conn:
            # Review replication strategy
            conn.execute(
                f"CREATE KEYSPACE IF NOT EXISTS {db_name} WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : {self._goal_units()} }}"
            )
            conn.execute(f"GRANT ALL PERMISSIONS ON KEYSPACE {db_name} to '{user}'")

    def _configure(self, event):
        if self._num_units() != self._goal_units():
            raise DeferEventError(event, "Units not up in _configure()")

        bind_address = self._bind_address()
        if bind_address is None:
            raise DeferEventError(event, "No ip address in _configure()")
        peer_rel = self.model.get_relation("cassandra-peers")
        peer_rel.data[self.unit]["peer_address"] = bind_address

        needs_restart = False

        conf = self._config_file(event)
        container = self.unit.get_container("cassandra")
        if yaml.safe_load(container.pull(CONFIG_PATH).read()) != yaml.safe_load(conf):
            container.push(CONFIG_PATH, conf)
            needs_restart = True

        layer = self._build_layer(event)
        services = container.get_plan().to_dict().get("services", {})
        if services != layer["services"]:
            container.add_layer("cassandra", layer, combine=True)
            needs_restart = True

        if needs_restart:
            restart(container)

        if self.unit.is_leader():
            self._root_password(event)

        self.unit.status = ActiveStatus()
        logger.debug("Pod spec set successfully.")

    def _build_layer(self, event):
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
                        "CASSANDRA_SEEDS": self._seeds(event),
                    },
                }
            },
        }
        return layer

    def _seeds(self, event):
        bind_address = self._bind_address()
        if bind_address is None:
            raise DeferEventError(event, "No ip address in _seeds()")
        peers = [bind_address]
        rel = self.model.get_relation("cassandra-peers")
        for unit in rel.units:
            addr = rel.data[unit].get("peer_address")
            if addr is None:
                raise DeferEventError(event, "No peer ip address in _seeds()")
            peers.append(addr)
        peers.sort()
        return ",".join(peers[:3])

    def _num_units(self):
        relation = self.model.get_relation("cassandra-peers")
        # The relation does not list ourself as a unit so we must add 1
        return len(relation.units) + 1 if relation is not None else 1

    def _goal_units(self):
        # We need to shell out here as goal state is not yet implemented in operator
        # See https://github.com/canonical/operator/pull/453
        goal_state = json.loads(
            subprocess.check_output(["goal-state", "--format", "json"])
        )
        return len(goal_state["units"])

    def _bind_address(self, timeout=60):
        try:
            return str(self.model.get_binding("cassandra-peers").network.bind_address)
        except TypeError as e:
            if str(e) == "'NoneType' object is not iterable":
                return None
            else:
                raise

    def _config_file(self, event):
        bind_address = self._bind_address()
        if bind_address is None:
            raise DeferEventError(event, "No ip address in _config_file()")
        conf = {
            "cluster_name": f"juju-cluster-{self.app.name}",
            "num_tokens": 256,
            "listen_address": bind_address,
            "start_native_transport": "true",
            "native_transport_port": self.model.config["port"],
            "seed_provider": [
                {
                    "class_name": "org.apache.cassandra.locator.SimpleSeedProvider",
                    "parameters": [{"seeds": self._seeds(event)}],
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
