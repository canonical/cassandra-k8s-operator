#!/usr/bin/env python3
# Copyright 2020 dylan
# See LICENSE file for licensing details.

import functools
import json
import logging
import secrets
import string
import subprocess
import yaml

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


CLUSTER_PORT = 7001
UNIT_ADDRESS = "{}-{}.{}-endpoints.{}.svc.cluster.local"
CQL_PROTOCOL_VERSION = 4
ROOT_USER = "charm_root"


class DeferEventError(Exception):
    def __init__(self, event):
        super().__init__()
        self.event = event


def status_catcher(func):
    @functools.wraps(func)
    def new_func(self, *args, **kwargs):
        try:
            func(self, *args, **kwargs)
        except DeferEventError as e:
            logger.info(f"Defering event {str(e.event)}")
            e.event.defer()

    return new_func


class CassandraOperatorCharm(CharmBase):
    stored = StoredState()

    def __init__(self, *args):
        super().__init__(*args)
        self.stored.set_default(root_password="")
        # If the root_password() method partially completes we need to store the password while keeping self.stored.root_password empty
        self.stored.set_default(root_password_secondary="")
        self.framework.observe(self.on.config_changed, self.on_config_changed)
        self.framework.observe(self.on["cql"].relation_changed, self.on_cql_changed)
        self.framework.observe(
            self.on["cassandra-peers"].relation_joined, self.on_cassandra_peers_joined
        )
        self.framework.observe(
            self.on["cassandra_peers"].relation_changed, self.on_cassandra_peers_changed
        )
        self.framework.observe(
            self.on["cassandra_peers"].relation_departed,
            self.on_cassandra_peers_departed,
        )

    def on_config_changed(self, event):
        self.configure()
        for relation in self.model.relations["cql"]:
            self.update_cql(relation)

    def on_cql_changed(self, event):
        self.update_cql(event.relation)

    @status_catcher
    def on_cassandra_peers_joined(self, event):
        if self.unit.is_leader():
            self.root_password(event)

    def on_cassandra_peers_changed(self, event):
        self.configure()

    def on_cassandra_peers_departed(self, event):
        self.configure()

    def update_cql(self, relation):
        if self.unit.is_leader():
            logger.info("Setting relation data")
            if str(self.model.config["port"]) != relation.data[self.app].get(
                "port", None
            ):
                relation.data[self.app]["port"] = str(self.model.config["port"])

    def root_password(self, event):
        if self.stored.root_password:
            return self.stored.root_password

        # Without this the query to create a user for some reason does nothing
        if self.num_units() != self.goal_units():
            raise DeferEventError(event)

        # First create a new superuser
        auth_provider = PlainTextAuthProvider(
            username="cassandra", password="cassandra"
        )
        profile = ExecutionProfile(load_balancing_policy=RoundRobinPolicy())
        cluster = Cluster(
            [self.cql_address()],
            port=self.model.config["port"],
            auth_provider=auth_provider,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            protocol_version=CQL_PROTOCOL_VERSION,
        )
        try:
            try:
                session = cluster.connect()
            except NoHostAvailable as e:
                logger.info(f"Caught exception {type(e)}:{e}")
                raise DeferEventError(event)
            # Set system_auth replication here once we have pebble
            # See https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/configuration/secureConfigNativeAuth.html
            if not self.stored.root_password_secondary:
                alphabet = string.ascii_letters + string.digits
                self.stored.root_password_secondary = "".join(
                    secrets.choice(alphabet) for i in range(20)
                )
            query = SimpleStatement(
                f"CREATE ROLE {ROOT_USER} WITH PASSWORD = '{self.stored.root_password_secondary}' AND SUPERUSER = true AND LOGIN = true",
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
            username=ROOT_USER, password=self.stored.root_password_secondary
        )
        cluster = Cluster(
            [self.cql_address()],
            port=self.model.config["port"],
            auth_provider=auth_provider,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            protocol_version=CQL_PROTOCOL_VERSION,
        )
        try:
            try:
                session = cluster.connect()
            except NoHostAvailable as e:
                logger.info(f"Caught exception {type(e)}:{e}")
                raise DeferEventError(event)
            random_password = "".join(secrets.choice(alphabet) for i in range(20))
            session.execute(
                "ALTER ROLE cassandra WITH PASSWORD=%s AND SUPERUSER=false",
                (random_password,),
            )
        finally:
            cluster.shutdown()
        self.stored.root_password = self.stored.root_password_secondary
        return self.stored.root_password

    def configure(self):
        if not self.unit.is_leader():
            self.unit.status = ActiveStatus()
            return

        pod_spec = self.build_pod_spec()
        self.model.pod.set_spec(pod_spec)

        self.unit.status = ActiveStatus()
        logger.debug("Pod spec set successfully.")

    def build_pod_spec(self):
        config = self.model.config

        image_details = {"imagePath": config["image_path"]}

        spec = {
            "version": 3,
            "containers": [
                {
                    "name": self.app.name,
                    "imageDetails": image_details,
                    "command": [
                        "sh",
                        "-c",
                        "cp /mnt/cassandra.yaml /etc/cassandra/cassandra.yaml &&"
                        "docker-entrypoint.sh",
                    ],
                    "ports": [
                        {
                            "containerPort": config["port"],
                            "name": "cql",
                            "protocol": "TCP",
                        },
                        {
                            "containerPort": CLUSTER_PORT,
                            "name": "cluster",
                            "protocol": "TCP",
                        },
                    ],
                    "volumeConfig": [
                        {
                            "name": "config",
                            "mountPath": "/mnt",
                            # "mountPath": "/etc/charm/cassandra",
                            "files": [
                                {
                                    "path": "cassandra.yaml",
                                    "content": self.config_file(),
                                }
                            ],
                        }
                    ],
                    # 'kubernetes': # probes here
                    # These need to be set or docker-entrypoint.sh will change them to default values
                    "envConfig": {
                        "CASSANDRA_SEEDS": self.seeds(),
                    },
                }
            ],
        }
        return spec

    def seeds(self):
        seeds = UNIT_ADDRESS.format(self.meta.name, 0, self.meta.name, self.model.name)
        num_units = self.goal_units()
        if num_units >= 2:
            seeds = (
                seeds
                + ","
                + UNIT_ADDRESS.format(
                    self.meta.name, 1, self.meta.name, self.model.name
                )
            )
        if num_units >= 3:
            seeds = (
                seeds
                + ","
                + UNIT_ADDRESS.format(
                    self.meta.name, 2, self.meta.name, self.model.name
                )
            )
        return seeds

    def num_units(self):
        relation = self.model.get_relation("cassandra-peers")
        # The relation does not list ourself as a unit so we must add 1
        return len(relation.units) + 1 if relation is not None else 1

    def goal_units(self):
        # We need to shell out here as goal state is not yet implemented in operator
        # See https://github.com/canonical/operator/pull/453
        goal_state = json.loads(
            subprocess.check_output(["goal-state", "--format", "json"])
        )
        return len(goal_state["units"])

    def cql_address(self, timeout=60):
        try:
            return str(
                self.model.get_binding("cassandra-peers").network.ingress_address
            )
        except TypeError as e:
            if str(e) == "'NoneType' object is not iterable":
                return None
            else:
                raise

    def config_file(self):
        conf = {
            "cluster_name": f"juju-cluster-{self.app.name}",
            "num_tokens": 256,
            "listen_address": "0.0.0.0",
            "start_native_transport": "true",
            "native_transport_port": self.model.config["port"],
            "seed_provider": [
                {
                    "class_name": "org.apache.cassandra.locator.SimpleSeedProvider",
                    "parameters": [{"seeds": self.seeds()}],
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
    main(CassandraOperatorCharm)
