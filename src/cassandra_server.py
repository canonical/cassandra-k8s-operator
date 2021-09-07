# Copyright 2020 Canonical Ltd.
# See LICENSE file for licensing details.

import contextlib
import logging
import secrets
import string
from typing import List, Optional

from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import (
    EXEC_PROFILE_DEFAULT,
    Cluster,
    ExecutionProfile,
    NoHostAvailable,
    Session,
)
from cassandra.policies import RoundRobinPolicy
from cassandra.query import SimpleStatement
from ops.charm import CharmBase
from ops.framework import EventBase
from ops.model import MaintenanceStatus

CQL_PORT = 9042
CQL_PROTOCOL_VERSION = 4
ROOT_USER = "charm_root"

logger = logging.getLogger(__name__)


class ClusterNotReadyError(Exception):
    pass


class Cassandra:
    """Class to manage connections to the Cassandra cluster"""

    def __init__(self, charm: CharmBase):
        """
        Cassandra class constructor

        Args:
            charm: The charm that is using this class
        """
        self.charm = charm

    @contextlib.contextmanager
    def connect(
        self, event: EventBase, username: str = ROOT_USER, password: Optional[str] = None
    ) -> Session:
        """Context manager to connect to the Cassandra cluster and return an active session

        Args:
            event: The current event object
            username: username to connect with
            password: password to connect with

        Returns:
            A cassandra session
        """
        if password is None:
            password = self.root_password(event)
        auth_provider = PlainTextAuthProvider(username=username, password=password)
        profile = ExecutionProfile(load_balancing_policy=RoundRobinPolicy())
        cluster = Cluster(
            [self.charm._bind_address()],
            port=CQL_PORT,
            auth_provider=auth_provider,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            protocol_version=CQL_PROTOCOL_VERSION,
        )
        try:
            session = cluster.connect()
            yield session
        finally:
            cluster.shutdown

    def create_user(self, event: EventBase, username: str) -> List[str]:
        """Create a new Cassandra user

        Args:
            event: The current event object
            username: The username of the new user

        Returns:
            A list of the form [username, password]

        """
        password = self._generate_password()
        try:
            with self.connect(event) as conn:
                conn.execute(
                    f"CREATE ROLE IF NOT EXISTS '{username}' WITH PASSWORD = '{password}' AND LOGIN = true"
                )
        except NoHostAvailable as e:
            logger.info("Caught exception %s:%s: deferring", type(e), e)
            self.charm.unit.status = MaintenanceStatus("Waiting for Database")
            event.defer()
            return []
        return [username, password]

    def create_db(self, event: EventBase, db_name: str, user: str, replication: int) -> bool:
        """Create a new keyspace and grant all permissions to user

        Args:
            event: The current event object
            db_name: Name of the new keyspace
            user: User which should have access to the keyspace
            replication: The replication factor for the keyspace
        """
        try:
            with self.connect(event) as conn:
                # TODO: Review replication strategy
                conn.execute(
                    f"CREATE KEYSPACE IF NOT EXISTS {db_name} WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : {replication} }}"
                )
                conn.execute(f"GRANT ALL PERMISSIONS ON KEYSPACE {db_name} to '{user}'")
        except NoHostAvailable as e:
            logger.info("Caught exception %s:%s: deferring", type(e), e)
            self.charm.unit.status = MaintenanceStatus("Waiting for Database")
            event.defer()
            return False
        return True

    def root_password(self, event) -> str:
        """If the root password is already set, return it. If the root password is not already
        set, generate a new one.

        Returns:
            The root password
        """
        peer_relation = self.charm.model.get_relation("cassandra-peers")
        if root_pass := peer_relation.data[self.charm.app].get("root_password", None):
            return root_pass

        # Without this the query to create a user for some reason does nothing
        if self.charm._num_units() != self.charm._goal_units():
            self.charm.unit.status = MaintenanceStatus("Waiting for units")
            event.defer()
            return ""

        # First create a new superuser
        try:
            with self.connect(event, username="cassandra", password="cassandra") as session:
                # Set system_auth replication here once we have one shot commands in pebble
                # See https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/configuration/secureConfigNativeAuth.html # noqa: W505
                root_pass_secondary = peer_relation.data[self.charm.app].get(
                    "root_password_secondary", None
                )
                if root_pass_secondary is None:
                    root_pass_secondary = self._generate_password()
                    peer_relation.data[self.charm.app][
                        "root_password_secondary"
                    ] = root_pass_secondary
                query = SimpleStatement(
                    f"CREATE ROLE {ROOT_USER} WITH PASSWORD = '{root_pass_secondary}' AND SUPERUSER = true AND LOGIN = true",
                    consistency_level=ConsistencyLevel.QUORUM,
                )
                session.execute(query)
        except NoHostAvailable as e:
            logger.info("Caught exception %s:%s: deferring", type(e), e)
            self.charm.unit.status = MaintenanceStatus("Waiting for Database")
            event.defer()
            return ""
        except InvalidRequest as e:
            if (
                not str(e)
                == f'Error from server: code=2200 [Invalid query] message="{ROOT_USER} already exists"'
            ):
                raise

        # Now disable the original superuser
        try:
            with self.connect(event, username=ROOT_USER, password=root_pass_secondary) as session:
                random_password = self._generate_password()
                session.execute(
                    "ALTER ROLE cassandra WITH PASSWORD=%s AND SUPERUSER=false",
                    (random_password,),
                )
        except NoHostAvailable as e:
            logger.info("Caught exception %s:%s: deferring", type(e), e)
            self.charm.unit.status = MaintenanceStatus("Waiting for Database")
            event.defer()
            return ""
        peer_relation.data[self.charm.app]["root_password"] = root_pass_secondary
        return root_pass_secondary

    @staticmethod
    def _generate_password() -> str:
        """Generate a random password

        Returns:
            The generated password
        """
        alphabet = string.ascii_letters + string.digits
        return "".join(secrets.choice(alphabet) for i in range(20))
