import contextlib
import logging
from typing import Optional

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import (
    EXEC_PROFILE_DEFAULT,
    Cluster,
    ExecutionProfile,
    NoHostAvailable,
    Session,
)
from cassandra.policies import RoundRobinPolicy
from charms.cassandra_k8s.v0.cassandra import DeferEventError
from ops.charm import CharmBase
from ops.model import MaintenanceStatus

CQL_PORT = 9042
CQL_PROTOCOL_VERSION = 4

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
    def connect(self, username: str = "charm_root", password: Optional[str] = None) -> Session:
        """Context manager to connect to the Cassandra cluster and return an active session

        Args:
            username: username to connect with
            password: password to connect with

        Returns:
            A cassandra session
        """
        if password is None:
            password = self.charm._root_password()
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
        except NoHostAvailable as e:
            logger.info("Caught exception %s:%s", type(e), e)
            self.charm.unit.status = MaintenanceStatus("Cassandra Starting")
            raise DeferEventError("Can't connect to database", "Waiting for Database")
        finally:
            cluster.shutdown

    def create_user(self, username: str, password: str) -> None:
        """Create a new Cassandra user

        Args:
            username: The username of the new user
            password: The password of the new user

        """
        with self.connect() as conn:
            conn.execute(
                f"CREATE ROLE IF NOT EXISTS '{username}' WITH PASSWORD = '{password}' AND LOGIN = true"
            )

    def create_db(self, db_name: str, user: str, replication: int) -> None:
        """Create a new keyspace and grant all permissions to user

        Args:
            db_name: Name of the new keyspace
            user: User which should have access to the keyspace
            replication: The replication factor for the keyspace
        """
        with self.connect() as conn:
            # TODO: Review replication strategy
            conn.execute(
                f"CREATE KEYSPACE IF NOT EXISTS {db_name} WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : {replication} }}"
            )
            conn.execute(f"GRANT ALL PERMISSIONS ON KEYSPACE {db_name} to '{user}'")
