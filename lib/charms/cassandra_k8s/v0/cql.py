# Copyright 2020 Canonical Ltd.
# See LICENSE file for licensing details.

"""# Cassandra charm library.

## Overview

This document explains how to integrate with the Cassandra charm for the
purposes of consuming a cassandra database. It also explains how alternative
implementations of the Cassandra charm may maintain the same interface and be
backward compatible with all currently integrated charms. Finally this document
is the authoritative reference on the structure of relation data that is shared
between Cassandra charms and any other charm that intends to use the database.

## Consumer Library Usage

Charms that would like to use a Cassandra database must use the `CassandraConsumer`
object from the charm library. Using the `CassandraConsumer` object requires
instantiating it, typically in the constructor of your charm. The `CassandraConsumer`
constructor requires the name of the relation over which a database will be used.
This relation must use the `cassandra` interface.

    from charms.cassandra_k8s.v0.cassandra import CassandraConsumer

    def __init__(self, *args):
        super().__init__(*args)
        ...
        self.cassandra_consumer = CassandraConsumer(
            self, "monitoring"}
        )
        ...

An instantiated `CassandraConsumer` object may be used to request new databases
using the `new_database()` method. This method requires no arguments unless you
require multiple databases. If multiple databases are requested, you must
provide a unique `name_suffix` argument. For example

    def _on_database_relation_joined(self, event):
        self.cassandra_consumer.new_database(name_suffix="db1")
        self.cassandra_consumer.new_database(name_suffix="db2")

The `address`, `port`, `databases`, and `credentials` methods can all be called
to get the relevant information from the relation data.

## Provider Library Usage

The `CassandraProvider` object may be used by Cassandra charms to manage
relations with their clients. For this purposes a Cassandra charm needs to do
three things

1. Instantiate the `CassandraProvider` object providing it with three key
pieces of information

  - Name of the relation that the Cassandra charm uses to interact with
    clients.  This relation must conform to the `cassandra` interface.

  - A service name. Although this is an arbitrary string, it must be the same
    string that clients will use as the key of their `consumes` specification.
    It is recommended that this key be `cassandra`.

  For example a Cassandra charm may instantiate the `CassandraProvider` in its
  constructor as follows

    from charms.cassandra_k8s.v0.cassandra import CassandraProvider

    def __init__(self, *args):
        super().__init__(*args)
        ...
        self.prometheus_provider = CassandraProvider(
                self, "database"
            )
        ...

2. A Cassandra charm must set the address, port, and credentials when the
information becomes available. For example

    ...
    self.cassandra_provider.update_port("database", port)
    self.cassandra_provider.update_address("database", address)
    self.cassandra_provider.set_credentials(rel_id, [username, password])
    ...

3. A Cassandra charm needs to respond to the `DataChanged` event of the
`CassandraProvider` by adding itself as and observer for these events, as in

    self.framework.observe(
        self.cassandra_provider.on.data_changed,
        self._on_provider_data_changed,
    )

In responding to the `DataChanged` event the Cassandra charm must create any
non existent databases and update the relation data to reflect reality. For
this purpose the `CassandraProvider` object exposes a `set_databases()` that a
list of database names can be provided to.

    def _on_provider_data_changed(self, event):
        ...
        existing_dbs = self.cassandra_provider.databases(event.rel_id):
        for db in self.cassandra_provider.requested_databases(event.rel_id):
            if db not in existing_dbs:
                self._create_database(db)
                existing_databases.append(db)
        self.cassandra_provider.set_databases(event.rel_id, existing_dbs)
        ...
"""

import json
import logging

from ops.charm import CharmBase
from ops.framework import EventBase, Object

LIBID = "fab458c53af54b0fa7ff696d71e243c1"
LIBAPI = 0
LIBPATCH = 2
logger = logging.getLogger(__name__)


class CassandraConsumerError(Exception):
    """Error base class."""


class NameDuplicateError(CassandraConsumerError):
    """Duplicate db names."""


class NameLengthError(CassandraConsumerError):
    """Name is too long."""


class CassandraConsumer(Object):
    """Cassandra consumer object."""

    def __init__(self, charm: CharmBase, name: str):
        """Constructor fot the CassandraConsumer object.

        Args:
            charm: The charm object that instantiated this class.
            name: The name of the cql relation.
        """
        super().__init__(charm, name)
        self.charm = charm
        self.relation_name = name

    def credentials(self, rel_id: int = None) -> list:
        """Returns the credentials.

        Args:
            rel_id: Relation id. Required for multi mode.

        Returns:
            The credentials in the form of [<username>, <password>]
        """
        rel = self.framework.model.get_relation(self.relation_name, rel_id)

        relation_data = rel.data[rel.app]
        creds_json = relation_data.get("credentials")
        return json.loads(creds_json) if creds_json is not None else []

    def port(self, rel_id: int = None) -> str:
        """Return the port which the cassandra instance is listening on.

        Args:
            rel_id: Relation id. Required for multi mode.
        """
        rel = self.framework.model.get_relation(self.relation_name, rel_id)

        return rel.data[rel.app].get("port")

    def address(self, rel_id: int = None) -> str:
        """Return the address which the cassandra instance is listening on.

        Args:
            rel_id: Relation id. Required for multi mode.
        """
        rel = self.framework.model.get_relation(self.relation_name, rel_id)

        return rel.data[rel.app].get("address")


class CassandraProvider(Object):
    """Cassandra provider object."""

    def __init__(self, charm: CharmBase, name: str):
        """Constructor for CassandraProvider.

        Args:
            charm: The charm object that instantiated this class.
            name: The name of the cql relation.
        """
        super().__init__(charm, name)
        self.charm = charm
        self.name = name

    def update_port(self, relation_name: str, port: int) -> None:
        """Update the port which Cassandra is listening on.

        Args:
            relation_name: The name of the cql relation.
            port: The port number.
        """
        if self.charm.unit.is_leader():
            for relation in self.charm.model.relations[relation_name]:
                logger.info("Setting port data for relation %s", relation)
                if str(port) != relation.data[self.charm.app].get("port", None):
                    relation.data[self.charm.app]["port"] = str(port)

    def update_address(self, relation_name: str, address: str) -> None:
        """Update the address which Cassandra is listening on.

        Args:
            relation_name: The name of the cql relation.
            address: The address which Cassandra is listening on.
        """
        if self.charm.unit.is_leader():
            for relation in self.charm.model.relations[relation_name]:
                logger.info("Setting address data for relation %s", relation)
                if str(address) != relation.data[self.charm.app].get("address", None):
                    relation.data[self.charm.app]["address"] = str(address)

    def credentials(self, rel_id: int) -> list:
        """Return the set credentials.

        Args:
            rel_id: Relation id to look up credentials for.
        Returns: A (username, password) tuple.
        """
        rel = self.framework.model.get_relation(self.name, rel_id)
        creds_json = rel.data[self.charm.app].get("credentials", "[]")
        return json.loads(creds_json)

    def set_credentials(self, rel_id: int, creds) -> None:
        """Set the credentials for a related charm.

        Args:
            rel_id: Relation id to set credentials for.
            creds: A tuple or list of the form [username, password].
        """
        rel = self.framework.model.get_relation(self.name, rel_id)
        rel.data[self.charm.app]["credentials"] = json.dumps(creds)

    def _on_relation_changed(self, event: EventBase) -> None:
        """Handle the relation changed event.

        Args:
            event: The event object.
        """
        self.on.data_changed.emit(event.relation.id, event.app.name)


def sanitize_name(name: str) -> str:
    """Make a name safe for use as a keyspace name."""
    # For now just change dashes to underscores. Fix this more in the future
    return name.replace("-", "_")
