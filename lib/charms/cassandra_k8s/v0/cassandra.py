"""
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

import functools
import json
import logging
import secrets
import string

from ops.framework import EventBase, EventSource, Object, ObjectEvents
from ops.model import BlockedStatus, MaintenanceStatus, WaitingStatus

LIBID = "fab458c53af54b0fa7ff696d71e243c1"
LIBAPI = 0
LIBPATCH = 1
logger = logging.getLogger(__name__)


class DeferEventError(Exception):
    def __init__(self, reason, status_message):
        super().__init__()
        self.reason = reason
        self.status_message = status_message


class BlockedStatusError(Exception):
    pass


class WaitingStatusError(Exception):
    pass


def status_catcher(func):
    @functools.wraps(func)
    def new_func(self, *args, **kwargs):
        try:
            func(self, *args, **kwargs)
        except DeferEventError as e:
            if len(args) >= 1 and isinstance(args[0], EventBase):
                event = args[0]
            else:
                logger.error("Can not defer: No event")
                raise
            logger.info("Defering event: %s because: %s", event, e.reason)
            self.unit.status = MaintenanceStatus(e.status_message)
            event.defer()
        except BlockedStatusError as e:
            self.unit.status = BlockedStatus(str(e))
        except WaitingStatusError as e:
            self.unit.status = WaitingStatus(str(e))

    return new_func


class CassandraConsumerError(Exception):
    pass


class NameDuplicateError(CassandraConsumerError):
    pass


class NameLengthError(CassandraConsumerError):
    pass


class DatabasesChangedEvent(EventBase):
    """Event emitted when the relation data has changed"""

    def __init__(self, handle, rel_id):
        super().__init__(handle)
        self.rel_id = rel_id

    def snapshot(self):
        return {"rel_id": self.rel_id}

    def restore(self, snapshot):
        self.rel_id = snapshot["rel_id"]


class CassandraConsumerEvents(ObjectEvents):
    databases_changed = EventSource(DatabasesChangedEvent)


class CassandraConsumer(Object):
    on = CassandraConsumerEvents()

    def __init__(self, charm, name):
        super().__init__(charm, name)
        self.charm = charm
        self.relation_name = name
        events = self.charm.on[name]
        self.framework.observe(events.relation_changed, self.on_relation_changed)

    def on_relation_changed(self, event):
        self.on.databases_changed.emit(rel_id=event.relation.id)

    def credentials(self, rel_id=None):
        """
        Returns a dict of credentials
        {"username": <username>, "password": <password>}

        Args:
            rel_id: Relation id. Required for multi mode.
        """
        rel = self.framework.model.get_relation(self.relation_name, rel_id)

        relation_data = rel.data[rel.app]
        creds_json = relation_data.get("credentials")
        return json.loads(creds_json) if creds_json is not None else ()

    def databases(self, rel_id=None):
        """List of currently available databases.

        Args:
            rel_id: Relation id. Required for multi mode.

        Returns:
            list: list of database names
        """
        rel = self.framework.model.get_relation(self.relation_name, rel_id)

        relation_data = rel.data[rel.app]
        dbs = relation_data.get("databases")
        return json.loads(dbs) if dbs else []

    def new_database(self, rel_id=None, name_suffix=""):
        """Request creation of an additional database.

        Args:
            rel_id: Relation id. Required for multi mode.
            name_suffix (str): Suffix to append to the datatbase name. This is
                required if you request multiple databases.
        """
        rel = self.framework.model.get_relation(self.relation_name, rel_id)

        if name_suffix:
            name_suffix = "_{}".format(name_suffix)
        db_name = "juju_db_{}_{}{}".format(
            sanitize_name(self.charm.model.name),
            sanitize_name(self.charm.app.name),
            sanitize_name(name_suffix),
        )
        # Cassandra does not allow keyspace names longer than 48 characters
        if len(db_name) > 48:
            raise NameLengthError("Database name can not be more than 48 characters")
        dbs = self._requested_databases(rel)
        dbs.append(db_name)
        if not len(dbs) == len(set(dbs)):
            raise NameDuplicateError("Database names are not unique")
        self._set_requested_databases(rel, dbs)

    def port(self, rel_id=None):
        """Return the port which the cassandra instance is listening on.

        Args:
            rel_id: Relation id. Required for multi mode.
        """
        rel = self.framework.model.get_relation(self.relation_name, rel_id)

        return rel.data[rel.app].get("port")

    def address(self, rel_id=None):
        """Return the address which the cassandra instance is listening on.

        Args:
            rel_id: Relation id. Required for multi mode.
        """
        rel = self.framework.model.get_relation(self.relation_name, rel_id)

        return rel.data[rel.app].get("address")

    def _requested_databases(self, relation):
        """Return the list of requested databases."""
        dbs_json = relation.data[self.charm.app].get("requested_databases", "[]")
        return json.loads(dbs_json)

    def _set_requested_databases(self, relation, requested_databases):
        """Set the list of requested databases."""
        relation.data[self.charm.app]["requested_databases"] = json.dumps(requested_databases)


class DataChangedEvent(EventBase):
    """Event emitted when the relation data has changed"""

    def __init__(self, handle, rel_id, app_name):
        super().__init__(handle)
        self.rel_id = rel_id
        self.app_name = app_name

    def snapshot(self):
        return {"rel_id": self.rel_id, "app_name": self.app_name}

    def restore(self, snapshot):
        self.rel_id = snapshot["rel_id"]
        self.app_name = snapshot["app_name"]


class CassandraProviderEvents(ObjectEvents):
    data_changed = EventSource(DataChangedEvent)


class CassandraProvider(Object):
    on = CassandraProviderEvents()

    def __init__(self, charm, name):
        super().__init__(charm, name)
        self.charm = charm
        self.name = name
        events = self.charm.on[name]
        self.framework.observe(events.relation_changed, self.on_relation_changed)

    def update_port(self, relation_name, port):
        if self.charm.unit.is_leader():
            for relation in self.charm.model.relations[relation_name]:
                logger.info("Setting port data for relation %s", relation)
                if str(port) != relation.data[self.charm.app].get("port", None):
                    relation.data[self.charm.app]["port"] = str(port)

    def update_address(self, relation_name, address):
        if self.charm.unit.is_leader():
            for relation in self.charm.model.relations[relation_name]:
                logger.info("Setting address data for relation %s", relation)
                if str(address) != relation.data[self.charm.app].get("address", None):
                    relation.data[self.charm.app]["address"] = str(address)

    def credentials(self, rel_id):
        rel = self.framework.model.get_relation(self.name, rel_id)
        creds_json = rel.data[self.charm.app].get("credentials", "[]")
        return json.loads(creds_json)

    def set_credentials(self, rel_id, creds):
        rel = self.framework.model.get_relation(self.name, rel_id)
        rel.data[self.charm.app]["credentials"] = json.dumps(creds)

    def requested_databases(self, rel_id):
        rel = self.framework.model.get_relation(self.name, rel_id)
        return json.loads(rel.data[rel.app].get("requested_databases", "[]"))

    def databases(self, rel_id):
        rel = self.framework.model.get_relation(self.name, rel_id)
        return json.loads(rel.data[self.charm.app].get("databases") or "[]")

    def set_databases(self, rel_id, dbs):
        rel = self.framework.model.get_relation(self.name, rel_id)
        rel.data[self.charm.app]["databases"] = json.dumps(dbs)

    def on_relation_changed(self, event):
        self.on.data_changed.emit(event.relation.id, event.app.name)


def sanitize_name(name):
    """Make a name safe for use as a keyspace name"""
    # For now just change dashes to underscores. Fix this more in the future
    return name.replace("-", "_")


def generate_password():
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for i in range(20))
