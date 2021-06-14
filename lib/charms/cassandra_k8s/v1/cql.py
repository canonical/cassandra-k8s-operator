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
from ops.charm import CharmEvents
from ops.framework import EventBase, EventSource
from ops.relation import ConsumerBase, ProviderBase

LIBID = "abcdefg"
LIBAPI = 1
LIBPATCH = 0
logger = logging.getLogger(__name__)


class DeferEventError(Exception):
    def __init__(self, event, reason):
        super().__init__()
        self.event = event
        self.reason = reason


def status_catcher(func):
    @functools.wraps(func)
    def new_func(self, *args, **kwargs):
        try:
            func(self, *args, **kwargs)
        except DeferEventError as e:
            logger.info("Deferring event: %s because: %s", str(e.event), e.reason)
            e.event.defer()

    return new_func


class CQLConsumer(ConsumerBase):
    def __init__(self, charm, name, consumes, multi=False):
        super().__init__(charm, name, consumes, multi)
        self.charm = charm
        self.relation_name = name

    def credentials(self, rel_id=None):
        """
        Returns a dict of credentials
        {"username": <username>, "password": <password>}
        """
        rel = self.framework.model.get_relation(self.relation_name, rel_id)

        relation_data = rel.data[rel.app]
        creds_json = relation_data.get('credentials')
        return json.loads(creds_json) if creds_json is not None else ()

    def databases(self, rel_id=None):
        """List of currently available databases

        Returns:
            list: list of database names
        """
        rel = self.framework.model.get_relation(self.relation_name, rel_id)

        relation_data = rel.data[rel.app]
        dbs = relation_data.get('databases')
        return json.loads(dbs) if dbs else []

    def new_database(self, rel_id=None):
        """Request creation of an additional database

        """
        if not self.charm.unit.is_leader():
            return

        rel = self.framework.model.get_relation(self.relation_name, rel_id)

        rel_data = rel.data[self.charm.app]
        dbs = rel_data.get('requested_databases', 0)
        rel.data[self.charm.app]['requested_databases'] = str(dbs + 1)

    def request_databases(self, num_databases, rel_id=None):
        """Request n databases"""
        if not self.charm.unit.is_leader():
            return

        rel = self.framework.model.get_relation(self.relation_name, rel_id)

        rel.data[self.charm.app]['requested_databases'] = str(num_databases)

    def port(self, rel_id=None):
        """Return the port which the cassandra instance is listening on"""
        rel = self.framework.model.get_relation(self.relation_name, rel_id)

        return rel.data[rel.app].get("port")

    def address(self, rel_id=None):
        """Return the address which the cassandra instance is listening on"""
        rel = self.framework.model.get_relation(self.relation_name, rel_id)

        return rel.data[rel.app].get("address")


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


class CQLProviderCharmEvents(CharmEvents):
    data_changed = EventSource(DataChangedEvent)


class CQLProvider(ProviderBase):
    on = CQLProviderCharmEvents()

    def __init__(self, charm, name, service, version=None):
        super().__init__(charm, name, service, version)
        self.charm = charm
        events = self.charm.on[name]
        self.framework.observe(events.relation_changed, self.on_cql_changed)

    def update_port(self, relation_name, port):
        if self.charm.unit.is_leader():
            for relation in self.charm.model.relations[relation_name]:
                logger.info("Setting port data for relation %s", relation)
                if str(port) != relation.data[self.charm.app].get(
                    "port", None
                ):
                    relation.data[self.charm.app]["port"] = str(port)

    def update_address(self, relation_name, address):
        if self.charm.unit.is_leader():
            for relation in self.charm.model.relations[relation_name]:
                logger.info("Setting address data for relation %s", relation)
                if str(address) != relation.data[self.charm.app].get(
                    "address", None
                ):
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
        return int(rel.data[rel.app].get("requested_databases", 0))

    def databases(self, rel_id):
        rel = self.framework.model.get_relation(self.name, rel_id)
        return json.loads(rel.data[self.charm.app].get("databases") or "[]")

    def set_databases(self, rel_id, dbs):
        rel = self.framework.model.get_relation(self.name, rel_id)
        rel.data[self.charm.app]["databases"] = json.dumps(dbs)

    def on_cql_changed(self, event):
        self.on.data_changed.emit(event.relation.id, event.app.name)


def sanitize_name(name):
    """Make a name safe for use as a keyspace name"""
    # For now just change dashes to underscores. Fix this more in the future
    return name.replace('-', '_')


def generate_password():
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for i in range(20))
