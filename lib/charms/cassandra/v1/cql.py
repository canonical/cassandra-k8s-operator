import functools
import json
import logging
import secrets
import string
from .relation import ConsumerBase, ProviderBase

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
            logger.info(f"Defering event: {str(e.event)} because: {e.reason}")
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
        if rel_id is None:
            rel_id = super()._stored.relation_id
        rel = self.framework.model.get_relation(self.relation_name, rel_id)

        relation_data = rel.data[rel.app]
        creds_json = relation_data.get('credentials')
        creds = json.loads(creds_json) if creds_json is not None else ()
        return creds

    def databases(self, rel_id=None):
        """List of currently available databases

        Returns:
            list: list of database names
        """
        if rel_id is None:
            rel_id = super()._stored.relation_id
        rel = self.framework.model.get_relation(self.relation_name, rel_id)

        relation_data = rel.data[rel.app]
        dbs = relation_data.get('databases')
        databases = json.loads(dbs) if dbs else []

        return databases

    def new_database(self, rel_id=None):
        """Request creation of an additional database

        """
        if not self.charm.unit.is_leader():
            return

        if rel_id is None:
            rel_id = super()._stored.relation_id
        rel = self.framework.model.get_relation(self.relation_name, rel_id)

        rel_data = rel.data[self.charm.app]
        dbs = rel_data.get('requested_databases', 0)
        rel.data[self.charm.app]['requested_databases'] = str(dbs + 1)

    def request_databases(self, n, rel_id=None):
        """Request n databases"""
        if not self.charm.unit.is_leader():
            return

        if rel_id is None:
            rel_id = super()._stored.relation_id

        rel = self.framework.model.get_relation(self.relation_name, rel_id)

        rel.data[self.charm.app]['requested_databases'] = str(n)

    def port(self, rel_id=None):
        """Return the port which the cassandra instance is listening on"""
        if rel_id is None:
            rel_id = super()._stored.relation_id
        rel = self.framework.model.get_relation(self.relation_name, rel_id)

        return rel.data[rel.app].get("port")


class CQLProvider(ProviderBase):
    def __init__(self, charm, name, service, version=None):
        super().__init__(charm, name, service, version)
        self.charm = charm
        events = self.charm.on[name]
        self.framework.observe(events.relation_changed, self.on_cql_changed)

    def update_port(self, relation_name, port):
        if self.charm.unit.is_leader():
            for relation in self.charm.model.relations[relation_name]:
                logger.info(f"Setting address data for relation {relation}")
                if str(port) != relation.data[self.charm.app].get(
                    "port", None
                ):
                    relation.data[self.charm.app]["port"] = str(port)

    def on_cql_changed(self, event):
        if not self.charm.unit.is_leader():
            return

        creds_json = event.relation.data[self.charm.app].get("credentials", None)
        if creds_json is None:
            username = f"juju-user-{event.app.name}"
            password = generate_password()
            self.charm.create_user(event, username, password)
            credentials = (username, password)
            event.relation.data[self.charm.app]["credentials"] = json.dumps(credentials)
        else:
            credentials = json.loads(creds_json)

        num_dbs = int(event.relation.data[event.app].get("requested_databases", 0))
        dbs_json = event.relation.data[self.charm.app].get("databases") or "[]"
        dbs = json.loads(dbs_json)
        if num_dbs > len(dbs):
            for i in range(len(dbs), num_dbs):
                db_name = f"juju_db_{sanitize_name(event.app.name)}_{i}"
                self.charm.create_db(event, db_name, credentials[0])
                dbs.append(db_name)
        event.relation.data[self.charm.app]["databases"] = json.dumps(dbs)


def sanitize_name(name):
    """Make a name safe for use as a keyspace name"""
    # For now just change dashes to underscores. Fix this more in the future
    return name.replace('-', '_')


def generate_password():
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for i in range(20))
