import base64
import json
import jsonpickle
import logging
import zlib

from ops.charm import CharmBase, CharmEvents, RelationBrokenEvent, RelationChangedEvent
from ops.framework import EventBase, EventSource, StoredState
from ops.relation import ConsumerBase, ProviderBase
from typing import List


LIBID = "987654321"
LIBAPI = 1
LIBPATCH = 0

logger = logging.getLogger(__name__)


class GrafanaDashboardsChanged(EventBase):
    """Event emitted when Grafana sources change"""

    def __init__(self, handle, data=None):
        super().__init__(handle)
        self.data = data

    def snapshot(self):
        """Save grafana source information"""
        return {"data": self.data}

    def restore(self, snapshot):
        """Restore grafana source information"""
        self.data = snapshot["data"]


class GrafanaDashboardEvents(CharmEvents):
    """Events raised by :class:`GrafanaSourceEvents`"""

    dashboards_changed = EventSource(GrafanaDashboardsChanged)


class GrafanaDashboardConsumer(ConsumerBase):
    _stored = StoredState()

    def __init__(
        self, charm: CharmBase, name: str, consumes: dict, multi=False
    ) -> None:
        """Construct a Grafana charm client.

        The :class:`GrafanaConsumer` object provides an interface
        to Grafana. This interface supports providing additional
        sources for Grafana to monitor. For example, if a charm
        exposes some metrics which are consumable by a dashboard
        (such as Prometheus), then an additional source can be added
        by instantiating a :class:`GrafanaConsumer` object and
        adding its datasources as follows:

            self.grafana = GrafanaConsumer(self, "grafana-source", {"grafana-source"}: ">=2.0"})
            self.grafana.add_source({
                "source-type": <source-type>,
                "address": <address>,
                "port": <port>
            })

        Args:

            charm: a :class:`CharmBase` object which manages this
                :class:`GrafanaConsumer` object. Generally this is
                `self` in the instantiating class.
            name: a :string: name of the relation between `charm`
                the Grafana charmed service.
            consumes: a :dict: of acceptable monitoring service
                providers. The keys of the dictionary are :string:
                names of grafana source service providers. Typically,
                this is `grafana-source`. The values of the dictionary
                are corresponding minimal acceptable semantic versions
                for the service.
            multi: an optional (default `False`) flag to indicate if
                this object should support interacting with multiple
                service providers.

        """
        super().__init__(charm, name, consumes, multi)

        self.charm = charm

        self._stored.set_default(dashboards={})

    def add_dashboard(self, data: str, rel_id=None) -> None:
        rel = self.framework.model.get_relation(self.name, rel_id)
        rel_id = rel_id if rel_id is not None else rel.id

        prom_rel = self.framework.model.get_relation("monitoring")
        prom_unit = prom_rel.units.pop()

        prom_target = "{}_juju_{}_{}_{}".format(
            json.loads(prom_rel.data[self.charm.app]["job_name"]),
            self.charm.model.name,
            prom_unit.app.name,
            prom_rel.id
        )

        self._update_dashboards(data, rel_id, prom_unit, prom_target)

    def _update_dashboards(self, data, rel_id, prom_unit, prom_target):
        stored_data = {
            "monitoring": jsonpickle.encode(prom_unit),
            "monitoring_target": prom_target,
            "rel_id": rel_id,
            "template": base64.b64encode(zlib.compress(data.encode(), 9)).decode()
        }
        rel = self.framework.model.get_relation(self.name, rel_id)

        self._stored.dashboards[rel_id] = stored_data
        rel.data[self.charm.app]["dashboards"] = json.dumps(
            stored_data
        )


class GrafanaDashboardProvider(ProviderBase):
    on = GrafanaDashboardEvents()
    _stored = StoredState()

    def __init__(self, charm: CharmBase, name: str, service: str, version=None) -> None:
        """A Grafana based Monitoring service consumer

        Args:
            charm: a :class:`CharmBase` instance that manages this
                instance of the Grafana source service.
            name: string name of the relation that is provides the
                Grafana source service.
            service: string name of service provided. This is used by
                :class:`GrafanaProvider` to validate this service as
                acceptable. Hence the string name must match one of the
                acceptable service names in the :class:`GrafanaSourceProvider`s
                `consumes` argument. Typically this string is just "grafana".
            version: a string providing the semantic version of the Grafana
                source being provided.

        """
        super().__init__(charm, name, service, version)
        self.charm = charm
        events = self.charm.on[name]

        self._stored.set_default(dashboards=dict())

        self.framework.observe(
            events.relation_changed, self.on_grafana_dashboard_relation_changed
        )

    def on_grafana_dashboard_relation_changed(self, event: RelationChangedEvent) -> None:
        """Handle relation changes in related consumers.

        If there are changes in relations between Grafana source providers
        and consumers, this event handler (if the unit is the leader) will
        get data for an incoming grafana-source relation through a
        :class:`GrafanaSourcesChanged` event, and make the relation data
        is available in the app's datastore object. The Grafana charm can
        then respond to the event to update its configuration
        """
        if not self.charm.unit.is_leader():
            return

        logger.info("CHANGED {}".format(event))
        logger.info("FROM {}".format(event.app))
        logger.info("REL {}".format(event.relation))

        rel = event.relation

        logger.info("DATA: {}".format(event.relation.data[event.app]))
        logger.info("REL ID: {}".format(rel.id))

        data = (
            json.loads(event.relation.data[event.app].get("dashboards", {}))
            if event.relation.data[event.app].get("dashboards", {})
            else None
        )
        if not data:
            logger.info("NO DATA")
            return

        self._stored.dashboards[rel.id] = data
        self.on.dashboards_changed.emit()

    @property
    def dashboards(self):
        dashboards = []
        for dash in self._stored.dashboards.values():
            dashboards.append(dash)
        return dashboards