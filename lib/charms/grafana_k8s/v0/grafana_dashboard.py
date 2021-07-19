import base64
import json
import jsonpickle
import logging
import zlib

from jinja2 import Template

from ops.charm import CharmBase, CharmEvents, RelationChangedEvent
from ops.model import Unit
from ops.framework import EventBase, EventSource, StoredState
from ops.relation import ConsumerBase, ProviderBase

from typing import Dict, List

LIBID = "987654321"
LIBAPI = 1
LIBPATCH = 0

logger = logging.getLogger(__name__)


class GrafanaDashboardsChanged(EventBase):
    """Event emitted when Grafana dashboards change"""

    def __init__(self, handle, data=None):
        super().__init__(handle)
        self.data = data

    def snapshot(self) -> Dict:
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
        """Construct a Grafana dashboard charm client.

        The :class:`GrafanaDashboardConsumer` object provides an interface
        to Grafana. This interface supports providing additional
        dashboards for Grafana to display. For example, if a charm
        exposes some metrics which are consumable by a dashboard
        (such as Prometheus), then an additional dashboard can be added
        by instantiating a :class:`GrafanaDashboardConsumer` object and
        adding its datasources as follows:

            self.grafana = GrafanaConsumer(self, "grafana-source", {"grafana-source"}: ">=2.0"})
            self.grafana.add_dashboard(data: str)

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
        """
        Add a dashboard to Grafana. `data` should be a string representing
        a jinja template which can be templated with the appropriate
        `grafana_datasource` and `prometheus_job_name`
        """
        rel = self.framework.model.get_relation(self.name, rel_id)
        rel_id = rel_id if rel_id is not None else rel.id

        prom_rel = self.framework.model.get_relation("monitoring")
        prom_unit = prom_rel.units.pop()

        prom_target = json.loads(prom_rel.data[self.charm.app]["job_name"])
        prom_job = "{}_juju_{}_{}_{}".format(
            prom_target,
            self.charm.model.name,
            prom_unit.app.name,
            prom_rel.id
        )

        self._update_dashboards(data, rel_id, prom_unit, prom_target, prom_job)

    def _update_dashboards(self, data: str, rel_id: int, prom_unit: Unit, prom_job: str, prom_target: str) -> None:
        """
        Update the dashboards in the relation data bucket
        """
        stored_data = {
            "monitoring": jsonpickle.encode(prom_unit),
            "monitoring_target": prom_target,
            "monitoring_job": prom_job,
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
                instance of the Grafana dashboard service.
            name: string name of the relation that is provides the
                Grafana dashboard service.
            service: string name of service provided. This is used by
                :class:`GrafanaDashboardProvider` to validate this service as
                acceptable. Hence the string name must match one of the
                acceptable service names in the :class:`GrafanaDashboardProvider`s
                `consumes` argument. Typically this string is just "grafana".
            version: a string providing the semantic version of the Grafana
                dashboard being provided.

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

        If there are changes in relations between Grafana dashboard providers
        and consumers, this event handler (if the unit is the leader) will
        get data for an incoming grafana-dashboard relation through a
        :class:`GrafanaDashboardssChanged` event, and make the relation data
        is available in the app's datastore object. The Grafana charm can
        then respond to the event to update its configuration
        """
        if not self.charm.unit.is_leader():
            return

        rel = event.relation

        data = (
            json.loads(event.relation.data[event.app].get("dashboards", {}))
            if event.relation.data[event.app].get("dashboards", {})
            else None
        )
        if not data:
            logger.info("NO DATA")
            return

        # Not all operator framework objects can be encoded to JSON or rpicled normally
        # but unpickle it and try to find a matching application to ghe one on the other side
        # of the dashboards so we know we have the right datasource
        source_prom_rel = jsonpickle.decode(data["monitoring"])
        our_prom_rel = self.charm.framework.model.get_relation('grafana-source')

        source = None
        for p in our_prom_rel.units:
            if source_prom_rel.app.name == p.app.name:
                source = p
                break

        if source is None:
            logger.error("No matching Prometheus relation for dashboard source")
            return

        grafana_datasource = "{}".format(
            [x["source-name"] for x in self.charm.source_provider.sources if source.app.name in x["source-name"]][0]
        )

        # The dashboards are WAY too big since this ultimately calls out to Juju to set the relation data,
        # and it overflows the maximum argument length for subprocess, so we have to use b64, annoyingly.

        # Worse, Python3 expects absolutely everything to be a byte, and a plain `base64.b64encode()` is still
        # too large, so we have to go through hoops of encoding to byte, compressing with zlib, converting
        # to base64 so it can be converted to JSON, then all the way back
        tm = Template(zlib.decompress(base64.b64decode(data["template"].encode())).decode())
        msg = tm.render(grafana_datasource=grafana_datasource,
                        prometheus_target=data["monitoring_target"],
                        prometheus_job=data["monitoring_job"])

        msg = base64.b64encode(zlib.compress(msg.encode(), 9)).decode()

        if not json.dumps(self._stored.dashboards.get(rel.id, "")) == json.dumps(msg):
            self._stored.dashboards[rel.id] = msg
            self.on.dashboards_changed.emit()

    @property
    def dashboards(self) -> List:
        """
        Returns a list of known dashboards
        """
        dashboards = []
        for dash in self._stored.dashboards.values():
            dashboards.append(dash)
        return dashboards
