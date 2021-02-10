#!/usr/bin/env python3
# Copyright 2020 dylan
# See LICENSE file for licensing details.

import logging

from ops.charm import CharmBase
from ops.main import main
from ops.model import ActiveStatus, MaintenanceStatus

log = logging.getLogger(__name__)


CLUSTER_PORT = 7001
UNIT_ADDRESS = "{}-{}.{}-endpoints.{}.svc.cluster.local"


class CassandraOperatorCharm(CharmBase):
    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.config_changed, self.on_config_changed)
        self.framework.observe(self.on["cql"].relation_changed, self.on_cql_changed)

    def on_config_changed(self, _):
        self.configure()
        for relation in self.model.relations["cql"]:
            self.update_cql(relation)

    def on_cql_changed(self, event):
        self.update_cql(event.relation)

    def update_cql(self, relation):
        if self.unit.is_leader():
            log.info("Setting relation data")
            if str(self.model.config["port"]) != relation.data[self.app].get(
                "port", None
            ):
                relation.data[self.app]["port"] = str(self.model.config["port"])

    def configure(self):
        if not self.unit.is_leader():
            log.debug("Unit is not leader. Cannot set pod spec.")
            self.unit.status = ActiveStatus()
            return

        self.unit.status = MaintenanceStatus("Building pod spec.")
        log.debug("Building pod spec.")

        pod_spec = self.build_pod_spec()
        log.debug("Setting pod spec.")
        self.model.pod.set_spec(pod_spec)

        self.unit.status = ActiveStatus()
        log.debug("Pod spec set successfully.")

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
                        "echo $(cat /etc/hosts | grep cassandra-endpoints | cut -f 1) listen-addr"
                        " >> /etc/hosts && docker-entrypoint.sh",
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
                    # 'kubernetes': # probes here
                    "envConfig": {
                        "CASSANDRA_CLUSTER_NAME": "charm-cluster",
                        "CASSANDRA_SEEDS": self.seeds(),
                        "CASSANDRA_LISTEN_ADDRESS": "listen-addr",
                    },
                }
            ],
        }
        return spec

    def seeds(self):
        seeds = UNIT_ADDRESS.format(self.meta.name, 0, self.meta.name, self.model.name)
        num_units = self.num_units()
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
        relation = self.model.get_relation("cassandra")
        # The relation does not list ourself as a unit so we must add 1
        return len(relation.units) + 1 if relation is not None else 1


if __name__ == "__main__":
    main(CassandraOperatorCharm)
