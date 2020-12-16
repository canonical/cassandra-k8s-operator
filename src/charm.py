#!/usr/bin/env python3
# Copyright 2020 dylan
# See LICENSE file for licensing details.

import logging

from ops.charm import CharmBase
from ops.main import main
from ops.model import ActiveStatus, MaintenanceStatus

log = logging.getLogger(__name__)


CQL_PORT = 9042
CLUSTER_PORT = 7001


class CassandraOperatorCharm(CharmBase):
    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.config_changed, self._on_config_changed)

    def _on_config_changed(self, _):
        self.configure()

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
                    "ports": [
                        {"containerPort": CQL_PORT, "name": "cql", "protocol": "TCP"},
                        {
                            "containerPort": CLUSTER_PORT,
                            "name": "cluster",
                            "protocol": "TCP",
                        },
                    ]
                    # 'kubernetes': # probes here
                }
            ],
        }
        return spec


if __name__ == "__main__":
    main(CassandraOperatorCharm)
