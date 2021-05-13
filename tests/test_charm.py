# Copyright 2020 dylan
# See LICENSE file for licensing details.

import unittest

import cassandra.cluster
import ops.model

from ops.testing import Harness
from charm import CassandraOperatorCharm
from unittest.mock import patch


class FakeConnection:
    def __init__(self, responses=None):
        self.responses = responses if responses is not None else {}

    def __call__(self, event=None):
        return self

    def __enter__(self):
        return self

    def __exit__(self, x_type, x_value, x_traceback):
        pass

    def execute(self, query, wildcards=None):
        return self.responses.get(query)


class ConfigFile:
    def read(self):
        return ""


@patch("charm.generate_password", new=lambda: "password")
@patch.object(cassandra.cluster.Cluster, "connect", new=FakeConnection())
@patch.object(CassandraOperatorCharm, "_goal_units", new=lambda x: 1)
@patch.object(CassandraOperatorCharm, "_bind_address", new=lambda x: "1.1.1.1")
@patch.object(ops.model.Container, "pull", new=lambda x, y: ConfigFile())
@patch.object(ops.model.Container, "push", new=lambda x, y, z: None)
class TestCharm(unittest.TestCase):
    @patch.object(CassandraOperatorCharm, "_goal_units", new=lambda x: 1)
    @patch.object(CassandraOperatorCharm, "_bind_address", new=lambda x: "1.1.1.1")
    @patch.object(ops.model.Container, "pull", new=lambda x, y: ConfigFile())
    @patch.object(ops.model.Container, "push", new=lambda x, y, z: None)
    def setUp(self):
        self.harness = Harness(CassandraOperatorCharm)
        self.addCleanup(self.harness.cleanup)
        self.harness.begin_with_initial_hooks()
        self.harness.set_leader(True)

    def test_relation_is_set(self):
        rel_id = self.harness.add_relation("cql", "otherapp")
        self.assertIsInstance(rel_id, int)
        self.harness.add_relation_unit(rel_id, "otherapp/0")
        self.harness.update_relation_data(rel_id, "otherapp", {})
        self.assertEqual(
            self.harness.get_relation_data(rel_id, self.harness.model.app.name)["port"],
            "9042",
        )

    def test_port_change(self):
        rel_id = self.harness.add_relation("cql", "otherapp")
        self.assertIsInstance(rel_id, int)
        self.harness.add_relation_unit(rel_id, "otherapp/0")
        self.harness.update_relation_data(rel_id, "otherapp", {})
        self.harness.update_config({"port": "9043"})
        self.assertEqual(
            self.harness.get_relation_data(rel_id, self.harness.model.app.name)["port"],
            "9043",
        )

    def test_root_password_is_set(self):
        self.assertEqual(self.harness.charm._stored.root_password, "")
        self.assertEqual(self.harness.charm._root_password(None), "password")

    @patch.object(CassandraOperatorCharm, "_goal_units", new=lambda x: 2)
    def test_peers_changed(self):
        rel_id = self.harness.charm.model.get_relation("cassandra-peers").id
        self.harness.add_relation_unit(rel_id, "cassandra/1")
        self.harness.update_relation_data(
            rel_id, "cassandra/1", {"peer_address": "1.1.1.1"}
        )
        seeds = self.harness.charm._seeds(None).split(",")
        assert len(seeds) == 2
