# Copyright 2020 dylan
# See LICENSE file for licensing details.

import unittest

import cassandra.cluster

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


@patch("charm.generate_password", new=lambda: "password")
@patch.object(cassandra.cluster.Cluster, "connect", new=FakeConnection())
@patch.object(CassandraOperatorCharm, "goal_units", new=lambda x: 1)
@patch.object(CassandraOperatorCharm, "cql_address", new=lambda x: "1.1.1.1")
class TestCharm(unittest.TestCase):
    def setUp(self):
        self.harness = Harness(CassandraOperatorCharm)
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()
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
        with patch.object(
            CassandraOperatorCharm,
            "goal_units",
            return_value=self.harness.charm.num_units(),
        ):
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
        self.assertEqual(self.harness.charm.stored.root_password, "")
        self.assertEqual(self.harness.charm.root_password(None), "password")
