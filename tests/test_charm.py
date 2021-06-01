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

import json
import unittest
import yaml

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


SAMPLE_CONFIG = """authenticator: PasswordAuthenticator
authorizer: CassandraAuthorizer
cluster_name: juju-cluster-cassandra-k8s
commitlog_sync: periodic
commitlog_sync_period_in_ms: 10000
endpoint_snitch: GossipingPropertyFileSnitch
listen_address: 1.1.1.1
native_transport_port: 9042
num_tokens: 256
partitioner: org.apache.cassandra.dht.Murmur3Partitioner
seed_provider:
- class_name: org.apache.cassandra.locator.SimpleSeedProvider
  parameters:
  - seeds: "1.1.1.1"
start_native_transport: 'true'
"""


FILES = {}


def fake_push(self, path, content):
    global FILES
    FILES[path] = content


def fake_pull(self, path):
    return ConfigFile(FILES.get(path, ""))


class ConfigFile:
    def __init__(self, content):
        self.content = content

    def read(self):
        return self.content


@patch("charm.generate_password", new=lambda: "password")
@patch.object(cassandra.cluster.Cluster, "connect", new=FakeConnection())
@patch.object(CassandraOperatorCharm, "_goal_units", new=lambda x: 1)
@patch.object(CassandraOperatorCharm, "_bind_address", new=lambda x: "1.1.1.1")
@patch.object(ops.model.Container, "pull", new=fake_pull)
@patch.object(ops.model.Container, "push", new=fake_push)
class TestCharm(unittest.TestCase):
    @patch.object(CassandraOperatorCharm, "_goal_units", new=lambda x: 1)
    @patch.object(CassandraOperatorCharm, "_bind_address", new=lambda x: "1.1.1.1")
    @patch.object(ops.model.Container, "pull", new=fake_pull)
    @patch.object(ops.model.Container, "push", new=fake_push)
    def setUp(self):
        self.harness = Harness(CassandraOperatorCharm)
        self.addCleanup(self.harness.cleanup)
        self.harness.begin_with_initial_hooks()
        self.harness.set_leader(True)

    def tearDown(self):
        global FILES
        FILES = {}

    def test_relation_is_set(self):
        rel_id = self.harness.add_relation("cql", "otherapp")
        self.assertIsInstance(rel_id, int)
        self.harness.add_relation_unit(rel_id, "otherapp/0")
        self.harness.update_relation_data(rel_id, "otherapp", {})
        self.assertEqual(
            self.harness.get_relation_data(rel_id, self.harness.model.app.name)["port"],
            "9042",
        )

    def test_request_db(self):
        rel_id = self.harness.add_relation("cql", "otherapp")
        self.assertIsInstance(rel_id, int)
        self.harness.add_relation_unit(rel_id, "otherapp/0")
        self.harness.update_relation_data(
            rel_id, "otherapp", {"requested_databases": "1"}
        )
        data = self.harness.get_relation_data(rel_id, "cassandra-k8s")
        assert len(json.loads(data["databases"])) == 1

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
    def test_scale_up(self):
        rel_id = self.harness.charm.model.get_relation("cassandra-peers").id
        self.harness.add_relation_unit(rel_id, "cassandra/1")
        self.harness.update_relation_data(
            rel_id, "cassandra/1", {"peer_address": "1.1.1.1"}
        )
        seeds = self.harness.charm._seeds(None).split(",")
        assert len(seeds) == 2

    def test_config_file_is_set(self):
        sample_content = yaml.safe_load(SAMPLE_CONFIG)
        content_str = (
            self.harness.charm.unit.get_container("cassandra")
            .pull("/etc/cassandra/cassandra.yaml")
            .read()
        )
        content = yaml.safe_load(content_str)
        assert content == sample_content

    def test_prometheus_data_set(self):
        rel_id = self.harness.add_relation("monitoring", "otherapp")
        self.assertIsInstance(rel_id, int)
        self.harness.add_relation_unit(rel_id, "otherapp/0")
        self.harness.update_relation_data(rel_id, "otherapp", {})
        self.assertEqual(
            json.loads(
                self.harness.get_relation_data(rel_id, self.harness.model.app.name)[
                    "targets"
                ]
            ),
            ["1.1.1.1:7070"],
        )
