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
from unittest.mock import patch

import cassandra.cluster
import ops.model
import ops.testing
import yaml
from ops.testing import Harness

from charm import CassandraOperatorCharm


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
        rel_id = self.harness.add_relation("database", "otherapp")
        self.assertIsInstance(rel_id, int)
        self.harness.add_relation_unit(rel_id, "otherapp/0")
        self.harness.update_relation_data(rel_id, "otherapp", {})
        self.assertEqual(
            self.harness.get_relation_data(rel_id, self.harness.model.app.name)["port"],
            "9042",
        )
        self.assertEqual(
            self.harness.get_relation_data(rel_id, self.harness.model.app.name)["address"],
            "1.1.1.1",
        )

    def test_request_db(self):
        rel_id = self.harness.add_relation("database", "otherapp")
        self.assertIsInstance(rel_id, int)
        self.harness.add_relation_unit(rel_id, "otherapp/0")
        self.harness.update_relation_data(
            rel_id, "otherapp", {"requested_databases": '["db_name"]'}
        )
        data = self.harness.get_relation_data(rel_id, "cassandra-k8s")
        assert len(json.loads(data["databases"])) == 1

    def test_port_change(self):
        rel_id = self.harness.add_relation("database", "otherapp")
        self.assertIsInstance(rel_id, int)
        self.harness.add_relation_unit(rel_id, "otherapp/0")
        self.harness.update_relation_data(rel_id, "otherapp", {})
        self.harness.update_config({"port": "9043"})
        self.assertEqual(
            self.harness.get_relation_data(rel_id, self.harness.model.app.name)["port"],
            "9043",
        )

    def test_root_password_is_set(self):
        rel = self.harness.charm.model.get_relation("cassandra-peers")
        self.assertEqual(rel.data[self.harness.charm.app].get("root_password", None), None)
        self.assertEqual(bool(self.harness.charm.cassandra.root_password()), True)

    @patch.object(CassandraOperatorCharm, "_goal_units", new=lambda x: 2)
    def test_scale_up(self):
        rel_id = self.harness.charm.model.get_relation("cassandra-peers").id
        self.harness.add_relation_unit(rel_id, "cassandra/1")
        self.harness.update_relation_data(rel_id, "cassandra/1", {"peer_address": "1.1.1.1"})
        seeds = self.harness.charm._seeds().split(",")
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

    @patch("ops.testing._TestingModelBackend.network_get")
    @patch("ops.testing._TestingPebbleClient.list_files")
    def test_prometheus_data_set(self, mock_net_get, mock_list_files):
        bind_address = "1.1.1.1"
        fake_network = {
            "bind-addresses": [
                {
                    "interface-name": "eth0",
                    "addresses": [{"hostname": "cassandra-tester-0", "value": bind_address}],
                }
            ]
        }
        mock_net_get.return_value = fake_network
        rel_id = self.harness.add_relation("monitoring", "otherapp")
        self.assertIsInstance(rel_id, int)
        self.harness.add_relation_unit(rel_id, "otherapp/0")
        self.harness.update_relation_data(rel_id, "otherapp", {})
        self.assertEqual(
            json.loads(
                self.harness.get_relation_data(rel_id, self.harness.model.app.name)["scrape_jobs"]
            )[0]["static_configs"][0]["targets"],
            ["*:9500"],
        )

    @patch("ops.testing._TestingModelBackend.network_get")
    @patch("ops.testing._TestingPebbleClient.list_files")
    def test_heap_size_default(self, mock_net_get, mock_list_files):
        cassandra_environment = self._start_cassandra_and_get_pebble_service().environment

        self.assertEqual(cassandra_environment["JVM_OPTS"], "-Xms6G -Xmx6G")
        self.assertEqual(self.harness.model.unit.status, ops.model.ActiveStatus())

    @patch("ops.testing._TestingModelBackend.network_get")
    @patch("ops.testing._TestingPebbleClient.list_files")
    def test_heap_size_config_success(self, mock_net_get, mock_list_files):
        self.harness.update_config({"heap_size": "1g"})

        cassandra_environment = self._start_cassandra_and_get_pebble_service().environment

        self.assertEqual(cassandra_environment["JVM_OPTS"], "-Xms1g -Xmx1g")
        self.assertEqual(self.harness.model.unit.status, ops.model.ActiveStatus())

    @patch("ops.testing._TestingModelBackend.network_get")
    @patch("ops.testing._TestingPebbleClient.list_files")
    def test_heap_size_config_invalid(self, mock_net_get, mock_list_files):
        self.harness.update_config({"heap_size": "0.5g"})

        self.assertEqual(
            self.harness.model.unit.status,
            ops.model.BlockedStatus("Invalid Cassandra heap size setting: '0.5g'"),
        )

    def _start_cassandra_and_get_pebble_service(self):
        container = self.harness.model.unit.get_container("cassandra")

        self.harness.charm.on.cassandra_pebble_ready.emit(container)

        pebble_plan = self.harness.get_container_pebble_plan("cassandra")
        return pebble_plan.services["cassandra"]
