#!/usr/bin/env python3

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

import pytest


@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test):
    my_charm = await ops_test.build_charm(".")
    await ops_test.model.set_config({"update-status-hook-interval": "15s"})
    await ops_test.model.deploy(
        f"{my_charm.parents[0]}/cassandra-k8s_ubuntu-20.04-amd64.charm",
        resources={
            "cassandra-image": "cassandra:3.11",
            "cassandra-prometheus-exporter": "cassandra-exporter-agent.jar",
        },
    )
    await ops_test.model.wait_for_idle(wait_for_active=True)
