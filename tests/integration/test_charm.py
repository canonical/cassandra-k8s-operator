#!/usr/bin/env python3

# Copyright 2020 Canonical Ltd.
# See LICENSE file for licensing details.

import pytest


@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test):
    my_charm = await ops_test.build_charm(".")
    await ops_test.model.set_config({"update-status-hook-interval": "15s"})
    await ops_test.model.deploy(
        my_charm,
        config={"heap_size": "1G"},
        resources={
            "cassandra-image": "cassandra:3.11",
            "cassandra-prometheus-exporter": "cassandra-exporter-agent.jar",
        },
    )
    await ops_test.model.wait_for_idle(status="active")
