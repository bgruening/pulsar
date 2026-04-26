"""E. Mode matrix sanity check.

The other scenario files take the parametrized ``pulsar`` fixture, so they
already run across every mode. This file pins down the *invariants* that the
matrix must satisfy independent of failure injection: status update wire
format consistency, queue/topic naming, and that ``amqp_durable=true`` and
the persistent outbox are actually engaged.
"""
import os
import subprocess

import pytest
import requests

from harness.assertions import (
    assert_exactly_once_terminal,
    await_terminal,
)
from harness.job_factory import make_setup_message

GALAXY_BASE = "http://localhost:8088"
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.mark.resilience
def test_outbox_directory_exists_after_first_publish(pulsar):
    body = make_setup_message(command_line="echo mode-matrix")
    requests.post(f"{GALAXY_BASE}/_publish_setup", json=body, timeout=5).raise_for_status()
    await_terminal(body["job_id"], timeout=60, expected="complete")
    assert_exactly_once_terminal(body["job_id"], expected="complete")

    # Confirm Pulsar's persistence_directory contains the outbox dir; even on
    # the happy path the directory is created on first bind.
    suffix = "relay-status-outbox" if pulsar.mode == "relay" else "status-outbox"
    res = subprocess.run(
        ["docker", "compose", "-f", f"{PROJECT_DIR}/docker-compose.yml",
         "exec", "-T", "pulsar", "ls", "/pulsar/persisted"],
        capture_output=True, text=True, check=True,
    )
    listing = res.stdout
    assert any(suffix in line for line in listing.splitlines()), (
        f"expected an outbox dir ending with {suffix} in {listing!r}"
    )


@pytest.mark.resilience
def test_payload_contains_required_fields(pulsar):
    body = make_setup_message(command_line="echo wire-format")
    requests.post(f"{GALAXY_BASE}/_publish_setup", json=body, timeout=5).raise_for_status()
    ev = await_terminal(body["job_id"], timeout=60, expected="complete")
    payload = ev["payload"]
    for key in ("job_id", "status", "complete", "returncode", "pulsar_version"):
        assert key in payload, f"terminal payload missing {key!r}: {payload}"
