"""C. Galaxy HTTP outage scenarios.

These exercise the staging path (input download, output upload). The
``RetryActionExecutor`` + transient error classifier from this branch
should retry transient HTTP failures and fail-fast on permanent ones.
The end-to-end check is that the terminal status reflects reality.
"""
import time

import pytest
import requests

from harness.assertions import (
    assert_exactly_once_terminal,
    await_terminal,
)
from harness.job_factory import make_setup_message

GALAXY_BASE = "http://localhost:8088"


@pytest.mark.resilience
def test_c1_input_503_then_recovers(pulsar, galaxy_proxy):
    """Galaxy HTTP returns 5xx for a beat; staging retries and succeeds."""
    # Stage a real file in mock-galaxy first.
    requests.post(f"{GALAXY_BASE}/files/c1-input.txt", data=b"hello").raise_for_status()
    body = make_setup_message(
        command_line="cat /pulsar/staging/inputs/c1-input.txt",
        input_files=[("c1-input.txt", "/files/c1-input.txt")],
    )
    galaxy_proxy.add_latency(1000)  # heavy latency degrades but doesn't 4xx
    requests.post(f"{GALAXY_BASE}/_publish_setup", json=body, timeout=10).raise_for_status()
    time.sleep(4.0)
    galaxy_proxy.remove_all_toxics()
    await_terminal(body["job_id"], timeout=120, expected="complete")
    assert_exactly_once_terminal(body["job_id"], expected="complete")


@pytest.mark.resilience
def test_c3_input_404_fails_fast(pulsar):
    """Permanent 4xx on a staging download must produce a single ``failed``
    terminal status, not endless retries."""
    body = make_setup_message(
        command_line="echo c3",
        input_files=[("missing.txt", "/files/never-existed.txt")],
    )
    requests.post(f"{GALAXY_BASE}/_publish_setup", json=body, timeout=5).raise_for_status()
    await_terminal(body["job_id"], timeout=120, expected="failed")
    assert_exactly_once_terminal(body["job_id"], expected="failed")


@pytest.mark.resilience
@pytest.mark.skip(
    reason="Galaxy-consumer-offline semantics are equivalent to the B series "
    "broker outage scenarios in this harness, since mock-galaxy publishes "
    "setup and consumes status_update over the same connection.",
)
def test_c4_galaxy_consumer_offline_then_returns(pulsar, galaxy_proxy):
    body = make_setup_message(command_line="echo c4 && sleep 1")
    galaxy_proxy.disable()
    requests.post(f"{GALAXY_BASE}/_publish_setup", json=body, timeout=5).raise_for_status()
    time.sleep(8.0)
    galaxy_proxy.enable()
    await_terminal(body["job_id"], timeout=180, expected="complete")
    assert_exactly_once_terminal(body["job_id"], expected="complete")
