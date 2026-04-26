"""Build setup-message payloads for resilience scenarios.

The scenarios don't run real Galaxy tools — they just need a job that runs
a deterministic shell command and surfaces input/output staging through
mock-galaxy's HTTP endpoints. This factory keeps the wire format consistent
across scenarios.
"""
import uuid

GALAXY_URL = "http://toxiproxy:8088"


def make_setup_message(
    job_id=None,
    command_line="echo hello",
    input_files=None,
    output_files=None,
):
    """Build a ``setup`` message body matching pulsar.client.setup_handler.

    Args:
        job_id: deterministic id (default: random uuid)
        command_line: shell command to run
        input_files: list of (remote_path, galaxy_url) pairs to download
        output_files: list of (local_path, galaxy_upload_url) pairs to upload

    Returns:
        Dict ready to POST to mock-galaxy's ``/_publish_setup`` endpoint.
    """
    job_id = job_id or uuid.uuid4().hex[:12]
    setup_actions = []
    if input_files:
        for remote_path, url in input_files:
            setup_actions.append({
                "name": remote_path,
                "type": "input",
                "action": {
                    "action_type": "remote_transfer",
                    "url": f"{GALAXY_URL}{url}",
                    "source": {"path": remote_path},
                    "path": remote_path,
                },
            })
    output_actions = []
    if output_files:
        for remote_path, url in output_files:
            output_actions.append({
                "name": remote_path,
                "type": "output",
                "action": {
                    "action_type": "remote_transfer",
                    "url": f"{GALAXY_URL}{url}",
                    "source": {"path": remote_path},
                    "path": remote_path,
                },
            })

    body = {
        "job_id": job_id,
        "command_line": command_line,
        "setup": True,
        "remote_staging": {
            "setup": setup_actions,
            "action_mapper": {"default_action": "remote_transfer"},
            "client_outputs": {"action_mapper": {"default_action": "remote_transfer"}},
        },
    }
    if output_actions:
        body["remote_staging"].setdefault("postprocess", []).extend(output_actions)
    return body
