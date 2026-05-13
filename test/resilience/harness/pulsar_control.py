"""Drive the Pulsar container from a test: kill, sigterm, restart, wait-ready.

The harness shells out to ``docker compose`` rather than using the Docker
Python SDK to keep the dependency surface small and to make the commands easy
to reproduce by hand when debugging a flaky scenario.
"""
import os
import subprocess
import time

import requests

SERVICE = "pulsar"

# RabbitMQ management API on the broker container; we hit it directly
# (bypassing toxiproxy) so readiness checks aren't perturbed by fault toxics.
RABBITMQ_MGMT = "http://localhost:15672/api"
RABBITMQ_AUTH = ("guest", "guest")


def _compose_env(**overrides):
    """Build a subprocess env that lets ``docker compose`` find its plugin.

    Inherits the parent env (HOME, DOCKER_CONFIG, etc.) — Docker Desktop
    discovers the compose plugin via ``~/.docker/cli-plugins/``, so a
    stripped env makes ``docker compose`` fall through to plain ``docker``
    and fail on ``-f``.
    """
    env = dict(os.environ)
    env.update(overrides)
    return env


def _docker_compose(*args, project_dir):
    cmd = ["docker", "compose", "-f", f"{project_dir}/docker-compose.yml", *args]
    return subprocess.run(cmd, capture_output=True, text=True, check=False, env=_compose_env())


class PulsarControl:
    def __init__(self, project_dir, service=SERVICE, mode="amqp"):
        self.project_dir = project_dir
        self.service = service
        self.mode = mode

    def start(self, wait_ready=True, fresh=False):
        # Force-recreate so PULSAR_MODE changes between parametrized tests
        # actually take effect, and wipe persisted state so prior-run job
        # directories don't trip the F3 idempotent-setup guard.
        if fresh:
            self._wipe_state()
        cmd = ["docker", "compose", "-f", f"{self.project_dir}/docker-compose.yml",
               "up", "-d", "--force-recreate", self.service]
        subprocess.run(
            cmd,
            env=_compose_env(PULSAR_MODE=self.mode),
            check=True,
        )
        if wait_ready:
            self.wait_until_consuming()

    def _wipe_state(self):
        # Remove staging + persistence contents and purge any leftover queues.
        # Pulsar must be down for the bind-mount wipe to be safe.
        subprocess.run(
            ["docker", "compose", "-f", f"{self.project_dir}/docker-compose.yml",
             "rm", "-fsv", self.service],
            env=_compose_env(),
            check=False,
        )
        for vol in ("resilience_pulsar-staging", "resilience_pulsar-persisted"):
            subprocess.run(
                ["docker", "volume", "rm", "-f", vol],
                env=_compose_env(),
                check=False,
            )
        # Wipe RabbitMQ control queues so a redelivered setup from a prior
        # parametrization doesn't surprise the next test.
        try:
            for q in ("pulsar__setup", "pulsar__kill", "pulsar__status",
                      "pulsar__status_update", "pulsar__status_update_ack"):
                requests.delete(
                    f"{RABBITMQ_MGMT}/queues/%2F/{q}/contents",
                    auth=RABBITMQ_AUTH, timeout=2,
                )
        except Exception:
            pass
        # Drop only the topic/message keys in Valkey — never the user table —
        # so the bootstrap admin survives and we don't have to restart the
        # relay container. The Lua script runs server-side: SCAN for keys
        # whose prefix is *not* ``user`` and ``DEL`` them in batches.
        lua = (
            "local cursor='0';"
            "repeat "
            "  local r=redis.call('SCAN',cursor,'COUNT',500);"
            "  cursor=r[1];"
            "  for _,k in ipairs(r[2]) do "
            "    if not (string.sub(k,1,4)=='user') then "
            "      redis.call('DEL',k);"
            "    end "
            "  end "
            "until cursor=='0';"
            "return 'ok'"
        )
        subprocess.run(
            ["docker", "compose", "-f", f"{self.project_dir}/docker-compose.yml",
             "exec", "-T", "relay", "valkey-cli", "EVAL", lua, "0"],
            env=_compose_env(),
            check=False, capture_output=True,
        )
        # Drop mock-galaxy's relay long-poll cursor, JWT cache and recorder
        # via its admin endpoint. No process restart, so the AMQP/relay
        # consumer threads stay attached.
        try:
            requests.post(
                "http://localhost:8088/_consumer/reset", timeout=2,
            )
        except Exception:
            pass

    def stop(self):
        _docker_compose("stop", self.service, project_dir=self.project_dir)

    def kill(self, signal="KILL"):
        _docker_compose("kill", "-s", signal, self.service, project_dir=self.project_dir)
        if self.mode in ("amqp", "amqp_ack"):
            # RabbitMQ doesn't notice the TCP drop until the AMQP heartbeat
            # times out (default 580s) so the queue's `consumers` count
            # stays at 1 long after the container is dead. The next
            # ``start.wait_until_consuming`` would then see the stale count
            # and return before the new pulsar has actually attached. Force
            # the broker to drop the dead consumer by closing each
            # consumer's connection through the management API.
            _force_drop_setup_consumer_connections()

    def sigterm(self):
        self.kill("TERM")

    def restart(self, wait_ready=True):
        self.stop()
        self.start(wait_ready=wait_ready)

    def wait_until_consuming(self, timeout=60.0, poll_interval=0.1):
        """Block until Pulsar has bound consumers for the control queues.

        Both modes wait for a fresh ``bind_manager_to`` log line from the
        *current* pulsar container, scoped via ``--since`` to the start of
        this call. Polling the broker for a non-zero consumer count is
        racy after a kill+restart cycle: the old consumer count can
        linger for several seconds (especially under a toxiproxy
        latency toxic) until RabbitMQ notices the TCP drop, so the check
        passes against the *previous* pulsar container's stale registration.
        For AMQP modes we additionally require the management API to
        show a consumer attached, but only after the bind log confirms
        the new container has actually run bind_app.

        Relay mode additionally waits for ``Acquired pulsar-relay access
        token``, which is logged immediately before the consumer thread
        issues its first long-poll. Without this, the harness can return
        between ``bind_manager_to_relay`` and the first
        ``POST /messages/poll`` — the relay only delivers messages
        published *after* a waiter exists, so a setup message that
        beats pulsar's first poll by even a few ms is permanently lost
        (see B3 flake before this guard).

        ``poll_interval`` defaults to 0.1 s — the docker-compose-logs +
        mgmt-API combo takes ~30 ms each, so a tight poll cadence shaves
        the dead-poll overhead off the suite without saturating either
        endpoint.
        """
        bind_marker = "bind_manager_to"
        # Logged by pulsar_relay_client.auth right before the first
        # long_poll() call lands on the relay.
        relay_first_poll_marker = "Acquired pulsar-relay access token"
        deadline = time.time() + timeout
        start_ts = time.time()
        while time.time() < deadline:
            res = _docker_compose(
                "logs", "--since", f"{int(time.time() - start_ts) + 2}s",
                self.service, project_dir=self.project_dir,
            )
            stdout = res.stdout or ""
            if bind_marker in stdout:
                if self.mode == "relay":
                    if relay_first_poll_marker in stdout:
                        return
                else:
                    # AMQP modes: also confirm the broker sees the consumer.
                    if _amqp_setup_has_consumer():
                        return
            time.sleep(poll_interval)
        raise TimeoutError(
            f"Pulsar did not bind {self.mode} consumers within {timeout}s"
        )


def _amqp_setup_has_consumer():
    try:
        r = requests.get(
            f"{RABBITMQ_MGMT}/queues/%2F/pulsar__setup",
            auth=RABBITMQ_AUTH, timeout=2,
        )
    except Exception:
        return False
    if r.status_code != 200:
        return False
    return int(r.json().get("consumers", 0)) > 0


def _force_drop_setup_consumer_connections():
    """Close every connection currently consuming from pulsar control queues.

    pulsar.kill leaves AMQP heartbeats unacknowledged but the broker won't
    drop the dead consumer until heartbeat timeout (~580s by default). Use
    the RabbitMQ management API to look up consumers on the pulsar control
    queues and explicitly close their connections. Idempotent and safe to
    call when no consumers are present.
    """
    # Only drop consumers on queues *Pulsar* reads from. pulsar__status_update
    # is consumed by mock-galaxy; closing that connection here would silently
    # break the recorder for the rest of the test.
    for q in ("pulsar__setup", "pulsar__kill", "pulsar__status"):
        try:
            r = requests.get(
                f"{RABBITMQ_MGMT}/queues/%2F/{q}",
                auth=RABBITMQ_AUTH, timeout=2,
            )
        except Exception:
            continue
        if r.status_code != 200:
            continue
        for cd in r.json().get("consumer_details", []) or []:
            conn_name = cd.get("channel_details", {}).get("connection_name")
            if not conn_name:
                continue
            try:
                requests.delete(
                    f"{RABBITMQ_MGMT}/connections/{requests.utils.quote(conn_name, safe='')}",
                    auth=RABBITMQ_AUTH, timeout=2,
                    headers={"X-Reason": "resilience-suite force-drop after pulsar kill"},
                )
            except Exception:
                pass
