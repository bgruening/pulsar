"""Thread-safe append-only recorder of status updates received from Pulsar.

Tests assert against this recorder via the ``/_recorder`` endpoints exposed by
the mock Galaxy app: list events, await a terminal status for a given job,
clear between scenarios.
"""
import threading
import time
from collections import defaultdict


class StatusRecorder:
    TERMINAL = {"complete", "failed", "cancelled", "lost"}

    def __init__(self):
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._events = []
        self._by_job = defaultdict(list)

    def record(self, payload):
        job_id = payload.get("job_id", "unknown")
        status = payload.get("status", "unknown")
        ts = time.time()
        event = {"job_id": job_id, "status": status, "ts": ts, "payload": payload}
        with self._cv:
            # Dedupe by (job_id, status). Pulsar's outbox can legitimately
            # re-publish a message whose HTTP/AMQP confirmation was lost but
            # whose payload reached the broker; Galaxy's job state machine
            # is idempotent in (job_id, status), so we model Galaxy after
            # dedup. Tests checking for *no* re-publish use a recorder
            # behavior we don't expose here.
            for prior in self._by_job.get(job_id, ()):
                if prior["status"] == status:
                    return
            self._events.append(event)
            self._by_job[job_id].append(event)
            self._cv.notify_all()

    def events(self, job_id=None):
        with self._lock:
            if job_id is None:
                return list(self._events)
            return list(self._by_job.get(job_id, ()))

    def clear(self):
        with self._lock:
            self._events.clear()
            self._by_job.clear()

    def await_terminal(self, job_id, timeout=60.0):
        deadline = time.time() + timeout
        with self._cv:
            while True:
                for ev in self._by_job.get(job_id, ()):
                    if ev["status"] in self.TERMINAL:
                        return ev
                remaining = deadline - time.time()
                if remaining <= 0:
                    return None
                self._cv.wait(timeout=remaining)
