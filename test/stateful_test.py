"""Tests for terminal status handling in :class:`StatefulManagerProxy`.

Every manager runs behind this proxy, so this is where a status returned by a
proxied manager becomes deactivation, staging, and the state-change callback
that message-driven clients depend on - they never poll for status, so a
terminal status that fires no callback is a job that hangs forever.
"""
import time
from contextlib import contextmanager
from shutil import rmtree

from pulsar.managers import status
from pulsar.managers.queued import QueueManager
from pulsar.managers.stateful import StatefulManagerProxy
from .test_utils import minimal_app_for_managers

TEST_JOB_ID = "4"
# An empty remote staging config is enough for postprocessing to find nothing to
# collect and report success, without dragging a real action mapper into these
# tests.
TEST_LAUNCH_CONFIG = {"command_line": "true", "remote_staging": {}}


class _ScriptedStatusManager(QueueManager):
    """Runs nothing, reports whatever status the test scripts."""

    def __init__(self, *args, **kwds):
        super().__init__(*args, **kwds)
        self.scripted_status = status.QUEUED
        self.deactivated = []

    def get_status(self, job_id):
        return self.scripted_status

    def _deactivate_job(self, job_id):
        self.deactivated.append(job_id)


class _FailingLaunchManager(_ScriptedStatusManager):
    """Fails at launch, the way a manager that cannot reach its DRM would."""

    def launch(self, *args, **kwds):
        raise Exception("Test failure launching job")


class _RecordingStatefulManagerProxy(StatefulManagerProxy):
    """Records state changes without starting a monitor thread.

    ``set_state_change_callback`` would also build a ``ManagerMonitor``, whose
    polling would race the explicit ``get_status`` calls these tests make.
    """

    def __init__(self, manager, **kwds):
        super().__init__(manager, **kwds)
        self.callbacks = []

    def _default_status_change_callback(self, job_status, job_id):
        self.callbacks.append((job_status, job_id))


def test_failed_status_deactivates_and_notifies():
    with _launched_job() as (proxy, manager, job_id):
        manager.scripted_status = status.FAILED
        # Outputs are staged back before the job is reported terminal, so a job
        # killed for walltime or memory still returns its partial stdout/stderr.
        assert proxy.get_status(job_id) == status.POSTPROCESSING
        _wait_for_callback(proxy)
        assert proxy.callbacks == [(status.FAILED, job_id)]
        assert proxy.active_jobs.active_job_ids() == []
        assert manager.deactivated == [job_id]
        assert proxy.get_status(job_id) == status.FAILED


def test_lost_status_is_not_terminal():
    with _launched_job() as (proxy, manager, job_id):
        manager.scripted_status = status.LOST
        assert proxy.get_status(job_id) == status.LOST
        # A manager reports LOST for a job whose external id it has not recovered
        # yet, and the monitor is bound before recover_active_jobs runs, so the
        # job has to stay active and be allowed to come back.
        assert proxy.active_jobs.active_job_ids() == [job_id]
        assert manager.deactivated == []
        time.sleep(.1)
        assert proxy.callbacks == []

        manager.scripted_status = status.COMPLETE
        assert proxy.get_status(job_id) == status.POSTPROCESSING
        _wait_for_callback(proxy)
        assert proxy.callbacks == [(status.COMPLETE, job_id)]


def test_complete_status_postprocesses_and_notifies():
    with _launched_job() as (proxy, manager, job_id):
        manager.scripted_status = status.COMPLETE
        assert proxy.get_status(job_id) == status.POSTPROCESSING
        _wait_for_callback(proxy)
        assert proxy.callbacks == [(status.COMPLETE, job_id)]
        assert proxy.active_jobs.active_job_ids() == []
        assert proxy.get_status(job_id) == status.COMPLETE


def test_cancelled_status_deactivates_without_notification():
    with _launched_job() as (proxy, manager, job_id):
        manager.scripted_status = status.CANCELLED
        assert proxy.get_status(job_id) == status.CANCELLED
        assert proxy.active_jobs.active_job_ids() == []
        assert manager.deactivated == [job_id]
        # The client asked for the cancellation, so it is not waiting to be told.
        time.sleep(.1)
        assert proxy.callbacks == []


def test_terminal_status_is_reported_once():
    with _launched_job() as (proxy, manager, job_id):
        manager.scripted_status = status.FAILED
        proxy.get_status(job_id)
        _wait_for_callback(proxy)
        # A terminal status is recorded, so the proxied manager is never asked
        # again and cannot walk the job back out of it.
        manager.scripted_status = status.RUNNING
        for _ in range(3):
            assert proxy.get_status(job_id) == status.FAILED
        assert proxy.callbacks == [(status.FAILED, job_id)]


def test_preprocessing_failure_is_reported_once():
    with _proxy(_FailingLaunchManager) as (proxy, manager):
        job_id = proxy.setup_job(TEST_JOB_ID, "tool1", "1.0.0")
        proxy.preprocess_and_launch(job_id, TEST_LAUNCH_CONFIG)
        assert proxy.callbacks == [(status.FAILED, job_id)]
        for _ in range(3):
            assert proxy.get_status(job_id) == status.FAILED
        # Nothing ran, so there is nothing to stage back and nothing more to say.
        time.sleep(.1)
        assert proxy.callbacks == [(status.FAILED, job_id)]


@contextmanager
def _proxy(manager_class=_ScriptedStatusManager):
    app = minimal_app_for_managers()
    manager = manager_class("test", app, num_concurrent_jobs=0)
    proxy = _RecordingStatefulManagerProxy(manager)
    try:
        yield proxy, manager
    finally:
        try:
            proxy.shutdown()
        except Exception:
            pass
        rmtree(app.staging_directory, ignore_errors=True)


@contextmanager
def _launched_job():
    with _proxy() as (proxy, manager):
        job_id = proxy.setup_job(TEST_JOB_ID, "tool1", "1.0.0")
        proxy.preprocess_and_launch(job_id, TEST_LAUNCH_CONFIG)
        assert proxy.active_jobs.active_job_ids() == [job_id]
        yield proxy, manager, job_id


def _wait_for_callback(proxy, timeout=5):
    time_end = time.time() + timeout
    while time.time() < time_end:
        if proxy.callbacks:
            return
        time.sleep(.01)
    raise AssertionError("Timed out waiting for a state change callback.")
