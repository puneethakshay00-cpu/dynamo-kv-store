"""Properties checked over many seeded fault-injection scenarios."""

from dynamokv.config import Config
from dynamokv.experiments import sweep
from dynamokv.sim import WorkloadSettings, run_scenario

SHORT = WorkloadSettings(duration=3.0, settle=8.0)


def test_runs_are_deterministic():
    first, second = run_scenario(11, settings=SHORT), run_scenario(11, settings=SHORT)
    assert first.as_dict() == second.as_dict()


def test_no_acknowledged_write_is_lost_under_crashes_partitions_and_loss():
    reports = sweep(range(30), settings=SHORT, workers=2)
    assert all(r.ok for r in reports), [r.summary() for r in reports if not r.ok]
    assert sum(r.writes_ok for r in reports) > 5000
    assert sum(len(r.faults) for r in reports) > 50


def test_strict_quorum_never_serves_a_stale_read():
    reports = sweep(range(10), Config(sloppy_quorum=False), settings=SHORT, workers=2)
    assert all(r.ok for r in reports)
    assert sum(r.stale_reads for r in reports) == 0


def test_the_papers_vector_clocks_lose_acknowledged_writes():
    report = run_scenario(0, Config(clock="vclock"), SHORT, faults=False)
    assert report.lost_writes
