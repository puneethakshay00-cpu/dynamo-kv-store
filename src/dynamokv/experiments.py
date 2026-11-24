"""Batches of simulator runs, spread over processes."""

from __future__ import annotations

from collections.abc import Iterable
from concurrent.futures import ProcessPoolExecutor

from .checker import Report
from .config import Config
from .sim import WorkloadSettings, run_scenario


def _run(job: tuple) -> Report:
    seed, config, faults, settings = job
    return run_scenario(seed, config, settings, faults=faults)


def sweep(seeds: Iterable[int], config: Config = Config(), faults: bool = True, workers: int = 1,
          settings: WorkloadSettings = WorkloadSettings()) -> list[Report]:
    jobs = [(seed, config, faults, settings) for seed in seeds]
    if workers <= 1 or len(jobs) < 4:
        return [_run(job) for job in jobs]
    with ProcessPoolExecutor(workers) as pool:
        return list(pool.map(_run, jobs, chunksize=4))
