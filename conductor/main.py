#!/usr/bin/env python
import asyncio
import sys
import traceback
import warnings
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, Set

import toml
from crontab import CronTab

from . import consts
from .exceptions import JobFormatError, JobFormatWarning
from .job import Job
from .utils import load_run_next, log, update_run_next

POLL_PERIOD: int = 60  # seconds


class Main:
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.jobs: Dict[str, Job] = {}
        self.run_next_dict: Dict[str, datetime] = load_run_next()

    def load_jobs(self) -> Set[str]:
        job_ids = set()
        for new_job in self.get_jobs():
            job_id = new_job.id
            job_ids.add(job_id)
            old_job = self.jobs.get(job_id)
            if new_job != old_job:
                self.jobs[job_id] = new_job
                if old_job is not None:
                    log(f"Reloaded job {job_id}")
                    self.tasks[job_id].cancel()
                run_next = self.run_next_dict.get(job_id)
                self.tasks[job_id] = asyncio.create_task(
                    self.schedule_job(new_job, run_next)
                )
        return job_ids

    def prune_jobs(self, jobs_to_keep: Set[str]):
        jobs_to_delete = self.jobs.keys() - jobs_to_keep
        for job_id in jobs_to_delete:
            self.tasks.pop(job_id).cancel()
            del self.jobs[job_id]

    def print_task_exceptions(self):
        for job_id, task in self.tasks.items():
            if task.done():
                e = task.exception()
                if e is not None:
                    log(f"Task {job_id} failed with exception:")
                    traceback.print_exception(
                        type(e), e, e.__traceback__, file=sys.stdout
                    )

    async def poll(self):
        self.load_jobs()
        log(f"Loaded {len(self.jobs)} jobs")

        while True:
            await asyncio.sleep(POLL_PERIOD)

            # load new jobs
            new_jobs = self.load_jobs()

            # prune removed jobs
            self.prune_jobs(jobs_to_keep=new_jobs)

            # check for exceptions
            self.print_task_exceptions()

    @staticmethod
    def get_jobs() -> Iterable[Job]:
        with warnings.catch_warnings():
            for filepath in Path(consts.JOBS_DIR).glob("*.toml"):
                try:
                    with open(filepath, encoding="utf-8") as fp:
                        data = toml.load(fp)
                    job = Job.from_data(data, filepath)
                except toml.TomlDecodeError:
                    log(f"Job file {filepath} is not valid TOML")
                except JobFormatError as e:
                    log(e)
                except JobFormatWarning as w:
                    log(w.message)
                    yield w.job
                else:
                    yield job

    @staticmethod
    async def schedule_job(job: Job, run_next: datetime = None):
        now = datetime.now()

        if job.start is not None and job.start > now:
            log(f"Not starting job {job.id}: start date in the future")
            return

        if job.end is not None and job.end <= now:
            log(f"Not starting job {job.id}: end date in the past")
            return

        tab = CronTab(job.crontab)

        if run_next is None:
            now = datetime.now()
            secs_til_next = tab.next(now, default_utc=False)
            next_run = now + timedelta(seconds=secs_til_next)
            update_run_next({job.id: next_run})
        else:
            secs_til_next = (run_next - datetime.now()).total_seconds()

        while True:
            await asyncio.sleep(secs_til_next)
            log(f"Starting job {job.id}")
            await job.run()
            now = datetime.now()
            secs_til_next = tab.next(now, default_utc=False)
            next_run = now + timedelta(seconds=secs_til_next)
            update_run_next({job.id: next_run})
