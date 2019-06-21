#!/usr/bin/env python
import asyncio
import io
import sys
import traceback
from datetime import datetime
from typing import Dict, Set

from . import utils
from .job import Job

POLL_PERIOD: int = 60  # seconds


class Main:
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.jobs: Dict[str, Job] = {}
        self.run_next_dict: Dict[str, datetime] = utils.load_run_next()

    def load_jobs(self) -> Set[str]:
        log_output = io.StringIO()
        job_ids = set()
        for new_job in utils.get_jobs(log_output=log_output, err_output=sys.stdout):
            job_id = new_job.id
            job_ids.add(job_id)
            old_job = self.jobs.get(job_id)
            if new_job != old_job:
                print(log_output.getvalue(), end="", timestamp=False)
                self.jobs[job_id] = new_job
                if old_job is not None:
                    print(f"Reloaded job {job_id}")
                    self.tasks[job_id].cancel()
                run_next = self.run_next_dict.get(job_id)
                self.tasks[job_id] = asyncio.create_task(
                    utils.schedule_job(new_job, run_next)
                )
            log_output.seek(0)
            log_output.truncate()
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
                    print(f"Task {job_id} failed with exception:")
                    traceback.print_exception(
                        type(e), e, e.__traceback__, file=sys.stdout
                    )

    async def poll(self):
        self.load_jobs()
        print(f"Loaded {len(self.jobs)} jobs")

        while True:
            await asyncio.sleep(POLL_PERIOD)

            # load new jobs
            new_jobs = self.load_jobs()

            # prune removed jobs
            self.prune_jobs(jobs_to_keep=new_jobs)

            # check for exceptions
            self.print_task_exceptions()
