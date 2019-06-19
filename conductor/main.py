#!/usr/bin/env python
from __future__ import annotations

import asyncio
import io
import os
import subprocess
import sys
import traceback
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from glob import glob
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    MutableMapping,
    Optional,
    Set,
    TextIO,
    Type,
)

import toml
from crontab import CronTab

RUN_NEXT_FILE_PATH: str = "config/run_next.blob"
POLL_PERIOD: int = 60  # seconds


class JobFormatError(Exception):
    pass


def platform_setup():
    if "win32" in sys.platform:
        # necessary for using asyncio.create_subprocess_exec
        asyncio.set_event_loop_policy(
            asyncio.WindowsProactorEventLoopPolicy()  # type: ignore
        )


@dataclass
class Job:
    name: str
    id: str
    command: str
    crontab: str
    arguments: Optional[List[str]] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    environment: Optional[Dict[str, Any]] = None

    @classmethod
    def from_data(
        cls: Type[Job],
        data: MutableMapping[str, Any],
        filename: str,
        *,
        log_output: TextIO = None,
        err_output: TextIO = None,
    ) -> Job:
        if log_output is None:
            log_output = open(os.devnull, "w")
        if err_output is None:
            err_output = open(os.devnull, "w")

        job = cls.validate(data, filename, err_output=err_output)

        cls.warn(job, data, log_output=log_output)

        return cls(**job)

    @classmethod
    def validate(
        cls, data: MutableMapping[str, Any], filename, *, err_output: TextIO
    ) -> Dict[str, Any]:
        job: Optional[Dict[str, Any]] = data.pop("job", None)
        if job is None:
            print("Job missing [job] section", file=err_output)
            raise JobFormatError

        name = job.get("name")

        if name is None:
            print("Job missing name", file=err_output)
            raise JobFormatError

        job_id = filename
        job["id"] = job_id

        job["environment"] = data.pop("environment", {})
        annot = cls.__annotations__  # pylint: disable=no-member

        for field, type_ in annot.items():
            if not type_.startswith("Optional") and field not in job:
                print(f"Job {job_id} missing required field {field}", file=err_output)
                raise JobFormatError

        try:
            CronTab(job["crontab"])
        except ValueError:
            print(f"Job {job_id} has invalid crontab entry", file=err_output)
            raise JobFormatError

        return job

    @classmethod
    def warn(
        cls, job: Dict[str, Any], data: MutableMapping[str, Any], *, log_output: TextIO
    ):
        annot = cls.__annotations__  # pylint: disable=no-member
        job_id = job["id"]
        fields = tuple(job)
        for field in fields:
            if field not in annot:
                print(f"Job {job_id} had extra field {field}", file=log_output)
                del job[field]

        for section in data:
            print(f"Job {job_id} had extra section {section}", file=log_output)

    async def run(self):
        args = self.arguments or []
        process = await asyncio.create_subprocess_exec(
            self.command,
            *args,
            stdout=subprocess.DEVNULL,
            stderr=sys.stderr,
            env=self.environment,
        )
        await process.wait()

    async def schedule(self, run_next: datetime = None):
        today = date.today()

        if self.start_date is not None and self.start_date > today:
            print(f"Not starting job {self.id}: start date in the future")
            return

        if self.end_date is not None and self.end_date <= today:
            print(f"Not starting job {self.id}: end date in the past")
            return

        tab = CronTab(self.crontab)

        if run_next is None:
            now = datetime.now()
            secs_til_next = tab.next(now, default_utc=False)
            next_run = now + timedelta(seconds=secs_til_next)
            update_run_next({self.id: next_run})
        else:
            secs_til_next = (run_next - datetime.now()).total_seconds()

        while True:
            await asyncio.sleep(secs_til_next)
            now = datetime.now()
            secs_til_next = tab.next(now, default_utc=False)
            next_run = now + timedelta(seconds=secs_til_next)
            print(f"{now:%Y-%m-%dT%H:%M:%S} starting job {self.id}")
            update_run_next({self.id: next_run})
            await self.run()


def load_run_next() -> Dict[str, datetime]:
    try:
        fp = open(RUN_NEXT_FILE_PATH, encoding="utf-8")
    except FileNotFoundError:
        return {}
    else:
        with fp:
            return toml.load(fp)  # type: ignore


def save_run_next(data: Dict[str, datetime]):
    with open(RUN_NEXT_FILE_PATH, "w", encoding="utf-8") as fp:
        toml.dump(data, fp)


def update_run_next(new_data: Dict[str, datetime]):
    data = load_run_next()
    data.update(new_data)
    save_run_next(new_data)


def get_jobs(*, log_output: TextIO = None, err_output: TextIO = None) -> Iterable[Job]:
    for filename in glob("jobs/*.toml"):
        try:
            with open(filename, encoding="utf-8") as fp:
                data = toml.load(fp)
            job = Job.from_data(
                data, filename, log_output=log_output, err_output=err_output
            )
        except toml.TomlDecodeError:
            print(f"Job file {filename} is not valid TOML", file=err_output)
        except JobFormatError:
            pass
        else:
            yield job


class Main:
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.jobs: Dict[str, Job] = {}
        self.run_next_dict: Dict[str, datetime] = load_run_next()

    def load_jobs(self) -> Set[str]:
        log_output = io.StringIO()
        job_ids = set()
        for new_job in get_jobs(log_output=log_output):
            job_id = new_job.id
            job_ids.add(job_id)
            old_job = self.jobs.get(job_id)
            if new_job != old_job:
                print(log_output.getvalue(), end="")
                self.jobs[job_id] = new_job
                if old_job is not None:
                    self.tasks[job_id].cancel()
                run_next = self.run_next_dict.get(job_id)
                self.tasks[job_id] = asyncio.create_task(new_job.schedule(run_next))
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
                    traceback.print_exception(type(e), e, e.__traceback__)

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


if __name__ == "__main__":
    platform_setup()
    main = Main()
    try:
        asyncio.run(main.poll())
    except KeyboardInterrupt:
        pass
