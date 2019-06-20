import asyncio
import builtins
import os
import sys
from datetime import datetime, timedelta
from typing import MutableMapping, Iterable, TextIO
from pathlib import Path

import toml
from crontab import CronTab

from job import Job, JobFormatError

RUN_NEXT_DIR: str = "config"
JOBS_DIR: str = "jobs"


def platform_setup():
    if "win32" in sys.platform:
        # necessary for using asyncio.create_subprocess_exec
        asyncio.set_event_loop_policy(
            asyncio.WindowsProactorEventLoopPolicy()  # type: ignore
        )


def monkey_patch():
    builtin_print = builtins.print

    def print_(*args, **kwargs):
        kwargs.setdefault("flush", True)
        return builtin_print(*args, **kwargs)

    builtins.print = print_


def process_env_vars():
    global JOBS_DIR, RUN_NEXT_DIR
    jobs_dir = os.environ.get("CONDUCTOR_JOBS_DIR")
    if jobs_dir is not None:
        JOBS_DIR = jobs_dir
    if not os.path.isdir(JOBS_DIR):
        print(f"Job directory {JOBS_DIR} is not a directory")
        exit(1)

    run_next_dir = os.environ.get("CONDUCTOR_RUN_NEXT_DIR")
    if run_next_dir is not None:
        RUN_NEXT_DIR = run_next_dir


def load_run_next() -> MutableMapping[str, datetime]:
    try:
        fp = open(f"{RUN_NEXT_DIR}/run_next.blob", encoding="utf-8")
    except FileNotFoundError:
        return {}
    else:
        with fp:
            return toml.load(fp)  # type: ignore


def save_run_next(data: MutableMapping[str, datetime]):
    with open(f"{RUN_NEXT_DIR}/run_next.blob", "w", encoding="utf-8") as fp:
        toml.dump(data, fp)


def update_run_next(new_data: MutableMapping[str, datetime]):
    data = load_run_next()
    data.update(new_data)
    save_run_next(new_data)


def get_jobs(*, log_output: TextIO = None, err_output: TextIO = None) -> Iterable[Job]:
    for filepath in Path(JOBS_DIR).glob("*.toml"):
        try:
            with open(filepath, encoding="utf-8") as fp:
                data = toml.load(fp)
            job = Job.from_data(
                data, filepath, log_output=log_output, err_output=err_output
            )
        except toml.TomlDecodeError:
            print(f"Job file {filepath} is not valid TOML", file=err_output)
        except JobFormatError:
            pass
        else:
            yield job


async def schedule_job(job: Job, run_next: datetime = None):
    now = datetime.now()

    if job.start is not None and job.start > now:
        print(f"Not starting job {job.id}: start date in the future")
        return

    if job.end is not None and job.end <= now:
        print(f"Not starting job {job.id}: end date in the past")
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
        now = datetime.now()
        secs_til_next = tab.next(now, default_utc=False)
        next_run = now + timedelta(seconds=secs_til_next)
        print(f"{now:%Y-%m-%dT%H:%M:%S} starting job {job.id}")
        update_run_next({job.id: next_run})
        await job.run()
