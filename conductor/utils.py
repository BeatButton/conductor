import asyncio
import sys
from datetime import datetime, timedelta
from glob import glob
from typing import Dict, Iterable, TextIO

import toml
from crontab import CronTab

from job import Job, JobFormatError

RUN_NEXT_FILE_PATH: str = "config/run_next.blob"


def platform_setup():
    if "win32" in sys.platform:
        # necessary for using asyncio.create_subprocess_exec
        asyncio.set_event_loop_policy(
            asyncio.WindowsProactorEventLoopPolicy()  # type: ignore
        )


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
