from __future__ import annotations

import asyncio
import os
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, List, MutableMapping, Optional, TextIO, Type

from crontab import CronTab

from . import consts
from .exceptions import JobFormatError
from .utils import log


@dataclass
class Job:
    name: str
    id: str
    command: str
    crontab: str
    start: Optional[datetime] = None
    end: Optional[datetime] = None
    environment: Optional[MutableMapping[str, Any]] = None

    @classmethod
    def from_data(
        cls: Type[Job],
        data: MutableMapping[str, Any],
        filepath: Path,
        *,
        log_output: TextIO = None,
        err_output: TextIO = None,
    ) -> Job:
        if log_output is None:
            log_output = open(os.devnull, "w")
        if err_output is None:
            err_output = open(os.devnull, "w")

        job = cls.validate(data, filepath, err_output=err_output)

        cls.warn(job, data, log_output=log_output)

        return cls(**job)

    @classmethod
    def validate(
        cls, data: MutableMapping[str, Any], filepath: Path, *, err_output: TextIO
    ) -> MutableMapping[str, Any]:
        job_id = filepath.stem

        job: Optional[MutableMapping[str, Any]] = data.pop("job", None)
        if job is None:
            log(f"Job {job_id} missing [job] section", file=err_output)
            raise JobFormatError

        job["id"] = job_id
        job["environment"] = data.pop("environment", {})
        annot = cls.__annotations__  # pylint: disable=no-member

        for field, type_ in annot.items():
            if not type_.startswith("Optional") and field not in job:
                log(f"Job {job_id} missing required field {field}", file=err_output)
                raise JobFormatError

        start = job.get("start")
        if start is not None:
            if not isinstance(start, date):
                log(
                    f"Job {job_id} field start should be a date or time",
                    file=err_output,
                )
                raise JobFormatError
            if not isinstance(start, datetime):
                job["start"] = datetime.combine(start, time.min)

        end = job.get("end")
        if end is not None:
            if not isinstance(end, date):
                log(f"Job {job_id} field end should be a date or time", file=err_output)
                raise JobFormatError
            if not isinstance(end, datetime):
                job["end"] = datetime.combine(end, time.min)

        try:
            CronTab(job["crontab"])
        except ValueError:
            log(f"Job {job_id} has invalid crontab entry", file=err_output)
            raise JobFormatError

        return job

    @classmethod
    def warn(
        cls,
        job: MutableMapping[str, Any],
        data: MutableMapping[str, Any],
        *,
        log_output: TextIO,
    ):
        annot = cls.__annotations__  # pylint: disable=no-member
        job_id = job["id"]
        fields = tuple(job)
        for field in fields:
            if field not in annot:
                log(f"Job {job_id} had extra field {field}", file=log_output)
                del job[field]

        for section in data:
            log(f"Job {job_id} had extra section {section}", file=log_output)

    async def run(self):
        process = await asyncio.create_subprocess_shell(
            self.command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            env={**os.environ, **self.environment},
            cwd=consts.JOBS_DIR,
        )

        _, stderr = await process.communicate()

        if stderr:
            log(
                f"Job {self.id} encountered an error in execution:\n"
                f"{stderr.decode()}"
            )
