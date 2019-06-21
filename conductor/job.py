from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, List, MutableMapping, Optional, TextIO, Type

from crontab import CronTab

from . import utils


class JobFormatError(Exception):
    pass


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
            print(f"Job {job_id} missing [job] section", file=err_output)
            raise JobFormatError

        job["id"] = job_id
        job["environment"] = data.pop("environment", {})
        annot = cls.__annotations__  # pylint: disable=no-member

        for field, type_ in annot.items():
            if not type_.startswith("Optional") and field not in job:
                print(f"Job {job_id} missing required field {field}", file=err_output)
                raise JobFormatError

        start = job.get("start")
        if start is not None:
            if type(start) is date:
                job["start"] = datetime.combine(start, time.min)
            elif not isinstance(start, datetime):
                print(
                    f"Job {job_id} field start should be a date or time",
                    file=err_output,
                )
                raise JobFormatError

        end = job.get("end")
        if end is not None:
            if type(end) is date:
                job["end"] = datetime.combine(end, time.min)
            elif not isinstance(end, datetime):
                print(
                    f"Job {job_id} field end should be a date or time", file=err_output
                )
                raise JobFormatError

        try:
            CronTab(job["crontab"])
        except ValueError:
            print(f"Job {job_id} has invalid crontab entry", file=err_output)
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
                print(f"Job {job_id} had extra field {field}", file=log_output)
                del job[field]

        for section in data:
            print(f"Job {job_id} had extra section {section}", file=log_output)

    async def run(self):
        process = await asyncio.create_subprocess_shell(
            self.command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            env=self.environment,
            cwd=utils.JOBS_DIR,
        )

        _, stderr = await process.communicate()

        if stderr:
            print(
                f"Job {self.id} encountered an error in execution:\n"
                f"{stderr.decode()}"
            )
