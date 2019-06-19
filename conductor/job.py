from __future__ import annotations

from dataclasses import dataclass
import os
import sys
import asyncio
import subprocess
from datetime import date
from crontab import CronTab
from typing import Optional, List, Dict, Type, TextIO, MutableMapping, Any


class JobFormatError(Exception):
    pass


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
