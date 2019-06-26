from __future__ import annotations

import asyncio
import os
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, MutableMapping, Optional, Type, Union
from warnings import warn

from crontab import CronTab

from . import consts
from .consts import NoneType
from .exceptions import JobFormatError, JobFormatWarning
from .utils import log


@dataclass
class Job:
    name: str
    id: str
    command: str
    crontab: str
    start: Optional[Union[datetime, date]] = None
    end: Optional[Union[datetime, date]] = None
    environment: Optional[MutableMapping[str, Any]] = None

    @classmethod
    def from_data(
        cls: Type[Job], data: MutableMapping[str, Any], filepath: Path
    ) -> Job:
        job = cls.validate(data, filepath)

        cls.warn(job, data)

        cls.cast(job)

        return cls(**job)

    @classmethod
    def validate(
        cls, data: MutableMapping[str, Any], filepath: Path
    ) -> MutableMapping[str, Any]:
        job_id = filepath.stem

        job: Optional[MutableMapping[str, Any]] = data.pop("job", None)
        if job is None:
            raise JobFormatError(f"Job {job_id} missing [job] section")

        job["id"] = job_id
        job["environment"] = data.pop("environment", {})
        annot = cls.__annotations__  # pylint: disable=no-member

        for field, type_ in annot.items():
            realtype = eval(type_)
            value = job.get(field)
            origin = getattr(realtype, "__origin__", None)
            optional = origin is Union and realtype.__args__[-1] is NoneType

            if optional:
                args = realtype.__args__
                if len(args) > 2:
                    realtype = Union[args[:-1]]
                else:
                    realtype = args[0]
                    origin = getattr(realtype, "__origin__", None)
            elif value is None:
                raise JobFormatError(f"Job {job_id} missing required field {field}")

            if origin is Union:
                realtype = realtype.__args__
            elif origin is not None:
                realtype = origin

            if value is not None and not isinstance(value, realtype):
                raise JobFormatError(
                    f"Field {field} in job {job_id} got {type(value)} but expected {realtype}"
                )

        try:
            CronTab(job["crontab"])
        except ValueError:
            raise JobFormatError(f"Job {job_id} has invalid crontab entry")

        return job

    @classmethod
    def warn(cls, job: MutableMapping[str, Any], data: MutableMapping[str, Any]):
        annot = cls.__annotations__  # pylint: disable=no-member
        job_id = job["id"]
        fields = tuple(job)
        for field in fields:
            if field not in annot:
                warn(JobFormatWarning(f"Job {job_id} had extra field {field}"))
                del job[field]

        for section in data:
            warn(JobFormatWarning(f"Job {job_id} had extra section {section}"))

    @classmethod
    def cast(cls, job: MutableMapping[str, Any]):
        start = job.get("start")
        if start is not None:
            if not isinstance(start, datetime):
                job["start"] = datetime.combine(start, time.min)

        end = job.get("end")
        if end is not None:
            if not isinstance(end, datetime):
                job["end"] = datetime.combine(end, time.min)

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
