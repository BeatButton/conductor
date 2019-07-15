from __future__ import annotations

import asyncio
import os
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, MutableMapping, Optional, Sequence, Type, Union

from crontab import CronTab

from . import consts
from .consts import NoneType
from .utils import log


class JobFormatError(Exception):
    pass


class JobFormatWarning(Warning):
    def __init__(self, job: Job, *args: Sequence[str]):
        self.job = job
        super().__init__(*args)


@dataclass
class Job:
    name: str
    id: str
    command: str
    crontab: str
    directory: Optional[Union[str, Path]] = None
    stdout: Optional[Union[str, Path]] = None
    stderr: Optional[Union[str, Path]] = None
    start: Optional[Union[datetime, date]] = None
    end: Optional[Union[datetime, date]] = None
    environment: Optional[MutableMapping[str, Any]] = None

    @classmethod
    def from_data(
        cls: Type[Job], data: MutableMapping[str, Any], filepath: Path
    ) -> Job:
        job = cls.validate(data, filepath)

        cls.cast(job)

        cls.warn(job, data)

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
                    realtype = Union[args[:-1]]  # type: ignore
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

        jobdir = job.get("directory")
        if jobdir is not None:
            path = Path(jobdir)
            if not path.is_dir():
                raise JobFormatError(
                    f"Field directory in job {job_id} was not a directory: {path}"
                )
            job["directory"] = path

        return job

    @classmethod
    def warn(cls, job: MutableMapping[str, Any], data: MutableMapping[str, Any]):
        annot = cls.__annotations__  # pylint: disable=no-member
        job_id = job["id"]
        warnings = []

        fields = set(job)
        extra_fields = fields - set(annot)
        if extra_fields:
            for field in extra_fields:
                del job[field]
            warnings.append(f"extra field(s) {', '.join(extra_fields)}")

        if data:
            warnings.append(f"extra section(s) {', '.join(data)}")

        if warnings:
            raise JobFormatWarning(
                cls(**job), f"Job {job_id} had {' and '.join(warnings)}"
            )

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
        cwd = self.directory or consts.JOBS_DIR
        process = await asyncio.create_subprocess_shell(
            self.command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={**os.environ, **self.environment},
            cwd=cwd,
            text=True,
        )

        stdout, stderr = map(bytes.decode, await process.communicate())

        if stdout and self.stdout is not None:
            with open(cwd / self.stdout, "a", encoding="utf-8") as fp:
                assert type(stdout) is str
                log(stdout, file=fp)

        if stderr and self.stderr is not None:
            with open(cwd / self.stderr, "a", encoding="utf-8") as fp:
                log(stderr, file=fp)
