import asyncio
import os
import sys
from datetime import datetime
from typing import MutableMapping

import toml

from . import consts


def platform_setup():
    if "win32" in sys.platform:
        # necessary for using asyncio.create_subprocess_exec
        asyncio.set_event_loop_policy(
            asyncio.WindowsProactorEventLoopPolicy()  # type: ignore
        )


def log(*args, **kwargs):
    kwargs.setdefault("flush", True)
    now = datetime.now()
    args = (now.strftime(r"%Y-%m-%dT%H:%M:%S"), *args)
    return print(*args, **kwargs)


def process_env_vars():
    jobs_dir = os.environ.get("CONDUCTOR_JOBS_DIR")
    if jobs_dir is not None:
        consts.JOBS_DIR = jobs_dir
    if not os.path.isdir(consts.JOBS_DIR):
        raise RuntimeError(f"Job directory {consts.JOBS_DIR} is not a directory")

    run_next_dir = os.environ.get("CONDUCTOR_RUN_NEXT_DIR")
    if run_next_dir is not None:
        consts.RUN_NEXT_DIR = run_next_dir


def load_run_next() -> MutableMapping[str, datetime]:
    try:
        os.makedirs(consts.RUN_NEXT_DIR)
    except FileExistsError:
        pass
    try:
        fp = open(f"{consts.RUN_NEXT_DIR}/run_next.blob", encoding="utf-8")
    except FileNotFoundError:
        return {}
    else:
        with fp:
            return toml.load(fp)  # type: ignore


def save_run_next(data: MutableMapping[str, datetime]):
    with open(f"{consts.RUN_NEXT_DIR}/run_next.blob", "w", encoding="utf-8") as fp:
        toml.dump(data, fp)


def update_run_next(new_data: MutableMapping[str, datetime]):
    data = load_run_next()
    data.update(new_data)
    save_run_next(new_data)
