from datetime import datetime
from numbers import Number
from typing import Callable, List, Union

class CronTab:
    def __init__(self, crontab: str) -> None: ...
    def next(
        self,
        now: datetime = ...,
        increments: List[Callable] = ...,
        delta: bool = ...,
        default_utc: bool = ...,
    ) -> float: ...
    def previous(
        self, now: datetime = ..., delta: bool = ..., default_utc: bool = ...
    ) -> float: ...
    def test(self, entry: Union[str, Number]) -> bool: ...
