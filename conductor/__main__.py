import asyncio
import warnings

from . import utils
from .main import Main

utils.platform_setup()
utils.process_env_vars()

main = Main()

try:
    asyncio.run(main.poll())
except KeyboardInterrupt:
    pass
