import asyncio

from . import utils
from .main import Main

utils.platform_setup()
utils.monkey_patch()
utils.process_env_vars()

main = Main()

try:
    asyncio.run(main.poll())
except KeyboardInterrupt:
    pass
