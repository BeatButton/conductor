import asyncio
import warnings

from . import utils
from .exceptions import JobFormatWarning
from .main import Main

utils.platform_setup()
utils.process_env_vars()

main = Main()
warnings.filterwarnings("error", category=JobFormatWarning)

try:
    asyncio.run(main.poll())
except KeyboardInterrupt:
    pass
