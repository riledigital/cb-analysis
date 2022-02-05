import os
import sys
from pathlib import Path
import logging

logging.basicConfig(level=logging.DEBUG)

sys.path.append(Path(os.getcwd()).as_posix())
print(sys.path)

from cbanalysis.main import Main

Main()
