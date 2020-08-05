"""
Utility module to use python logging uniformly.

usage:

```python
from deb.utils.logging import logger
logger.info("all good in the neighborhood!")
```

Author: Par (turalabs.com)
Contact:

license: GPL v3 - PLEASE REFER TO DEB/LICENSE FILE
"""

import logging
from logging import DEBUG, INFO, WARNING, WARN, ERROR, CRITICAL, FATAL
import sys

from deb.utils.config import config


def __get_config_level():
    try:
        return eval(config['logging']['level'].as_str())
    except Exception:
        return INFO


def __get_config_stream():
    try:
        if config['logging']['output'].as_str().lower() == 'stdout':
            return sys.stdout
        else:
            return sys.stderr
    except Exception:
        return sys.stderr


# setup logging and logger
logging.basicConfig(format='[%(levelname)-8s] [%(asctime)s] [%(module)-35s][%(lineno)04d] : %(message)s',
                    level=__get_config_level(),
                    stream=__get_config_stream())
logger: logging.Logger = logging




