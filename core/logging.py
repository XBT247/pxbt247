# core/logging.py
import logging
from typing import Optional

class Logger:
    _configured = False

    @classmethod
    def configure(cls, config: Optional[dict] = None):
        if not cls._configured:
            config = config or {
                'version': 1,
                'disable_existing_loggers': False,
                'formatters': {
                    'standard': {
                        'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
                    }
                },
                'handlers': {
                    'default': {
                        'level': 'INFO',
                        'formatter': 'standard',
                        'class': 'logging.StreamHandler',
                        'stream': 'ext://sys.stdout'
                    }
                },
                'root': {
                    'handlers': ['default'],
                    'level': 'INFO'
                }
            }
            logging.config.dictConfig(config)
            cls._configured = True

    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        return logging.getLogger(name)