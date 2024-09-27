import os
from logging.config import dictConfig
from pathlib import Path

import aiofiles
import yaml

from assets.src import schemas
from assets.src.discord.services import dev_env

WORKING_DIR = f"{str(Path.home())}/bot"


async def update_config_with_latest_values(cluster: schemas.Cluster, configuration):
    """Update the config file with latest values"""
    if configuration["modules"][cluster.name][cluster.layer]["id"] != cluster.id:
        configuration["modules"][cluster.name][cluster.layer]["id"] = cluster.id
        async with aiofiles.open(f"{WORKING_DIR}/config.yml", "w") as file:
            await file.write(yaml.dump(configuration))


async def load():
    """Load configuration file"""
    async with aiofiles.open(f"{WORKING_DIR}/config.yml", "r") as file:
        return yaml.safe_load(await file.read())


def configure_logging():
    # Hypercorn loggers configuration
    hypercorn_logging_config = {
        "version": 1,
        "disable_existing_loggers": False,  # Ensure we don't disable app's existing loggers
        "formatters": {
            "default": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            },
        },
        "handlers": {
            "hypercorn_file": {
                "level": "INFO",
                "class": "logging.FileHandler",
                "filename": "assets/data/logs/hypercorn.log",
                "formatter": "default",
            },
            "app": {
                "level": "INFO",
                "class": "logging.FileHandler",
                "filename": "assets/data/logs/app.log",
                "formatter": "default",
            },
            "commands": {
                "level": "INFO",
                "class": "logging.FileHandler",
                "filename": "assets/data/logs/commands.log",
                "formatter": "default",
            },
            "rewards": {
                "level": "INFO",
                "class": "logging.FileHandler",
                "filename": "assets/data/logs/rewards.log",
                "formatter": "default",
            },
            "db_optimization": {
                "level": "INFO",
                "class": "logging.FileHandler",
                "filename": "assets/data/logs/db_optimization.log",
                "formatter": "default",
            },
            "stats": {
                "level": "DEBUG" if not dev_env else "DEBUG",
                "class": "logging.FileHandler",
                "filename": "assets/data/logs/stats.log",
                "formatter": "default",
            },
            "nextcord": {
                "level": "INFO",
                "class": "logging.FileHandler",
                "filename": "assets/data/logs/nextcord.log",
                "formatter": "default",
            },
        },
        "loggers": {
            "hypercorn.error": {  # Hypercorn error log
                "handlers": ["hypercorn_file"],
                "level": "INFO",
                "propagate": False,  # Ensure it doesn't propagate into other logs
            },
            "hypercorn.access": {  # Hypercorn access log
                "handlers": ["hypercorn_file"],
                "level": "INFO",
                "propagate": False,  # Same as above
            },
            "app": {  # Hypercorn access log
                "handlers": ["app"],
                "level": "INFO",
                "propagate": False,  # Same as above
            },
            "commands": {  # Hypercorn access log
                "handlers": ["commands"],
                "level": "INFO",
                "propagate": False,  # Same as above
            },
            "rewards": {  # Hypercorn access log
                "handlers": ["rewards"],
                "level": "INFO",
                "propagate": False,  # Same as above
            },
            "db_optimization": {  # Hypercorn access log
                "handlers": ["db_optimization"],
                "level": "INFO",
                "propagate": False,  # Same as above
            },
            "stats": {  # Hypercorn access log
                "handlers": ["stats"],
                "level": "DEBUG" if not dev_env else "DEBUG",
                "propagate": False,  # Same as above
            },
            "nextcord": {  # Hypercorn access log
                "handlers": ["nextcord"],
                "level": "INFO",
                "propagate": False,  # Same as above
            },
        },
    }

    # Apply Hypercorn-specific log configurations
    dictConfig(hypercorn_logging_config)
