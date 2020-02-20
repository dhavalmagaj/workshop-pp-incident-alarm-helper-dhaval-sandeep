import argparse
import logging
import logging.config
import os

from alarm_incidents import get_alarms


def configure_logging(log_dir: str):
    """Configuring standard logging"""

    log_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "simple": {
                "format": "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "simple",
                "stream": "ext://sys.stdout",
            },
            "log_file": {
                "level": "INFO",
                "class": "logging.FileHandler",
                "formatter": "simple",
                "filename": os.path.join(log_dir, "app.log"),
                "encoding": "utf-8",
            },
        },
        "root": {"level": "INFO", "handlers": ["console", "log_file"],},
    }

    logging.config.dictConfig(log_config)


def main(name: str = None):
    """Main function to invoke the hello_world module"""

    log_folder = "logs/"
    os.makedirs(log_folder, exist_ok=True)

    # Configue the standard logging
    configure_logging(log_folder)



def get_args():
    """Argument parser"""

    parser = argparse.ArgumentParser(description="Hello World")

    parser.add_argument(
        "--sd", dest="start_date", type=str, help="Provide a name to say hello",
    )

    pargs, _ = parser.parse_known_args()

    return pargs


if __name__ == "__main__":
    pargs = get_args()

    main(pargs.name)