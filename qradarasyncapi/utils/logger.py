"""Module to setup logging"""

import logging.config
import os
from pathlib import Path
import yaml


class SetupLogging:
    def __init__(self):
        self.default_config = (
            Path(__file__).parent.parent.parent
            / "config"
            / "logging_config.yaml"
        )

    def setup_logging(
        self,
        event_processor="default",
        client="client",
        default_level=logging.DEBUG,
    ):
        try:
            path = self.default_config
            path_to_folder = (
                Path(__file__).parent.parent.parent
                / "new_logs"
                / f"{event_processor}"
            )
            path_to_file = path_to_folder / f"pymongologs_{client}.json"
            if os.path.exists(path):
                if not os.path.exists(path_to_folder):
                    print("Creating logging directory")
                    os.makedirs(path_to_folder)

                with open(path, "r", encoding="utf-8") as log_file:
                    config = yaml.safe_load(log_file.read())
                    config["handlers"]["filehandler"][
                        "filename"
                    ] = path_to_file
                    logging.config.dictConfig(config)
                    logging.captureWarnings(False)
            else:
                logging.basicConfig(level=default_level)
        except TypeError as te:
            logging.exception("Error occured while setting up logging...", te)
