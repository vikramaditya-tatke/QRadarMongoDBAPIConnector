import logging
import time
import initiate
from utils.logger import SetupLogging


def main():
    """ """
    SetupLogging().setup_logging()
    start = time.perf_counter()
    initiate.starter()
    stop = time.perf_counter()
    logging.info(msg=f"Finished in {(stop - start) / 60 / 60} hours")


if __name__ == "__main__":
    main()
