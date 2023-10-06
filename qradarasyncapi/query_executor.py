import logging
import time
from datetime import date, datetime, timedelta
import initiate
from utils.logger import SetupLogging
from utils.constants import (
    SHORT_DURATION_QUERIES,
    SHORT_SEARCH_DURATION,
    LONG_SEARCH_DURATION,
)


class QueryExecutor:
    """_summary_"""

    def __init__(self, queries):
        self.queries = queries
        self.new_queries = {}

    def query_creator(self, event_processor, clients):
        """_summary_

        Args:
            event_processor (_type_): _description_
            clients (_type_): _description_
        """
        stop = datetime.fromisoformat((date.today()).isoformat())
        initial = datetime.fromisoformat(
            (stop - timedelta(days=1)).isoformat(timespec="seconds")
        )
        try:
            for client in clients:
                SetupLogging().setup_logging(
                    event_processor=event_processor, client=client
                )
                client_start = time.perf_counter()
                for query_name, query_expression in self.queries.items():
                    start = initial

                    while start <= stop:
                        if query_name in SHORT_DURATION_QUERIES:
                            query_duration, start = get_query_duration(
                                start, SHORT_SEARCH_DURATION
                            )
                        else:
                            query_duration, start = get_query_duration(
                                start, LONG_SEARCH_DURATION
                            )
                        start_perf_counter = time.perf_counter()
                        # - ##### for processorId
                        # - @@@@@ for DOMAINNAME(domainId)
                        # - !!!!! for start
                        # - $$$$$ for stop
                        new_query_expression = (
                            query_expression.replace(
                                "#####", f"{event_processor}"
                            )
                            .replace("@@@@@", f"'{client}'")
                            .replace("!!!!!", f"{query_duration.get('start')}")
                            .replace("$$$$$", f"{query_duration.get('stop')}")
                        )
                        # We can only modify the client here because:
                        # 1.  In the above for loop we are creating a new query expression that will be POSTed
                        #     to QRadar API endpoint.
                        #     So the client name needs to be in the original format recognised by QRadar.
                        # 2.  We will use the client variable as the database name
                        #     in MongoDB and MongoDB doesn't accept " " and "." in database names.
                        _response_header = initiate.initiate_producerconsumer(
                            query_expression=new_query_expression,
                            query_name=query_name,
                            client=client,
                            event_processor=event_processor,
                            query_start_time=start_perf_counter,
                            query_duration_start=query_duration.get("start"),
                            query_duration_stop=query_duration.get("stop"),
                        )
                    # Exiting the inner queries loop into the clients loop.
                    client_stop = time.perf_counter()
                    logging.info(
                        msg="Completed Data Fetching for Client",
                        extra={
                            "Log Time": datetime.utcnow(),
                            "Event Processor": f"{event_processor}",
                            "Client": f"{client}",
                            "Query Start": f"{query_duration.get('start')}",
                            "Query Stop": f"{query_duration.get('stop')}",
                            "Time to Complete (in minutes)": (
                                client_stop - client_start
                            )
                            / 60,
                        },
                    )
        except Exception as my_generic_exp:
            logging.exception(
                "Error Type: {} - Error Message: {}  ".format(
                    type(my_generic_exp).__name__, my_generic_exp
                )
            )


def get_query_duration(start, duration):
    query_duration = {
        "start": start.strftime("'%Y-%m-%d %H:%M:%S'"),
        "stop": "",
    }
    if duration == SHORT_SEARCH_DURATION:
        start = start + timedelta(minutes=SHORT_SEARCH_DURATION)
    elif duration == LONG_SEARCH_DURATION:
        start = start + timedelta(minutes=LONG_SEARCH_DURATION)
    query_duration["stop"] = start.strftime("'%Y-%m-%d %H:%M:%S'")
    return query_duration, start
