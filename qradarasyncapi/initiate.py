import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from utils.logger import SetupLogging
from producerconsumer import ProducerConsumer
from queue import Queue
from query_executor import QueryExecutor
from datetime import datetime
from utils import attributes
import multiprocessing as mp
import dotenv

# TODO: Handle exceptions properly by removing the generic "Exception"
dotenv.load_dotenv(Path(__file__).parent.parent / "config" / ".env")


def initiate_producerconsumer(
    query_name,
    query_expression,
    client,
    event_processor,
    query_start_time,
    query_duration_start,
    query_duration_stop,
):
    # Init Logger
    SetupLogging().setup_logging(
        event_processor=event_processor, client=client
    )
    producer_consumer = ProducerConsumer(
        query_name=query_name,
        query_expression=query_expression,
        client=client,
        event_processor=event_processor,
        query_start_time=query_start_time,
        query_duration_start=query_duration_start,
        query_duration_stop=query_duration_stop,
    )
    try:
        producer_consumer_response = producer_consumer.start_producer()
        if producer_consumer_response:
            (
                response_header,
                status,
                attempt,
            ) = producer_consumer_response
            if response_header:
                record_count = response_header.get("record_count")
                cursor_id = response_header.get("cursor_id")
                request_status = response_header.get("status")
                progress = response_header.get("progress")
                if record_count == 0 or record_count is None:
                    logging.info(
                        msg="No records found",
                        extra={
                            "Log Time": datetime.utcnow(),
                            "Search ID": f"{cursor_id}",
                            "Query Duration Start": f"{query_duration_start}",
                            "Query Duration Stop": f"{query_duration_stop}",
                            "Event Processor": f"{producer_consumer.event_processor}",
                            "Client": f"{producer_consumer.client}",
                            "Query": f"{producer_consumer.query_name}",
                            "Attempt": f"{attempt}",
                            "Progress": f"{progress}",
                            "Status": f"{request_status}",
                            "Completed Flag": f"{status}",
                            "Records Found": f"{record_count}",
                            "Response Header": f"{response_header}",
                        },
                    )
                elif not status:
                    logging.critical(
                        msg="Unknown condition has occured",
                        extra={
                            "Log Time": datetime.utcnow(),
                            "Search ID": f"{cursor_id}",
                            "Query Duration Start": f"{query_duration_start}",
                            "Query Duration Stop": f"{query_duration_stop}",
                            "Event Processor": f"{producer_consumer.event_processor}",
                            "Client": f"{producer_consumer.client}",
                            "Query": f"{producer_consumer.query_name}",
                            "Attempt": f"{attempt}",
                            "Progress": f"{progress}",
                            "Status": f"{request_status}",
                            "Completed Flag": f"{status}",
                            "Records Found": f"{record_count}",
                            "Response Header": f"{response_header}",
                        },
                    )
                else:
                    with ThreadPoolExecutor(
                        max_workers=2,
                    ) as executor:
                        executor.submit(
                            producer_consumer.dequeue, response_header
                        )
                        executor.submit(producer_consumer.enqueue, cursor_id)
            else:
                pass
    except TypeError as type_error:
        logging.exception(
            msg=f"Exhausted Attempts",
            extra={
                "Log Time": datetime.utcnow(),
                "Event Processor": f"{producer_consumer.event_processor}",
                "Client": f"{producer_consumer.client}",
                "Query": f"{producer_consumer.query_name}",
            },
        )
    except Exception as my_generic_exp:
        logging.exception(
            msg=f"Generic Exception: {my_generic_exp}",
            extra={
                "Log Time": datetime.utcnow(),
                "Event Processor": f"{producer_consumer.event_processor}",
                "Client": f"{producer_consumer.client}",
                "Query": f"{producer_consumer.query_name}",
            },
        )


def starter():
    """_summary_"""
    SetupLogging().setup_logging()
    attributes_dict = attributes.get_attributes()
    try:
        with mp.Pool(processes=8) as pool:
            query = QueryExecutor(queries=attributes_dict.queries)
            pool.starmap(
                query.query_creator,
                zip(
                    list(attributes_dict.ep_client_list.keys()),
                    list(attributes_dict.ep_client_list.values()),
                ),
            )
    except Exception as my_generic_exp:
        logging.exception(f"Generic Exception: {my_generic_exp}")
