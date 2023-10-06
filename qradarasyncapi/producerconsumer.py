from datetime import datetime
from queue import Queue, Empty
from json import JSONDecoder
import os
import logging
import time
from pathlib import Path
import dotenv
import requests
import json
from mongoclient import MongoDBConnection
from utils.logger import SetupLogging
from dateutil.relativedelta import relativedelta, SA
import urllib3
from utils.constants import (
    SHORT_DURATION_QUERIES,
    SHORT_SEARCH_DURATION,
    LONG_SEARCH_DURATION,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# TODO: Handle exceptions properly by removing the generic "Exception"
dotenv.load_dotenv(Path(__file__).parent.parent / "config" / ".env")


class ProducerConsumer:
    """_summary_"""

    def __init__(
        self,
        query_expression,
        query_name,
        client,
        event_processor,
        query_start_time,
        query_duration_start,
        query_duration_stop,
    ):
        self.sec_token = os.environ.get("MONGODB_SEC_TOKEN")
        self.qradar_console_id = os.environ.get("QRADAR_CONSOLE_ID")
        self.header = {
            "SEC": self.sec_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Version": "19.0",
        }
        self.query_url = f"https://{self.qradar_console_id}/api/ariel/searches"
        self.mongo_client = MongoDBConnection().get_client()
        self.queue = Queue(maxsize=20000)
        # Command Line Arguments as parameters start
        self.query_expression = query_expression
        self.query_name = "raw_" + query_name
        self.client = client
        self.event_processor = event_processor.replace(" ", "")
        # Command Line Arguments as parameters end
        self.database = None
        self.collection = None

        # Logging variables
        self.get_request_attempt = 1
        self.post_request_attempt = 1
        self.max_attempts = 10
        self.inserted_records = 0
        self.query_start_time = query_start_time
        self.query_duration_start = query_duration_start
        self.query_duration_stop = query_duration_stop
        SetupLogging().setup_logging(
            event_processor=self.event_processor, client=self.client
        )
        print(
            f"INSIDE INIT - EP: {self.event_processor}, Client: {self.client} - Query: {self.query_name} \t\t\t\t "
        )

    def do_post_requests(self, url):
        """Method of sending Post requests to QRadar for triggering and polling searches.

        Args:
            url (str): The Ariel searches URL

        Returns:
            _type_: _description_
        """
        session = requests.Session()
        try:
            result = session.post(
                url=url,
                params={"query_expression": self.query_expression},
                headers=self.header,
                verify=False,
                timeout=120,
            )
            result.raise_for_status()
            return result
        except requests.exceptions.HTTPError as http_err:
            error_response = http_err.response.json()
            if http_err.response.status_code in [500, 501, 502, 503, 504]:
                logging.error(
                    msg=f"Server Error",
                    extra={
                        "Event Processor": f"{self.event_processor}",
                        "Attempt": f"{self.post_request_attempt}",
                        "Client": f"{self.client}",
                        "Query": f"{self.query_name}",
                        "Query Duration Start": f"{self.query_duration_start}",
                        "Query Duration Stop": f"{self.query_duration_stop}",
                        "QRadar Error Code": f"{error_response.get('http_response').get('code')}",
                        "QRadar Error Message": f"{error_response.get('http_response').get('message')} - {error_response.get('message')}",
                    },
                )
                time.sleep(300)
            elif http_err.response.status_code in [402, 404, 422]:
                # self.post_request_attempt += 1
                error_response = http_err.response.json()
                logging.error(
                    msg=f"Client Error",
                    extra={
                        "Event Processor": f"{self.event_processor}",
                        "Attempt": f"{self.post_request_attempt}",
                        "QRadar Error Code": f"{error_response.get('http_response').get('code')}",
                        "QRadar Error Message": f"{error_response.get('http_response').get('message')} - {error_response.get('message')}",
                        "Client": f"{self.client}",
                        "Query": f"{self.query_name}",
                        "Query Duration Start": f"{self.query_duration_start}",
                        "Query Duration Stop": f"{self.query_duration_stop}",
                    },
                )
        except requests.exceptions.ConnectionError as conn_err:
            error_response = conn_err.response.json()
            # self.post_request_attempt += 1
            logging.error(
                msg=f"Connection Refused",
                extra={
                    "Event Processor": f"{self.event_processor}",
                    "Attempt": f"{self.post_request_attempt}",
                    "Client": f"{self.client}",
                    "Query": f"{self.query_name}",
                    "Query Duration Start": f"{self.query_duration_start}",
                    "Query Duration Stop": f"{self.query_duration_stop}",
                    "QRadar Error Code": f"{error_response.get('http_response').get('code')}",
                    "QRadar Error Message": f"{error_response.get('http_response').get('message')} - {error_response.get('message')}",
                },
            )
            time.sleep(30)
        except requests.exceptions.Timeout as timeout_err:
            error_response = http_err.response.json()
            # self.post_request_attempt += 1
            logging.error(
                msg=f"Read timed out",
                extra={
                    "Event Processor": f"{self.event_processor}",
                    "Attempt": f"{self.post_request_attempt}",
                    "Client": f"{self.client}",
                    "Query": f"{self.query_name}",
                    "Query Duration Start": f"{self.query_duration_start}",
                    "Query Duration Stop": f"{self.query_duration_stop}",
                    "QRadar Error Code": f"{error_response.get('http_response').get('code')}",
                    "QRadar Error Message": f"{error_response.get('http_response').get('message')} - {error_response.get('message')}",
                },
            )
            time.sleep(30)
        except requests.exceptions.RequestException as requests_exp:
            logging.exception(
                msg=f"Generic Exception: {requests_exp}",
                extra={
                    "Log Time": datetime.utcnow(),
                    "Event Processor": self.event_processor,
                    "Client": f"{self.client}",
                    "Query": f"{self.query_name}",
                    "Query Duration Start": f"{self.query_duration_start}",
                    "Query Duration Stop": f"{self.query_duration_stop}",
                },
            )
            time.sleep(30)

    def poller(
        self, regular_sleep, deployment_sleep, error_server_io_sleep, url
    ):
        try:
            result = self.do_post_requests(url)
            status_code = result.status_code
            response_header = result.json()
            completed: bool = response_header.get("completed")
            request_status = response_header.get("status")
            record_count = response_header.get("record_count")
            progress = response_header.get("progress")
            cursor_id = response_header.get("cursor_id")
            error_messages = response_header.get("error_messages")
            # Success Condition:
            if error_messages is None and completed:
                logging.info(
                    msg="Search Completed",
                    extra={
                        "Log Time": datetime.utcnow(),
                        "Request Type": "Polling",
                        "Status Code": f"{status_code}",
                        "Query Duration Start": f"{self.query_duration_start}",
                        "Query Duration Stop": f"{self.query_duration_stop}",
                        "Search ID": f"{cursor_id}",
                        "Event Processor": self.event_processor,
                        "Client": f"{self.client}",
                        "Query": f"{self.query_name}",
                        "Attempt": f"{self.post_request_attempt}",
                        "Progress": f"{progress}",
                        "Status": f"{request_status}",
                        "Completed Flag": f"{completed}",
                        "Records Found": f"{record_count}",
                        "Response Header": f"{response_header}",
                    },
                )
                return True, response_header
                # Normal Retry Condition
            elif error_messages is None and not completed:
                logging.info(
                    msg="Checking Search Status",
                    extra={
                        "Log Time": datetime.utcnow(),
                        "Request Type": "Polling",
                        "Status Code": f"{status_code}",
                        "Query Duration Start": f"{self.query_duration_start}",
                        "Query Duration Stop": f"{self.query_duration_stop}",
                        "Search ID": f"{cursor_id}",
                        "Event Processor": self.event_processor,
                        "Client": f"{self.client}",
                        "Query": f"{self.query_name}",
                        "Attempt": f"{self.post_request_attempt}",
                        "Progress": f"{progress}",
                        "Status": f"{request_status}",
                        "Completed Flag": f"{completed}",
                        "Records Found": f"{record_count}",
                        "Response Header": f"{response_header}",
                    },
                )
                print(
                    f"Attempt {self.post_request_attempt} - Process: {os.getpid()} Sleeping for {regular_sleep/60} minutes"
                )
                time.sleep(regular_sleep)
                self.post_request_attempt = self.post_request_attempt + 1
                return self.query_status(url)
            # Abnormal Retry Condition
            else:
                print(
                    f"Attempt {self.post_request_attempt} - Process: {os.getpid()} Sleeping for {regular_sleep / 60} minutes"
                )
                error_messages_code = error_messages[0].get("code")
                error_messages_message = error_messages[0].get("message")
                logging.error(
                    msg=f"Search Error {regular_sleep / 60} minutes",
                    extra={
                        "Log Time": datetime.utcnow(),
                        "Request Type": "Polling",
                        "Status Code": f"{status_code}",
                        "Query Duration Start": f"{self.query_duration_start}",
                        "Query Duration Stop": f"{self.query_duration_stop}",
                        "Search ID": f"{cursor_id}",
                        "Event Processor": self.event_processor,
                        "Client": f"{self.client}",
                        "Query": f"{self.query_name}",
                        "Attempt": f"{self.post_request_attempt}",
                        "Progress": f"{progress}",
                        "Status": f"{request_status}",
                        "Completed Flag": f"{completed}",
                        "Records Found": f"{record_count}",
                        "QRadar Error Code": f"{error_messages_code}",
                        "QRadar Error Message": f"{error_messages_message}",
                        "Response Header": f"{response_header}",
                    },
                )
                time.sleep(regular_sleep)
                self.post_request_attempt = self.post_request_attempt + 1
                # Adding a return False condition here to trigger a completely new API request.
                return False, response_header

            # if self.post_request_attempt == self.max_attempts:
            #     logging.info(
            #         msg=f"Exhausted Attempts",
            #         extra={
            #             "Log Time": datetime.utcnow(),
            #             "Request Type": "Polling",
            #             "Status Code": f"{status_code}",
            #             "Query Duration Start": f"{self.query_duration_start}",
            #             "Query Duration Stop": f"{self.query_duration_stop}",
            #             "Search ID": f"{cursor_id}",
            #             "Event Processor": self.event_processor,
            #             "Client": f"{self.client}",
            #             "Query": f"{self.query_name}",
            #             "Attempt": f"{self.post_request_attempt}",
            #             "Progress": f"{progress}",
            #             "Status": f"{request_status}",
            #             "Completed Flag": f"{completed}",
            #             "Records Found": f"{record_count}",
            #             "Response Header": f"{response_header}",
            #         },
            #     )
            #     time.sleep(regular_sleep)
            #     self.post_request_attempt += 1
            #     return self.query_status(url)
            # self.query_status(url)
            return True, None
        except Exception as my_generic_exp:
            peint("I am here")
            logging.exception(
                msg=f"Generic Exception: {my_generic_exp}",
                extra={
                    "Log Time": datetime.utcnow(),
                    "Event Processor": self.event_processor,
                    "Client": f"{self.client}",
                    "Query": f"{self.query_name}",
                    "Query Duration Start": f"{self.query_duration_start}",
                    "Query Duration Stop": f"{self.query_duration_stop}",
                },
            )

    def query_status(self, url: str):
        """Polls QRadar /airel/searches/cursor_id endpoint to retrieve the status of the search.
        Polling interval is set at 90 seconds for SHORT_DURATION_QUERIES -> Authentication Failure, Authentication Success, Allowed Traffic and to 300 seconds for other queries.
        It polls to check for a completed status max_attempts number of times.

        Args:
            self (ProducerConsumer): Object of type ProducerConsumer.
            url (str): This url contains the cursor_id received for the ongoing search.

        Returns:
            any: Returns None if the the number of attempts are exhausted, otherwise returns a boolean.
            Returns False if a new search needs to be triggered, otherwise returns True.

        """
        if self.query_name in SHORT_DURATION_QUERIES:
            regular_sleep = 90  # 1.5 minutes
        else:
            regular_sleep = 180  # 3 minutes
        deployment_sleep = 10800  # 3 hours
        error_server_io_sleep = 1800  # 30 minutes
        try:
            while self.post_request_attempt <= self.max_attempts:
                print("\n\n", self.post_request_attempt, "\n\n")
                polling_response = self.poller(
                    regular_sleep, deployment_sleep, error_server_io_sleep, url
                )
                if polling_response:
                    completed = polling_response[0]
                    response_header = polling_response[1]
                    if completed and response_header:
                        return completed, response_header

        except Exception as my_generic_exp:
            logging.exception(
                msg=f"Exhausted Attempts",
                extra={
                    "Log Time": datetime.utcnow(),
                    "Event Processor": self.event_processor,
                    "Client": f"{self.client}",
                    "Query": f"{self.query_name}",
                    "Query Duration Start": f"{self.query_duration_start}",
                    "Query Duration Stop": f"{self.query_duration_stop}",
                },
            )

    def start_check(self, cursor_id):
        polling_response = self.query_status(
            f"https://{self.qradar_console_id}/api/ariel/searches/{cursor_id}",
        )
        if polling_response:
            return polling_response[0], polling_response[1]

    def start_producer(self):
        try:
            result = self.do_post_requests(self.query_url)
            if result:
                response_header = result.json()
                cursor_id = response_header.get("cursor_id")
                record_count = response_header.get("record_count")
                logging.info(
                    msg=f"Search Triggered",
                    extra={
                        "Log Time": datetime.utcnow(),
                        "Request Type": "Trigger",
                        "Status Code": f"{result.status_code}",
                        "Query Duration Start": f"{self.query_duration_start}",
                        "Query Duration Stop": f"{self.query_duration_stop}",
                        "Search ID": f"{cursor_id}",
                        "Event Processor": self.event_processor,
                        "Client": f"{self.client}",
                        "Query": f"{self.query_name}",
                        "Attempt": f"{self.post_request_attempt}",
                        "Progress": f"{response_header.get('progress')}",
                        "Status": f"{response_header.get('status')}",
                        "Completed Flag": f"{response_header.get('completed')}",
                        "Records Found": f"{response_header.get('record_count')}",
                        "Response Header": f"{response_header}",
                    },
                )
                polling_response = self.start_check(cursor_id)
                if polling_response:
                    completed = polling_response[0]
                    response_header = polling_response[1]
                    if completed:
                        print("\n\nSearch Completed\n\n", response_header)
                        return (
                            response_header,
                            completed,
                            self.post_request_attempt,
                        )
                    else:
                        print("\n\nSearch not completed\n\n", response_header)
                        self.start_producer()
                else:
                    logging.info(
                        msg=f"Exhausted Attempts",
                        extra={
                            "Log Time": datetime.utcnow(),
                            "Request Type": "Trigger",
                            "Status Code": f"{result.status_code}",
                            "Query Duration Start": f"{self.query_duration_start}",
                            "Query Duration Stop": f"{self.query_duration_stop}",
                            "Search ID": f"{cursor_id}",
                            "Event Processor": self.event_processor,
                            "Client": f"{self.client}",
                            "Query": f"{self.query_name}",
                            "Attempt": f"{self.post_request_attempt}",
                            "Progress": f"{response_header.get('progress')}",
                            "Status": f"{response_header.get('status')}",
                            "Completed Flag": f"{response_header.get('completed')}",
                            "Records Found": f"{response_header.get('record_count')}",
                            "Response Header": f"{response_header}",
                        },
                    )
            else:
                pass
        except ValueError as value_error:
            logging.exception(
                msg=f"Invalid JSON",
                extra={
                    "Log Time": datetime.utcnow(),
                    "Event Processor": self.event_processor,
                    "Client": f"{self.client}",
                    "Query": f"{self.query_name}",
                    "Query Duration Start": f"{self.query_duration_start}",
                    "Query Duration Stop": f"{self.query_duration_stop}",
                },
            )
        except AttributeError as attr_error:
            print(attr_error.args)
            logging.exception(
                msg=f"Response Header invalid",
                extra={
                    "Log Time": datetime.utcnow(),
                    "Event Processor": self.event_processor,
                    "Client": f"{self.client}",
                    "Query": f"{self.query_name}",
                    "Query Duration Start": f"{self.query_duration_start}",
                    "Query Duration Stop": f"{self.query_duration_stop}",
                },
            )
        except TypeError as type_err:
            logging.exception(
                msg=f"",
                extra={
                    "Log Time": datetime.utcnow(),
                    "Event Processor": self.event_processor,
                    "Client": f"{self.client}",
                    "Query": f"{self.query_name}",
                    "Query Duration Start": f"{self.query_duration_start}",
                    "Query Duration Stop": f"{self.query_duration_stop}",
                },
            )
        except Exception as my_generic_exp:
            logging.exception(
                msg=f"Generic Exception: {my_generic_exp}",
                extra={
                    "Log Time": datetime.utcnow(),
                    "Event Processor": self.event_processor,
                    "Client": f"{self.client}",
                    "Query": f"{self.query_name}",
                    "Query Duration Start": f"{self.query_duration_start}",
                    "Query Duration Stop": f"{self.query_duration_stop}",
                },
            )

    def enqueue(self, cursor_id):
        url = f"https://{self.qradar_console_id}/api/ariel/searches/{cursor_id}/results"
        session = requests.Session()
        get_request_error_response = {
            "http_response": {
                "code": 500,
                "message": "Unexpected internal server error",
            },
            "code": 13,
            "description": "",
            "details": {},
            "message": 'Invocation was successful, but transformation to content type "APPLICATION_JSON" failed',
        }
        get_requests_error_message = (
            '"code":500,"message":"Unexpected internal server error"'
        )
        try:
            with session.get(
                url=url,
                headers=self.header,
                stream=True,
                verify=False,
                timeout=115,
            ) as query_request:
                logging.info(
                    msg="Started Fetching Data",
                    extra={
                        "Log Time": datetime.utcnow(),
                        "Request Type": "Fetching",
                        "Status Code": f"{query_request.status_code}",
                        "Query Duration Start": f"{self.query_duration_start}",
                        "Query Duration Stop": f"{self.query_duration_stop}",
                        "Search ID": f"{cursor_id}",
                        "Event Processor": self.event_processor,
                        "Client": f"{self.original_client_name}",
                        "Query": f"{self.query_name}",
                    },
                )
                lines = query_request.iter_lines(
                    decode_unicode=True, delimiter="},\n"
                )
                for line in lines:
                    if line is not None and line != "" and line:
                        self.queue_writer(line)
                    else:
                        continue
        except requests.exceptions.HTTPError:
            logging.exception(
                msg="Internal Server Error",
                extra={
                    "Log Time": datetime.utcnow(),
                    "Event Processor": self.event_processor,
                    "Client": f"{self.original_client_name}",
                    "Query": f"{self.query_name}",
                    "Query Duration Start": f"{self.query_duration_start}",
                    "Query Duration Stop": f"{self.query_duration_stop}",
                    "QRadar Error Code": get_request_error_response.get(
                        "http_response"
                    ).get("code"),
                    "QRadar Error Message": f"""{get_request_error_response.get("message")}""",
                },
            )
        except Exception as _my_generic_exp:
            logging.exception(
                msg="Generic Exception - Internal Server Error",
                extra={
                    "Log Time": datetime.utcnow(),
                    "Event Processor": self.event_processor,
                    "Client": f"{self.original_client_name}",
                    "Query": f"{self.query_name}",
                    "Query Duration Start": f"{self.query_duration_start}",
                    "Query Duration Stop": f"{self.query_duration_stop}",
                    "QRadar Error Code": get_request_error_response.get(
                        "http_response"
                    ).get("code"),
                    "QRadar Error Message": f"""{get_request_error_response.get("message")}""",
                },
            )

    def queue_writer(self, line):
        line = line.replace("\n", "").removeprefix('{  "events":[') + "}"
        self.queue.put(line, timeout=120, block=True)

    def insert_data(self, logs):
        try:
            result = self.collection.insert_many(logs)
            return result
        except Exception as my_generic_exp:
            logging.exception(
                msg=f"Generic Exception: {my_generic_exp}",
                extra={
                    "Log Time": datetime.utcnow(),
                    "Event Processor": self.event_processor,
                    "Client": f"{self.original_client_name}",
                    "Collection": f"{self.query_name}",
                    "Query": f"{self.query_name}",
                    "Query Duration Start": f"{self.query_duration_start}",
                    "Query Duration Stop": f"{self.query_duration_stop}",
                },
            )

    def dequeue(self, response_header):
        count = response_header.get("record_count")
        print(f"Count is: {count}")
        cursor_id = response_header.get("cursor_id")
        request_status = response_header.get("status")
        progress = response_header.get("progress")
        completed: bool = response_header.get("completed")
        record_count = count
        decoder = JSONDecoder()
        timeout = 120
        logs = []
        line = ""
        self.original_client_name = self.client
        self.client = self.client.replace(" ", "").replace(".", "")
        self.database = self.mongo_client.get_database(self.client)
        self.collection = self.database.get_collection(self.query_name)
        get_request_error_response = {
            "http_response": {
                "code": 500,
                "message": "Unexpected internal server error",
            },
            "code": 13,
            "description": "",
            "details": {},
            "message": 'Invocation was successful, but transformation to content type "APPLICATION_JSON" failed',
        }
        try:
            if count > 0:
                while record_count >= 1000:
                    line = self.queue.get(block=True, timeout=timeout)
                    line = line.removeprefix(",").replace("\n", "") + "}"
                    line_json, _idx = decoder.raw_decode(line)
                    line_json = self.add_date(line_json)
                    logs.append(line_json)
                    if len(logs) == 1000:
                        result = self.insert_data(
                            logs=logs,
                        )
                        self.inserted_records += len(result.inserted_ids)
                        record_count -= len(logs)
                        del logs[0:]
                    else:
                        continue

                if record_count < 1000:
                    while record_count != 0:
                        line = self.queue.get(block=True, timeout=timeout)
                        line = line.removeprefix(",").replace("\n", "") + "}"
                        line_json, _idx = decoder.raw_decode(line)
                        line_json = self.add_date(line_json)
                        logs.append(line_json)
                        record_count -= 1
                    result = self.insert_data(
                        logs=logs,
                    )
                    self.inserted_records += len(result.inserted_ids)
                logging.info(
                    msg="Completed Data Fetching for Query",
                    extra={
                        "Log Time": datetime.utcnow(),
                        "Search ID": f"{cursor_id}",
                        "Query Duration Start": f"{self.query_duration_start}",
                        "Query Duration Stop": f"{self.query_duration_stop}",
                        "Event Processor": self.event_processor,
                        "Client": f"{self.original_client_name}",
                        "Query": f"{self.query_name}",
                        "Attempt": f"{self.post_request_attempt}",
                        "Progress": f"{progress}",
                        "Status": f"{request_status}",
                        "Completed Flag": f"{completed}",
                        "Records Found": f"{count}",
                        "Records Inserted": f"{self.inserted_records}",
                        "Time to Complete (in minutes)": f"{(time.perf_counter() - self.query_start_time) / 60}",
                        "Response Header": f"{response_header}",
                    },
                )
            else:
                pass
        except ValueError as _value_error:
            print(_value_error.args)
            logging.error(
                msg=f"Internal Server Error",
                extra={
                    "Log Time": datetime.utcnow(),
                    "Search ID": f"{cursor_id}",
                    "Query Duration Start": f"{self.query_duration_start}",
                    "Query Duration Stop": f"{self.query_duration_stop}",
                    "Event Processor": self.event_processor,
                    "Client": f"{self.original_client_name}",
                    "Query": f"{self.query_name}",
                    "Attempt": f"{self.post_request_attempt}",
                    "Progress": f"{progress}",
                    "Status": f"{request_status}",
                    "Completed Flag": f"{completed}",
                    "Records Found": f"{count}",
                    "Records Inserted": f"{self.inserted_records}",
                    "Time to Complete (in minutes)": f"{(time.perf_counter() - self.query_start_time) / 60}",
                    "Response Header": f"{response_header}",
                },
            )
        except Empty as _empty_queue:
            logging.error(
                msg=f"Read timeout - No data received from QRadar for {timeout / 60} minutes",
                extra={
                    "Log Time": datetime.utcnow(),
                    "Search ID": f"{cursor_id}",
                    "Query Duration Start": f"{self.query_duration_start}",
                    "Query Duration Stop": f"{self.query_duration_stop}",
                    "Event Processor": self.event_processor,
                    "Client": f"{self.original_client_name}",
                    "Query": f"{self.query_name}",
                    "Attempt": f"{self.post_request_attempt}",
                    "Progress": f"{progress}",
                    "Status": f"{request_status}",
                    "Completed Flag": f"{completed}",
                    "Records Found": f"{count}",
                    "Records Inserted": f"{self.inserted_records}",
                    "Time to Complete (in minutes)": f"{(time.perf_counter() - self.query_start_time) / 60}",
                    "Response Header": f"{response_header}",
                },
            )
        except Full as _full_queue:
            logging.error(
                msg=f"Queue full for {timeout / 60} minutes",
                extra={
                    "Log Time": datetime.utcnow(),
                    "Search ID": f"{cursor_id}",
                    "Query Duration Start": f"{self.query_duration_start}",
                    "Query Duration Stop": f"{self.query_duration_stop}",
                    "Event Processor": self.event_processor,
                    "Client": f"{self.original_client_name}",
                    "Query": f"{self.query_name}",
                    "Attempt": f"{self.post_request_attempt}",
                    "Progress": f"{progress}",
                    "Status": f"{request_status}",
                    "Completed Flag": f"{completed}",
                    "Records Found": f"{count}",
                    "Records Inserted": f"{self.inserted_records}",
                    "Time to Complete (in minutes)": f"{(time.perf_counter() - self.query_start_time) / 60}",
                    "Response Header": f"{response_header}",
                },
            )
        except Exception as my_generic_exp:
            logging.exception(
                msg=f"Generic Exception: {my_generic_exp}",
                extra={
                    "Log Time": datetime.utcnow(),
                    "Event Processor": self.event_processor,
                    "Client": f"{self.original_client_name}",
                    "Query": f"{self.query_name}",
                    "Query Duration Start": f"{self.query_duration_start}",
                    "Query Duration Stop": f"{self.query_duration_stop}",
                },
            )

    def add_date(self, line_json):
        query_date_epoch = line_json.get("Start Time")
        if query_date_epoch is not None:
            query_timestamp, ms = divmod(query_date_epoch, 1000)
            report_date = datetime.fromtimestamp(query_timestamp).strftime(
                "%d/%m/%Y"
            )
            prev_saturday = (
                datetime.fromtimestamp(query_timestamp)
                + relativedelta(weekday=SA(-1))
            ).strftime("%d/%m/%Y")
            line_json["Start Time ISO"] = datetime.utcfromtimestamp(
                query_timestamp
            )
            line_json["WeekFrom"] = prev_saturday
            line_json["ReportDate"] = report_date
        line_json["createdAt"] = datetime.utcnow()
        return line_json
