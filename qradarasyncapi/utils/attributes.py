import json
from pathlib import Path
from prodict import Prodict


class Attributes(Prodict):
    ep_client_list: list[str]
    queries: list[str]
    times: dict


class MyAttributes:
    def __init__(self):
        self.root_dir = Path(__file__).parent.parent

    def get_ep_client_list(self) -> list[str]:
        """_summary_

        Returns:
            list[str]: _description_
        """
        path = self.root_dir / "input" / "ep_clients_manual.json"
        with open(path, "r") as epc:
            ep_client_list = json.load(epc)
        for ep in ep_client_list:
            ep_client_list[ep].sort()
        return ep_client_list

    def get_queries(self) -> list[str]:
        path = self.root_dir / "input" / "queries_manual.json"
        with open(path, "r") as que:
            queries = json.load(que)
        return queries

    def get_time(self) -> dict:
        path = self.root_dir / "input" / "time.json"
        with open(path, "r") as time:
            times = json.load(time)
        return times


def get_attributes() -> Attributes:
    my_attributes = MyAttributes()
    return Attributes(
        ep_client_list=my_attributes.get_ep_client_list(),
        queries=my_attributes.get_queries(),
        times=my_attributes.get_time(),
    )
