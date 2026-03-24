from typing import NamedTuple

import networkx as nx


class EventData(NamedTuple):
    event_class: str
    event_tm: nx.DiGraph
    tm_thecnique: str
    normalizer: str
