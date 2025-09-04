from datetime import datetime, timedelta
from airflow import DAG

from dili.trouble_maker import TroubleMakerOperator

from random import randint

DEFAULT_DURATION = 10
TASK_NUMBER = 4

GRAPH_MAX_DEPTH = 5
GRAPH_MIN_DEPTH = 1
GRAPH_MAX_WIDTH = 20
GRAPH_MIN_WIDTH = 10
EDGE_CHANCE = 30


def generate_task(i: int) -> TroubleMakerOperator:
    return TroubleMakerOperator(
        task_id=f"troublemaker_{i}",
        name=f"TroubleMaker #{i}",
        duration=DEFAULT_DURATION
    )


with DAG(
    dag_id="assignment_3",
    description="Generate a crazy DAG",
    schedule=timedelta(minutes=10),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["assignment", "gma", "demo"],
) as dag:

    depth = randint(GRAPH_MIN_DEPTH, GRAPH_MAX_DEPTH)
    nodes = []

    for i in range(1, depth + 1):
        rank = i + len(nodes)
        width = randint(GRAPH_MIN_WIDTH, GRAPH_MAX_WIDTH)

        new_nodes = list(map(generate_task, range(rank, rank + width)))

        for _, n in enumerate(nodes):
            for _, nn in enumerate(new_nodes):
                if (randint(1, 100) < EDGE_CHANCE):
                    n >> nn

        nodes.extend(new_nodes)

    list(filter(lambda x: len(x.get_direct_relatives(upstream=True)) == 0, nodes))
