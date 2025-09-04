from datetime import datetime, timedelta
from airflow import DAG

from dili.trouble_maker import TroubleMakerOperator

DEFAULT_DURATION = 10
TASK_NUMBER = 4


def generate_task(i: int) -> TroubleMakerOperator:
    return TroubleMakerOperator(
        task_id=f"troublemaker_{i}",
        name=f"TroubleMaker #{i}",
        duration=DEFAULT_DURATION * i
    )


with DAG(
    dag_id="assignment_2",
    description="DAG made of 4 independant tasks",
    schedule=timedelta(minutes=10),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["assignment", "gma", "demo"],
) as dag:
    tasks = list(map(generate_task, range(1, TASK_NUMBER + 1)))

    tasks
