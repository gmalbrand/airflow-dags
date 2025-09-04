import logging
import time

from airflow.sdk import BaseOperator
from airflow.exceptions import AirflowFailException


logger = logging.getLogger(__name__)


class TroubleMakerOperator(BaseOperator):

    def __init__(self, name: str, duration=0, generate_exception=False, generate_failure=False, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.duration = duration
        self.generate_exception = generate_exception
        self.generate_failure = generate_failure

    def execute(self, context):
        logger.debug("Trouble Making")

        if self.duration > 0:
            time.sleep(self.duration)

        if self.generate_exception:
            msg = "TroubleMaker generic exception"
            logger.error(msg)
            raise Exception(msg)

        if self.generate_failure:
            msg = "TroubleMaker Airflow Fail exception"
            logger.error(msg)
            raise AirflowFailException(msg)

        return "I am real TroubleMaker"
