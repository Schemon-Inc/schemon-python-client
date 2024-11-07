import time
from pyspark.sql.streaming import StreamingQueryListener
from schemon_python_logger.logger import Logger


class StreamingTriggerListener(StreamingQueryListener):
    def __init__(
        self,
        logger: Logger = None,
        stage: str = None,
        entity_name: str = None,
    ):
        self.logger = logger
        self.stage = stage
        self.entity_name = entity_name

    def onQueryStarted(self, event):
        message = f"Query started: {event.id}"
        if self.logger:
            self.logger.info(message, self.stage, self.entity_name)
        else:
            print(message)

    def onQueryProgress(self, event):
        message = f"Query running: {event.id} | Trigger executed at {time.strftime('%Y-%m-%d %H:%M:%S')}"
        if self.logger:
            self.logger.info(message, self.stage, self.entity_name)
        else:
            print(message)

    def onQueryIdle(self, event):
        message = f"Query running: {event.id} | Trigger is idle at {time.strftime('%Y-%m-%d %H:%M:%S')}"
        if self.logger:
            self.logger.info(message, self.stage, self.entity_name)
        else:
            print(message)

    def onQueryTerminated(self, event):
        message = f"Query terminated: {event.id}"
        if self.logger:
            self.logger.info(message, self.stage, self.entity_name)
        else:
            print(message)
