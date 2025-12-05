from projectoneflow.core.observability.logging import Logger
from projectoneflow.core.types import CO
from typing import Type, Any
import json
from datetime import datetime
from pyspark.sql.streaming.listener import StreamingQueryListener
from pyspark.java_gateway import ensure_callback_server_started

logger = Logger.get_logger(__name__)


class SparkQueryListener:
    """This is the custom spark query listener which is register with spark listener to capture spark queries when it is successed and completed"""

    def __init__(self, context: Type[CO]):
        self.context = context
        self._listener = None
        try:
            ensure_callback_server_started(self.context.spark._sc._gateway)
            self.context.spark._jsparkSession.listenerManager().register(
                self._jlistener
            )
        except Exception as e:
            logger.warning(
                f"Custom Spark Listener failed to register failed with error {e}"
            )

    def stop(self):
        """This method unregisters the spark listener from spark context"""
        try:
            if self._jlistener is not None:
                self.context.spark._jsparkSession.listenerManager().unregister(
                    self._jlistener
                )
            self._listener = None
        except Exception as e:
            logger.warning(
                f"Custom Spark Listener failed to un-register failed with error {e}"
            )

    @property
    def _jlistener(self):
        """This property returns newly initialized listener object"""
        if self._listener is None:
            self._listener = SparkQueryListener.JListener(self.context)
        return self._listener

    class JListener:
        def __init__(self, context):
            self.context = context

        class Java:
            implements = ["org.apache.spark.sql.util.QueryExecutionListener"]

        def onSuccess(self, funcName: Any, qe: Any, durationNs: Any):

            logger.debug(
                f"query with plan id {qe.sparkPlan().id()} triggerd by function {funcName} completed in duration {durationNs}"
            )

            self.context.metadata.append(
                "state_query_logs",
                f"{int(datetime.now().timestamp())}_{qe.sparkPlan().id()}",
                json.dumps(
                    {
                        "batch_id": self.context.batch_id,
                        "batch_name": self.context.batch_name,
                        "load_timestamp": f"{datetime.now()}",
                        "query_plan_id": f"{qe.sparkPlan().id()}",
                        "query_plan": qe.sparkPlan().toJSON(),
                        "exception": "",
                        "type": "finish",
                    }
                ),
            )

        def onFailure(self, funcName, qe, exception):
            logger.exception(
                f"query with plan id {qe.sparkPlan().id()} triggerd by function {funcName} failed with error {exception}",
                exc_info=False,
            )
            self.context.metadata.append(
                "state_query_logs",
                f"{int(datetime.now().timestamp())}_{qe.sparkPlan().id()}",
                json.dumps(
                    {
                        "batch_id": self.context.batch_id,
                        "batch_name": self.context.batch_name,
                        "load_timestamp": f"{datetime.now()}",
                        "query_plan_id": f"{qe.sparkPlan().id()}",
                        "query_plan": qe.sparkPlan().toJSON(),
                        "exception": f"{exception}",
                        "type": "exception",
                    }
                ),
            )


class SparkListener:
    """This is the custom spark query listener which is register with spark listener to capture spark raw core metrics"""

    def __init__(self, context: Type[CO]):
        self.context = context
        self._listener = None
        try:
            ensure_callback_server_started(self.context.spark._sc._gateway)
            self.context.spark._jsc.sc().addSparkListener(self._jlistener)
        except Exception as e:
            logger.warning(
                f"Custom Spark Listener failed to register failed with error {e}"
            )

    def stop(self):
        """This method unregisters the spark listener from spark context"""
        try:
            if self._jlistener is not None:
                self.context.spark._jsc.sc().removeSparkListener(self._jlistener)
            self._listener = None
        except Exception as e:
            logger.warning(
                f"Custom Spark Listener failed to un-register failed with error {e}"
            )

    @property
    def _jlistener(self):
        """This property returns newly initialized listener object"""
        if self._listener is None:
            self._listener = SparkListener.JListener(self.context)
        return self._listener

    class JListener:
        """This is spark listener interface implementation"""

        def __init__(self, context: Type[CO]):
            self.context = context
            self.jobs_lists = {}
            self.stage_to_job_mapping = {}

        def onStageCompleted(self, stageCompleted):
            """
            This method is called for every spark stage completed

            Parameters
            -------------------
            stageCompleted:Any
                 This is the stage completed event information for processing further

            """
            stageinfo = stageCompleted.stageInfo()
            taskMetrics = stageinfo.taskMetrics()
            stage_id = stageinfo.stageId()
            job_id = self.stage_to_job_mapping[stage_id]
            self.jobs_lists[job_id]["stages"][stage_id] = {
                "name": stageinfo.name(),
                "submissionTime": stageinfo.submissionTime().get(),
                "completionTime": stageinfo.completionTime().get(),
                "numTasks": stageinfo.numTasks(),
                "executorRunTime": taskMetrics.executorRunTime(),
                "executorCpuTime": taskMetrics.executorCpuTime() / 10000,
                "executorDeserializeTime": taskMetrics.executorDeserializeTime(),
                "executorDeserializeCpuTime": taskMetrics.executorDeserializeCpuTime(),
                "resultSerializationTime": taskMetrics.resultSerializationTime(),
                "jvmGCTime": taskMetrics.jvmGCTime(),
                "resultSize": taskMetrics.resultSize(),
                "diskBytesSpilled": taskMetrics.diskBytesSpilled(),
                "memoryBytesSpilled": taskMetrics.memoryBytesSpilled(),
                "peakExecutionMemory": taskMetrics.peakExecutionMemory(),
                "recordsRead": taskMetrics.inputMetrics().recordsRead(),
                "bytesRead": taskMetrics.inputMetrics().bytesRead(),
                "recordsWritten": taskMetrics.outputMetrics().recordsWritten(),
                "bytesWritten": taskMetrics.outputMetrics().bytesWritten(),
                "shuffleFetchWaitTime": taskMetrics.shuffleReadMetrics().fetchWaitTime(),
                "shuffleTotalBytesRead": taskMetrics.shuffleReadMetrics().totalBytesRead(),
                "shuffleTotalBlocksFetched": taskMetrics.shuffleReadMetrics().totalBlocksFetched(),
                "shuffleLocalBlocksFetched": taskMetrics.shuffleReadMetrics().localBlocksFetched(),
                "shuffleRemoteBlocksFetched": taskMetrics.shuffleReadMetrics().remoteBlocksFetched(),
                "shuffleLocalBytesRead": taskMetrics.shuffleReadMetrics().localBytesRead(),
                "shuffleRemoteBytesRead": taskMetrics.shuffleReadMetrics().remoteBytesRead(),
                "shuffleRemoteBytesReadToDisk": taskMetrics.shuffleReadMetrics().remoteBytesReadToDisk(),
                "shuffleRecordsRead": taskMetrics.shuffleReadMetrics().recordsRead(),
                "shuffleWriteTime": taskMetrics.shuffleWriteMetrics().writeTime()
                / 1000000,
                "shuffleBytesWritten": taskMetrics.shuffleWriteMetrics().bytesWritten(),
                "shuffleRecordsWritten": taskMetrics.shuffleWriteMetrics().recordsWritten(),
            }
            del self.stage_to_job_mapping[stage_id]

        def onStageSubmitted(self, stageSubmitted):
            pass

        def onTaskStart(self, taskStart):
            pass

        def onTaskGettingResult(self, taskGettingResult):
            pass

        def onTaskEnd(self, taskEnd):
            pass

        def onApplicationStart(self, applicationStart):
            """
            This method is called when spark application started

            Parameters
            -------------------
            applicationStart:Any
                This is the application start event information

            """
            logger.info(
                f"spark application started with name:{applicationStart.appName()},id:{applicationStart.appId()},time:{applicationStart.time()},user:{applicationStart.sparkUser()}"
            )

        def onApplicationEnd(self, applicationEnd):
            """
            This method is called when spark application ended

            Parameters
            -------------------
            applicationStart:Any
                This is the application end event information

            """
            logger.info(f"spark application ended at time:{applicationEnd.time()}")

            if len(self.jobs_lists) > 0:
                for i in self.jobs_lists:
                    self.context.metadata.append(
                        "state_job_logs",
                        f"{int(datetime.now().timestamp())}_{i}",
                        json.dumps(
                            {
                                "batch_id": self.context.batch_id,
                                "batch_name": self.context.batch_name,
                                "load_timestamp": f"{datetime.now()}",
                                "job_details": self.jobs_lists[i],
                            }
                        ),
                    )
                self.jobs_lists = {}

        def onExecutorMetricsUpdate(self, executorMetricsUpdate):
            pass

        def onStageExecutorMetrics(self, executorMetrics):
            pass

        def onOtherEvent(self, event):
            pass

        def onExecutorAdded(self, executorAdded):
            pass

        def onExecutorRemoved(self, executorRemoved):
            pass

        def onExecutorBlacklisted(self, executorBlacklisted):
            pass

        def onExecutorExcluded(self, executorExcluded):
            pass

        def onExecutorBlacklistedForStage(self, executorBlacklistedForStage):
            pass

        def onExecutorExcludedForStage(self, executorExcludedForStage):
            pass

        def onNodeBlacklistedForStage(self, nodeBlacklistedForStage):
            pass

        def onNodeExcludedForStage(self, nodeExcludedForStage):
            pass

        def onExecutorUnblacklisted(self, executorUnblacklisted):
            pass

        def onExecutorUnexcluded(self, executorUnexcluded):
            pass

        def onNodeBlacklisted(self, nodeBlacklisted):
            pass

        def onNodeExcluded(self, nodeExcluded):
            pass

        def onNodeUnblacklisted(self, nodeUnblacklisted):
            pass

        def onNodeUnexcluded(self, nodeUnexcluded):
            pass

        def onUnschedulableTaskSetAdded(self, unschedulableTaskSetAdded):
            pass

        def onUnschedulableTaskSetRemoved(self, unschedulableTaskSetRemoved):
            pass

        def onBlockUpdated(self, blockUpdated):
            pass

        def onUnpersistRDD(self, unpersistRDD):
            pass

        def onSpeculativeTaskSubmitted(self, speculativeTask):
            pass

        def onJobStart(self, jobStart: Any):
            """
            This method is called when spark job started

            Parameters
            -------------------
            jobStart:Any
                This is the spark job start event information

            """

            stage_ids = []
            job_id = jobStart.jobId()
            propeties = dict(jobStart.properties())
            self.jobs_lists[job_id] = {
                "group_id": propeties.get("spark.jobGroup.id", None),
                "result": "In-progress",
                "end_time": None,
                "job_id": job_id,
                "stages": {},
                "start_time": jobStart.time(),
                "rdd_scope": propeties.get("spark.rdd.scope", None),
            }
            stages_id = jobStart.stageIds().iterator()
            stages = jobStart.stageIds().size()
            while stages > 0:
                stage_id = stages_id.next()
                self.stage_to_job_mapping[stage_id] = job_id
                self.jobs_lists[job_id]["stages"][stage_id] = {}
                stage_ids.append(stage_id)
                stages -= 1

        def onJobEnd(self, jobEnd: Any):
            """
            This method is called when spark job Ended

            Parameters
            -------------------
            jobEnd:Any
                This is the spark job end event information
            """
            job_id = jobEnd.jobId()
            self.jobs_lists[job_id]["end_time"] = jobEnd.time()
            self.jobs_lists[job_id]["result"] = f"{jobEnd.jobResult()}"

            self.context.metadata.append(
                "state_job_logs",
                f"{int(datetime.now().timestamp())}_{job_id}",
                json.dumps(
                    {
                        "batch_id": self.context.batch_id,
                        "batch_name": self.context.batch_name,
                        "load_timestamp": f"{datetime.now()}",
                        "job_details": self.jobs_lists[job_id],
                    }
                ),
            )
            logger.debug(
                f"Job with id {job_id} is {jobEnd.jobResult()} with details:{self.jobs_lists[job_id]}"
            )
            del self.jobs_lists[job_id]

        class Java:
            implements = ["org.apache.spark.scheduler.SparkListenerInterface"]


class SparkStreamListener:
    """This class is custom spark streaming query listener implementation"""

    def __init__(self, context: Type[CO]):
        self.context = context
        self._listener = None
        try:
            ensure_callback_server_started(self.context.spark._sc._gateway)
            self.context.spark.streams.addListener(self.stream_listener)
        except Exception as e:
            logger.warning(
                f"Custom Stream Spark Listener failed to register failed with error {e}"
            )

    def stop(self):
        """This method unregisters the spark listener from spark context"""
        try:
            if self.stream_listener is not None:
                self.context.spark.streams.removeListener(self.stream_listener)
            self._listener = None
        except Exception as e:
            logger.warning(
                f"Custom Stream Spark Listener failed to un-register failed with error {e}"
            )

    @property
    def stream_listener(self):
        """This property returns newly initialized listener object"""
        if self._listener is None:
            self._listener = SparkStreamListener.StreamListener(self.context)
        return self._listener

    class StreamListener(StreamingQueryListener):
        """This is the custom stream query listener"""

        def __init__(self, context):
            self.context = context

        def onQueryStarted(self, event):
            logger.info(
                f"stream query {event.name} with id {event.id} has started at timestamp {event.timestamp}"
            )

        def onQueryProgress(self, event):
            try:
                logger.debug(
                    f"""Spark Streaming micro-stream for {event.progress.name} statistics are numInputRows:{event.progress.numInputRows},inputRowsPerSecond:{event.progress.inputRowsPerSecond},
            processedRowsPerSecond: {event.progress.processedRowsPerSecond},batchId: {event.progress.batchId},batchDuration: {event.progress.batchDuration},durationMs: {event.progress.durationMs},"""
                )
                self.context.metadata.append(
                    "state_stream_job_progress_logs",
                    f"{int(datetime.now().timestamp())}_{event.progress.name}",
                    json.dumps(
                        {
                            "batch_id": self.context.batch_id,
                            "batch_name": self.context.batch_name,
                            "load_timestamp": f"{datetime.now()}",
                            "query_progress": event.progress.json,
                        }
                    ),
                )
            except Exception as e:
                logger.debug(
                    f"Some Problem with parsing the stream query event, failing with error {e}"
                )

        def onQueryTerminated(self, event):
            try:
                if event.exception:
                    logger.exception(
                        f"stream query {event.id} failed with error {event.exception}",
                        exc_info=False,
                    )
                    self.context.metadata.append(
                        "state_stream_job_terminated_logs",
                        f"{int(datetime.now().timestamp())}_{event.id}",
                        json.dumps(
                            {
                                "batch_id": self.context.batch_id,
                                "batch_name": self.context.batch_name,
                                "load_timestamp": f"{datetime.now()}",
                                "exception": f"{event.exception}",
                            }
                        ),
                    )
                else:
                    self.context.metadata.append(
                        "state_stream_job_terminated_logs",
                        f"{int(datetime.now().timestamp())}_{event.id}",
                        json.dumps(
                            {
                                "batch_id": self.context.batch_id,
                                "batch_name": self.context.batch_name,
                                "load_timestamp": f"{datetime.now()}",
                                "exception": None,
                            }
                        ),
                    )
            except Exception as e:
                logger.debug(
                    f"Some Problem with parsing the stream query event, failing with error {e}"
                )

        def onQueryIdle(self, event):
            return super().onQueryIdle(event)
