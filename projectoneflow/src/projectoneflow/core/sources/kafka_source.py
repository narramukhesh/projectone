from projectoneflow.core.sources import SparkSource
from typing import Optional
from pydantic import Field
from projectoneflow.core.schemas.result import OutputResult
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming.query import StreamingQuery
from projectoneflow.core.schemas.sources import SparkSourceType
from typing import Type
from projectoneflow.core.types import F
from projectoneflow.core.exception.sources import WriteFunctionNotImplementedError


class KafkaSource(SparkSource):
    """This is a Kafka source implementation"""

    read_supported = ["file"]
    read_extract_supported = ["stream"]
    read_features_supported = [
        "drop_columns_feature",
        "schema_inference_from_registry",
        "filter_data_feature",
    ]
    write_supported = ["stream"]
    write_features_supported = []
    write_type_supported = ["append"]

    class ReadOptions(SparkSource.ReadOptions):
        """This class is kafka source read options"""

        bootstrap_server: str = Field(
            ...,
            description="bootstrap server of the kafka broker",
            alias="kafka.bootstrap.servers",
        )
        security_protocol: str = Field(
            "SASL_SSL",
            description="security protocol autentication with kafka broker",
            alias="kafka.security.protocol",
        )
        jaas_config: Optional[str] = Field(
            None,
            description="Jass configuration for autentication with broker",
            alias="kafka.sasl.jaas.config",
        )
        security_algorithm: Optional[str] = Field(
            "https",
            description="SSL endpoint algorithm",
            alias="kafka.ssl.endpoint.identification.algorithm",
        )
        security_mechanism: Optional[str] = Field(
            "PLAIN",
            description="security mechanism for autentication with broker",
            alias="kafka.sasl.mechanism",
        )
        startingOffsets: Optional[str] = Field(
            "earliest", description="starting offsets to get the kafka stream starting"
        )
        failOnDataLoss: Optional[str] = Field(
            "false", description="fail on data loss if any issue with kafka broker"
        )

    class WriteOptions(SparkSource.WriteOptions):
        """This class is kafka source write options"""

        bootstrap_server: str = Field(
            ...,
            description="bootstrap server of the kafka broker",
            alias="kafka.bootstrap.servers",
        )
        security_protocol: str = Field(
            "SASL_SSL",
            description="security protocol autentication with kafka broker",
            alias="kafka.security.protocol",
        )
        jaas_config: Optional[str] = Field(
            None, description="Jass configuration for autentication with broker"
        )
        security_algorithm: Optional[str] = Field(
            "https",
            description="SSL endpoint algorithm",
            alias="kafka.ssl.endpoint.identification.algorithm",
        )
        security_mechanism: Optional[str] = Field(
            "PLAIN",
            description="security mechanism for autentication with broker",
            alias="kafka.sasl.mechanism",
        )

    class SourceRead(SparkSource.SourceRead):
        """This is the Kafka Source Read implementation"""

        def run(self):
            """
            This is run implementation to populate the data
            """

            df = self.reader.option("subscribe", self.path).load()
            self._df = df
            if self.filter_expression is not None:
                self._df = self._df.filter(self.filter_expression)
            if self.drop_columns is not None:
                self._df = self._df.drop_columns(*self.drop_columns.split(","))

    class SinkWrite(SparkSource.SinkWrite):
        """This is the Kafka Sink Write Implementation"""

        def run(self, df):
            """
            This is run implementation to write the data

            Parameters
            ------------
            df: DataFrame
                dataframe to be written to target by executing the function

            Returns
            ----------
            OutputResult
                returns the output result after executing the output
            """
            exception = None
            status = "Success"
            load = "stream"
            query_result = None
            if self.type == "stream":
                try:
                    load = "stream"
                    query_result = (
                        df.writeStream.format("kafka")
                        .options(**self.options)
                        .option("topic", self.writer_args["table_name"])
                        .queryName(self.name)
                        .mode(self.writer_fn.__name__)
                        .trigger(**self.options["trigger"])
                        .option(
                            "checkpointLocation", self.options["checkpoint_location"]
                        )
                        .start()
                    )
                except Exception as e:
                    exception = e
                    status = "Failure"
            else:
                exception = None
                status = "Failure"
                load = "stream"

            result = OutputResult(
                load=load, result=query_result, status=status, exception=exception
            )
            return result

    @classmethod
    def get_write_function(cls, write_type: str) -> Type[F]:
        """
        This method returns the write function obj for specific write type

        Parameters
        ------------------
        cls: class
        write_type: str
            This will specify the write type to get the return write object
        """
        from projectoneflow.core.execution.write import append

        if write_type == "append":
            return append
        else:
            raise WriteFunctionNotImplementedError(
                f"{write_type} write type function is not implemented"
            )

    @staticmethod
    def read_batch(
        spark: SparkSession,
        source: str,
        source_type: SparkSourceType,
        path: str,
        options: ReadOptions,
    ) -> DataFrame:
        """
        This is a static method implemented by the source specific batch implementation

        Parameters
        ----------------
        spark: SparkSession
            This spark session object to communicate the cluster and do actions
        source: str
            source name like csv, parquet
        source_type:str
            source type to define whether file or table
        path: str
            source path location
        options:ReadOptions
            this is the options to be used for the input reader

        Returns
        -----------
        DataFrame
            returns the dataframe
        """

        df = (
            spark.read.format(source)
            .option("subscribe", path)
            .options(**options)
            .load()
        )
        return df

    @staticmethod
    def read_stream(
        spark: SparkSession,
        source: str,
        source_type: SparkSourceType,
        path: str,
        options: ReadOptions,
    ) -> DataFrame:
        """
        This is a static method implemented by the source specific batch implementation

        Parameters
        ----------------
        spark: SparkSession
            This spark session object to communicate the cluster and do actions
        source: str
            source name like csv, parquet
        source_type:str
            source type to define whether file or table
        path: str
            source path location
        options:ReadOptions
            this is the options to be used for the input reader

        Returns
        -----------
        DataFrame
            returns the dataframe
        """

        df = (
            spark.readStream.format(source)
            .option("subscribe", path)
            .options(**options)
            .load()
        )
        return df

    @staticmethod
    def write_stream(
        name: str,
        target_df: DataFrame,
        trigger: dict,
        checkpoint_location: str,
        writer_function: str,
        options: dict,
    ) -> StreamingQuery:
        """
        This is a static method implemented by the source specific batch implementation

        Parameters
        ----------------
        name:str
            Name of the streaming query
        target_df:DataFrame
            dataframe to write the output to target location
        trigger:str
            streaming query schedule
        checkpoint_location:str
            checkpoint location to store the state
        writer_function:str
            writer function to execute the micro-batch
        options:dict
            writer options

        Returns
        -----------
        StreamingQuery
            returns the streaming query object
        """
        query_result = (
            target_df.writeStream.format("kafka")
            .options(**options)
            .option("topic", options["table_name"])
            .option("checkpointLocation", checkpoint_location)
            .queryName(name)
            .trigger(trigger)
            .start()
        )
        return query_result
