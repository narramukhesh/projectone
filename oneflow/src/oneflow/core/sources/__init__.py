from oneflow.core.types import CT, RO, SI, SO, C
from typing import Type, Any
import re
from oneflow.core.schemas.sources import (
    SparkSourceExtractType,
    SinkType,
    WriteOptions,
    ReadOptions,
    SparkSourceType,
)
from oneflow.core.schemas.result import OutputResult
from oneflow.core.schemas.refresh import TaskRefreshTypes as SparkTaskRefreshTypes
import inspect
from oneflow.core.exception.sources import (
    SourceModuleNotImplemented,
    WriteFunctionNotImplementedError,
)
from abc import ABC, abstractmethod
from functools import partial
from importlib import import_module
from oneflow.core.observability.logging import Logger
from pyspark.sql import SparkSession, DataFrame

logger = Logger.get_logger(__name__)


class SparkSourceMixins:
    """This class is MIX-INS implementation for source specific implementation"""

    @staticmethod
    def format_options(ctx: Type[CT], options: Type[RO]):
        """
        This method is used to format the options like secrets and any environment variables

        Parameters
        ----------------
        ctx: Type[CT]
            This is a spark execution context manager
        options: Type[RO]
            This is options to be formatted

        Returns
        --------------
        Type[RO]
            Same options which are passed to the format options
        """
        secrets_pattern = r"\{\{[A-Za-z]+/[A-Za-z]+\}\}"
        for option in options.model_dump().keys():
            if isinstance(getattr(options, option), str):
                value = re.findall(secrets_pattern, getattr(options, option))
                for v in value:
                    v = v.replace("{{").replace("}}").split("/")
                    secret_scope = v[0]
                    secret_key = v[1]
                    setattr(
                        options,
                        option,
                        options.model_dump()[option].replace(
                            v,
                            ctx.secret_manager.resolve(
                                secret_scope=secret_scope, secret_key=secret_key
                            ),
                        ),
                    )
                setattr(
                    options,
                    option,
                    options.model_dump()[option].format(
                        RangeStart=ctx.refresh_policy.range_start,
                        RangeEnd=ctx.refresh_policy.range_end,
                    ),
                )
        return options

    @classmethod
    def build_from_input_config(cls, ctx: Type[CT], inputConfig: Type[SI]):
        """
        This method is used to build the input source class from the input configuration supplied

        Parameters
        ----------------
        ctx: Type[CT]
            This is a spark execution context manager
        inputConfig: Type[SI]
            This is spark input configuration object which is passed as source json object

        Returns
        --------------
            Returns the source reader object
        """
        options = cls.ReadOptions()
        if (
            inputConfig.source_extract_type == SparkSourceExtractType.stream
            and ctx.refresh_policy.type == SparkTaskRefreshTypes.stream
        ):
            reader = ctx.spark.readStream.format(inputConfig.source.value)
        else:
            reader = ctx.spark.read.format(inputConfig.source.value)

        if inputConfig.options:
            options = cls.format_options(ctx, inputConfig.options)

        if inputConfig.features:

            for feature in inputConfig.features.model_dump():
                feature_obj = getattr(cls, f"do_resolve_{feature}")
                options = feature_obj(
                    inputConfig.name,
                    inputConfig.path,
                    inputConfig.source,
                    inputConfig.source_type,
                    ctx,
                    inputConfig.features.to_dict()[feature],
                    options,
                )

        options = options.model_dump() if not isinstance(options, dict) else options

        return cls.create_reader(
            reader,
            inputConfig.name,
            inputConfig.path,
            options,
            inputConfig.source_type.value,
        )

    @classmethod
    def build_from_output_config(cls, ctx: Type[CT], outputConfig: Type[SO]):
        """
        This method is used to build the output source class from the output configuration supplied

        Parameters
        ----------------
        ctx: Type[CT]
            This is a spark execution context manager
        outputConfig: Type[SI]
            This is spark output configuration object which is passed as source json object

        Returns
        --------------
            Returns the sink writer object
        """
        writer_args = {}
        options = {}
        if outputConfig.options:
            options = cls.format_options(ctx, outputConfig.options)

        if outputConfig.write_type != "custom":
            try:
                writer = cls.get_write_function(outputConfig.write_type.value.lower())
            except WriteFunctionNotImplementedError:
                raise WriteFunctionNotImplementedError(
                    f"{outputConfig.write_type.value.lower()} write type function is not implemented"
                )
            writer_function_args = inspect.getfullargspec(writer).args
            if outputConfig.options is not None:
                option_values = outputConfig.options.model_dump()
                option_keys = list(option_values.keys())
                for argument in writer_function_args:
                    if argument in option_keys:
                        writer_args[argument] = option_values[argument]
        else:
            writer = outputConfig.custom_function.execution_function

        if outputConfig.features:

            for feature in outputConfig.features.model_dump():
                feature_obj = getattr(cls, f"do_resolve_{feature}")
                options = feature_obj(
                    outputConfig.name,
                    outputConfig.path,
                    outputConfig.sink,
                    outputConfig.sink_type,
                    ctx,
                    outputConfig.features.to_dict()[feature],
                    options,
                )

        final_options = (
            options.model_dump() if not isinstance(options, dict) else options
        )
        final_options["format"] = outputConfig.sink.value.lower()
        if outputConfig.sink_type == SinkType.file:
            writer_args["table_name"] = outputConfig.path
            final_options["file"] = True

        else:
            writer_args["table_name"] = outputConfig.path
            final_options["table"] = True

        if ctx.type == "stream":
            final_options["trigger"] = (
                options.trigger.trigger
                if hasattr(options, "trigger") and hasattr(options.trigger, "trigger")
                else {"once": True}
            )
            final_options["checkpointLocation"] = (
                ctx.metadata.state_location + f"/checkpoints/{outputConfig.name}"
                if (not hasattr(options, "checkpointLocation"))
                or (
                    hasattr(options, "checkpointLocation")
                    and getattr(options, "checkpointLocation") is None
                )
                else getattr(options, "checkpointLocation")
            )

        return cls.create_writer(
            outputConfig.name, ctx.type, writer, writer_args, final_options
        )


class SourceProxy:
    @classmethod
    def get_source_class(cls, source_name):
        """This method initiates the correct source class object"""
        source_class = source_name.lower()
        source_class = source_class.capitalize() + "Source"
        return getattr(cls.get_source_module(source_name), source_class)

    @classmethod
    def get_sink_class(cls, sink_name):
        """This method initiates the correct sink class object"""
        sink_name = sink_name.lower()
        sink_class = sink_name.capitalize() + "Sink"
        return getattr(cls.get_source_module(sink_name), sink_class)

    @staticmethod
    def get_source_module(name):
        """This method initiates the correct source module object"""
        root_source_module = "oneflow.core.sources.{}"
        if name == "delta":
            root_source_module = root_source_module.format("delta_source")
        elif name in ["parquet", "csv", "json", "excel"]:
            root_source_module = root_source_module.format("file_source")
        elif name == "jdbc":
            root_source_module = root_source_module.format("jdbc_source")
        elif name == "kafka":
            root_source_module = root_source_module.format("kafka_source")
        elif name == "odata":
            root_source_module = root_source_module.format("odata_source")
        else:
            raise SourceModuleNotImplemented(
                f"Provided {name} source module is not been implemented"
            )

        return import_module(root_source_module)


class SourceRead(ABC):
    """This class is a source reader interface"""

    def __init__(
        self,
        name: str,
        path: str,
        reader: Any,
        filter_expression: str,
        drop_columns: Any,
        incremental_state: Any,
        backfill_state: Any,
        source_type: Any,
    ):
        """
        This is Source initialization read implementation

        Parameters
        --------------
        name: str
            Name of the source input reader
        path: str
            path of the source input to extract data
        reader: Any
            This should be dataframe builder object
        filter_expression: Any
            This expression is string of filter to applied on dataframe
        drop_columns: Any
            This is a list of columns applied for the dataframe which to be dropped from source
        incremental_state: Any
            This field contains the information about this source incremental state
        backfill_state:Any
            This field contains the information about this source backfill state
        source_type: Any
            This field determine whether to read as the file load or table load
        """
        self.name = name
        self.path = path
        self.reader = reader
        self.filter_expression = filter_expression
        self.drop_columns = drop_columns
        self.source_type = source_type
        self.incremental_state = incremental_state
        self.backfill_state = backfill_state
        self._df = None

    @property
    def data(self):
        """This is a dataframe object returned by the current df"""
        if self._df is None:
            self.run()
        return self._df

    def run(self):
        """
        This is run implementation to populate the data
        """
        if self.source_type == SparkSourceType.file.value:
            df = self.reader.load(self.path)
        else:
            df = self.reader.table(self.path)
        self._df = df
        if self.filter_expression is not None:
            self._df = self._df.filter(self.filter_expression)
        if self.drop_columns is not None:
            self._df = self._df.drop(*self.drop_columns.split(","))


class SinkWrite(ABC):
    """This class is a sink writer interface"""

    def __init__(
        self, name: str, type: Any, writer_fn: Any, writer_args: Any, options: Any
    ):
        """
        This is Sink write initialization read implementation

        Parameters
        --------------
        name: str
            Name of the source input reader
        type: Any
            type of the
        writer_fn: Any
            This should writer function which will be executed as part of run
        writer_args: Any
            This expression is args to be passed for writer object
        options: Any
            This should be options to be passed for writer
        """
        self.name = name
        self.type = type
        self.writer_fn = writer_fn
        self.writer_args = writer_args
        self.options = options
        self.batch_app_name = ""

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
        load = "batch"
        query_result = None
        if self.type == "stream":
            try:
                load = "stream"
                query_result = (
                    df.writeStream.queryName(self.name)
                    .trigger(**self.options["trigger"])
                    .option("checkpointLocation", self.options["checkpointLocation"])
                    .foreachBatch(
                        lambda data, batchId: partial(
                            self.writer_fn,
                            source_df=data,
                            batchId=batchId,
                            batchAppName=self.batch_app_name,
                            options=self.options,
                        )(**self.writer_args)
                    )
                    .start()
                )
            except Exception as e:
                exception = e
                status = "Failure"

        else:
            try:
                query_result = partial(
                    self.writer_fn, source_df=df, options=self.options
                )(**self.writer_args)
            except Exception as e:
                exception = e
                status = "Failure"
        result = OutputResult(
            load=load, result=query_result, status=status, exception=exception
        )
        return result


class SparkSource(ABC, SparkSourceMixins):
    """This is interface for spark source"""

    read_supported = ["file", "table"]
    read_extract_supported = ["batch", "stream"]
    read_features_supported = [
        "change_data_feature",
        "drop_columns_feature",
        "schema_inference_from_registry",
        "filter_data_feature",
        "post_task_execution",
    ]
    write_supported = ["stream", "file", "table"]
    write_features_supported = ["create_data_object_if_not_exists"]
    write_type_supported = ["append", "overwrite", "scd1", "scd2", "scd3"]

    class SourceRead(SourceRead):
        """This is class implementation for the source reader"""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

    class SinkWrite(SinkWrite):
        """This is class implementation for the sink writer"""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

    class WriteOptions(WriteOptions):
        """This is a write options interface for source specific extentions of writer options"""

    class ReadOptions(ReadOptions):
        """This is a read options interface for source specific extentions of reader options"""

    @classmethod
    @abstractmethod
    def get_write_function(cls, write_type: str):
        """Source specific supported implementation"""

    @classmethod
    def create_reader(
        cls, reader: Any, name: str, path: str, options: Any, source_type: Any
    ):
        """
        This method is creates the source reader class with the options shared

        Parameters
        -----------------
        reader: Any
            This is a reader obj to be executed
        name: str
            This is the name of the source
        path: str
            This is the path of the source input
        options:Any
            This is the options required for the reader
        source_type: Any
            This is the source type required for the reader

        """
        filter_expr = options.get("filter_expr", None)
        drop_columns = options.get("drop_columns", None)
        incremental_state = options.get("incremental", None)
        backfill_state = options.get("backfill", None)
        options = {
            k: v
            for k, v in options.items()
            if k not in ["filter_expr", "drop_columns", "incremental", "backfill"]
        }
        reader = reader.options(**options)
        return cls.SourceRead(
            name,
            path,
            reader,
            filter_expr,
            drop_columns,
            incremental_state,
            backfill_state,
            source_type,
        )

    @classmethod
    def create_writer(
        cls, name: str, type: Any, writer_fn: Any, writer_args: Any, options: Any
    ):
        """
        This method is creates the source reader class with the options shared

        Parameters
        -----------------
        writer_fn: Any
            This is a writer function to be executed
        name: str
            This is the name of the sink
        path: str
            This is the path of the sink path
        type: Any
            This is the type of the sink type
        options:Any
            This is the options required for the writer
        writer_args: Any
            This is the sink writer function required for the writer
        """
        return cls.SinkWrite(name, type, writer_fn, writer_args, options)

    @staticmethod
    @abstractmethod
    def do_resolve_change_data_feature(
        name: str,
        path: str,
        source: Any,
        source_type: str,
        ctx: Type[CT],
        cdc: Any,
        options: Any,
    ):
        """This is abstract method for resolving the change data capture"""

    @staticmethod
    @abstractmethod
    def do_resolve_create_data_object_if_not_exists(
        name: str,
        path: str,
        source: Any,
        source_type: str,
        ctx: Type[CT],
        create_table_feature: Any,
        options: Any,
    ):
        """This is a abstract method for resolving the create table feature"""

    @staticmethod
    @abstractmethod
    def resolve_change_data_feature(
        spark: SparkSession,
        path: str,
        source_type: str,
        refresh_type: Type[CT],
        previous_cdc_value: Any,
        cdc: Any,
        options: Any,
    ):
        """This is abstract method for resolving the change data capture"""

    @staticmethod
    @abstractmethod
    def resolve_post_task_execution(
        spark: SparkSession,
        path: str,
        source_type: str,
        options: Any,
        operation: Any,
        target_path: Any,
    ):
        """This is abstract method for resolving the post task execution"""

    @staticmethod
    @abstractmethod
    def resolve_create_table_if_not_exists(
        spark: SparkSession,
        df: DataFrame,
        path: Any,
        sink_type: str,
        create_table_feature: Any,
        options: Any,
    ):
        """This is a abstract method for resolving the create table feature"""

    @classmethod
    def read_batch(
        cls: Type[C],
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
        df = spark.read.format(source).options(**options)
        if options.get("source_schema", None) is not None:
            df = df.schema(options["source_schema"])

        if source_type == SparkSourceType.file:
            df = df.load(path)
        else:
            df = df.table(path)
        return df

    @classmethod
    def read_stream(
        cls: Type[C],
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

        df = spark.readStream.format(source).options(**options)
        if options.get("source_schema", None) is not None:
            df = df.schema(options["source_schema"])

        if source_type == SparkSourceType.file:
            df = df.load(path)
        else:
            df = df.table(path)

        return df
